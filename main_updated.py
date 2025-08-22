from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import asyncio
import csv
import os
from datetime import datetime
import threading
from concurrent.futures import ThreadPoolExecutor
import queue
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill, Alignment

# === CSV и асинхронные утилиты ===

# Глобальный накопитель данных для Excel
excel_data_collector = {
    "all_products": [],  # Все товары в одной таблице
    "categories_summary": [],  # Сводка по категориям
    "parsing_log": []  # Лог парсинга
}

# Новый структурированный накопитель данных по категориям
category_data_collector = {}



# Глобальные переменные для восстановления
parsing_state = {
    "current_category": 0,
    "current_subcategory": 0,
    "total_categories": 0,
    "processed_items": 0,
    "last_successful_url": "",
    "start_time": None
}

def restart_browser():
    """Перезапускает браузер для избежания проблем с памятью"""
    global driver
    try:
        if 'driver' in globals() and driver:
            print("🔄 Перезапуск браузера...")
            driver.quit()
            time.sleep(2)
        
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--memory-pressure-off")
        
        driver = webdriver.Chrome(options=chrome_options)
        print("✅ Браузер перезапущен")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка перезапуска браузера: {e}")
        return False

def safe_get_page(url, retries=3):
    """Безопасное получение страницы с повторными попытками"""
    global driver
    
    for attempt in range(retries):
        try:
            print(f"   🌐 Переход на: {url} (попытка {attempt + 1})")
            driver.get(url)
            time.sleep(2)
            
            # Проверяем, что страница загрузилась
            if "Error" not in driver.title and len(driver.page_source) > 1000:
                parsing_state["last_successful_url"] = url
                return True
            else:
                print(f"   ⚠️ Страница загрузилась некорректно")
                
        except Exception as e:
            print(f"   ❌ Ошибка загрузки страницы (попытка {attempt + 1}): {e}")
            
            if attempt < retries - 1:
                print(f"   🔄 Перезапуск браузера перед следующей попыткой...")
                if not restart_browser():
                    continue
                time.sleep(3)
            
    print(f"   ❌ Не удалось загрузить страницу после {retries} попыток")
    return False

def safe_parse_with_retry(parse_function, context=""):
    """Безопасный парсинг с повторными попытками"""
    retries = 2
    
    for attempt in range(retries):
        try:
            result = parse_function()
            if result:  # Если результат не пустой
                return result
            else:
                print(f"   ⚠️ Пустой результат при парсинге {context} (попытка {attempt + 1})")
                
        except Exception as e:
            print(f"   ❌ Ошибка парсинга {context} (попытка {attempt + 1}): {e}")
            
            if attempt < retries - 1:
                print(f"   🔄 Пауза перед повторной попыткой...")
                time.sleep(5)
                
                # Обновляем страницу
                try:
                    driver.refresh()
                    time.sleep(3)
                except:
                    restart_browser()
                    if parsing_state["last_successful_url"]:
                        safe_get_page(parsing_state["last_successful_url"])
    
    print(f"   ❌ Парсинг {context} не удался после {retries} попыток")
    return []

def save_progress_checkpoint():
    """Сохраняет промежуточный прогресс"""
    try:
        if excel_data_collector["all_products"]:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            checkpoint_file = f"checkpoint_progress_{timestamp}.xlsx"
            filepath = os.path.join("results", checkpoint_file)
            
            # Создаем промежуточный Excel файл
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                if excel_data_collector["all_products"]:
                    df = pd.DataFrame(excel_data_collector["all_products"])
                    df.to_excel(writer, sheet_name='Промежуточные результаты', index=False)
                
                # Сохраняем состояние парсинга
                state_df = pd.DataFrame([parsing_state])
                state_df.to_excel(writer, sheet_name='Состояние парсинга', index=False)
            
            print(f"💾 Сохранен промежуточный результат: {checkpoint_file}")
            print(f"   📊 Товаров собрано: {len(excel_data_collector['all_products'])}")
            
    except Exception as e:
        print(f"❌ Ошибка сохранения промежуточного результата: {e}")

def update_parsing_progress(category_index, subcategory_index, total_categories):
    """Обновляет прогресс парсинга"""
    parsing_state["current_category"] = category_index
    parsing_state["current_subcategory"] = subcategory_index
    parsing_state["total_categories"] = total_categories
    parsing_state["processed_items"] += 1
    
    # Сохраняем промежуточный результат каждые 10 обработанных элементов
    if parsing_state["processed_items"] % 10 == 0:
        save_progress_checkpoint()

def add_to_excel_collector(data, category_name, subcategory_name, data_type="products"):
    """Добавляет данные в глобальный накопитель для Excel"""
    global excel_data_collector
    
    timestamp = datetime.now().isoformat()
    
    if data_type == "structured_blocks":
        # Обрабатываем структурированные блоки
        products_count = 0
        for block in data:
            block_title = block.get('block_title', 'Неизвестный блок')
            block_image = block.get('block_image', '')
            table_headers = ', '.join(block.get('table_headers', []))
            
            for product in block.get('products', []):
                row = {
                    'category': category_name,
                    'subcategory': subcategory_name,
                    'block_title': block_title,
                    'block_image': block_image,
                    'name': product.get('name', ''),
                    'url': product.get('url', ''),
                    'article': product.get('article', ''),
                    'image_url': product.get('image_url', ''),
                    'timestamp': timestamp,
                    'data_type': 'table_product',
                    'table_headers': block.get('table_headers', [])  # Сохраняем заголовки как список
                }
                
                # Добавляем все дополнительные параметры товара БЕЗ префикса
                for key, value in product.items():
                    if key not in ['name', 'url', 'article', 'image_url'] and not key.startswith('_'):
                        row[key] = str(value) if value is not None else ''
                
                excel_data_collector["all_products"].append(row)
                products_count += 1
        
        # Логируем
        excel_data_collector["parsing_log"].append({
            'timestamp': timestamp,
            'category': category_name,
            'subcategory': subcategory_name,
            'action': f'Добавлено {len(data)} блоков, {products_count} товаров',
            'data_type': 'structured_blocks'
        })
        
    elif data_type == "custom_list":
        # Обрабатываем custom_list товары
        for product in data:
            row = {
                'category': category_name,
                'subcategory': subcategory_name,
                'name': product.get('name', ''),
                'url': product.get('url', ''),
                'image_url': product.get('image_url', ''),
                'price': product.get('price', ''),
                'preorder_price': product.get('preorder_price', ''),
                'is_preorder': product.get('is_preorder', False),
                'timestamp': timestamp,
                'data_type': 'custom_list_product'
            }
            
            # Добавляем все дополнительные поля (например, характеристики)
            for key, value in product.items():
                if key not in ['name', 'url', 'image_url', 'price', 'preorder_price', 'is_preorder'] and not key.startswith('_'):
                    row[key] = str(value) if value is not None else ''
            
            excel_data_collector["all_products"].append(row)
        
        # Логируем
        excel_data_collector["parsing_log"].append({
            'timestamp': timestamp,
            'category': category_name,
            'subcategory': subcategory_name,
            'action': f'Добавлено {len(data)} товаров (custom_list)',
            'data_type': 'custom_list'
        })
    
    elif data_type == "regular_products":
        # Обрабатываем обычные товары
        for product in data:
            row = {
                'category': category_name,
                'subcategory': subcategory_name,
                'name': product.get('name', ''),
                'url': product.get('url', ''),
                'article': product.get('article', ''),
                'image_url': product.get('image_url', ''),
                'timestamp': timestamp,
                'data_type': 'regular_product'
            }
            # Добавляем все остальные поля БЕЗ переименования
            for key, value in product.items():
                if key not in ['name', 'url', 'article', 'image_url'] and not key.startswith('_'):
                    row[key] = str(value) if value is not None else ''
            
            excel_data_collector["all_products"].append(row)
        
        # Логируем
        excel_data_collector["parsing_log"].append({
            'timestamp': timestamp,
            'category': category_name,
            'subcategory': subcategory_name,
            'action': f'Добавлено {len(data)} товаров',
            'data_type': 'regular_products'
        })

def create_summary_statistics():
    """Создает сводную статистику по категориям"""
    global excel_data_collector
    
    # Группируем по категориям
    categories_stats = {}
    
    for product in excel_data_collector["all_products"]:
        cat = product.get('category', 'Неизвестная')
        subcat = product.get('subcategory', 'Неизвестная')
        data_type = product.get('data_type', 'unknown')
        
        if cat not in categories_stats:
            categories_stats[cat] = {
                'total_products': 0,
                'subcategories': {},
                'data_types': {}
            }
        
        categories_stats[cat]['total_products'] += 1
        
        if subcat not in categories_stats[cat]['subcategories']:
            categories_stats[cat]['subcategories'][subcat] = 0
        categories_stats[cat]['subcategories'][subcat] += 1
        
        if data_type not in categories_stats[cat]['data_types']:
            categories_stats[cat]['data_types'][data_type] = 0
        categories_stats[cat]['data_types'][data_type] += 1
    
    # Преобразуем в список для Excel
    summary_data = []
    for cat_name, stats in categories_stats.items():
        summary_data.append({
            'category': cat_name,
            'total_products': stats['total_products'],
            'subcategories_count': len(stats['subcategories']),
            'subcategories_list': ', '.join(stats['subcategories'].keys()),
            'data_types': ', '.join(f"{k}: {v}" for k, v in stats['data_types'].items())
        })
    
    excel_data_collector["categories_summary"] = summary_data

def save_consolidated_excel():
    """Сохраняет все данные в один Excel файл с несколькими листами"""
    global excel_data_collector
    
    if not excel_data_collector["all_products"]:
        print("❌ Нет данных для сохранения в Excel")
        return
    
    try:
        # Создаем сводную статистику
        create_summary_statistics()
        
        # Создаем имя файла
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"parsed_data_consolidated_{timestamp}.xlsx"
        filepath = os.path.join("results", filename)
        
        # Создаем директорию если её нет
        os.makedirs("results", exist_ok=True)
        
        # Отладочная информация
        data_types_count = {}
        for product in excel_data_collector["all_products"]:
            data_type = product.get('data_type', 'unknown')
            data_types_count[data_type] = data_types_count.get(data_type, 0) + 1
        
        print(f"📊 Создание консолидированного Excel файла: {filename}")
        print(f"   → Всего товаров: {len(excel_data_collector['all_products'])}")
        print(f"   → Типы данных: {data_types_count}")
        print(f"   → Категорий: {len(excel_data_collector['categories_summary'])}")
        print(f"   → Записей в логе: {len(excel_data_collector['parsing_log'])}")
        
        # Отладочная информация о полях
        if excel_data_collector["all_products"]:
            sample_product = excel_data_collector["all_products"][0]
            print(f"   🔍 Поля в первом товаре: {list(sample_product.keys())}")
            
            # Проверяем сколько товаров имеют непустой image_url
            image_url_count = sum(1 for p in excel_data_collector["all_products"] if p.get('image_url') and p.get('image_url').strip())
            print(f"   🖼️ Товаров с изображениями: {image_url_count}/{len(excel_data_collector['all_products'])}")
        
        # Создаем Excel книгу
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            
            # Группируем товары по структуре таблиц (по наборам заголовков)
            tables_by_headers = {}
            
            for product in excel_data_collector["all_products"]:
                # Определяем набор заголовков для этого товара
                headers = tuple(sorted([k for k in product.keys() if k not in ['category', 'subcategory', 'data_type', 'table_headers']]))
                
                if headers not in tables_by_headers:
                    tables_by_headers[headers] = {
                        'products': [],
                        'category': product.get('category', 'Неизвестная'),
                        'subcategory': product.get('subcategory', ''),
                        'table_headers': product.get('table_headers', [])
                    }
                
                tables_by_headers[headers]['products'].append(product)
            
            print(f"📊 Найдено {len(tables_by_headers)} различных структур таблиц")
            
            # Создаем листы для каждой структуры таблицы
            sheet_counter = 1
            for headers, table_data in tables_by_headers.items():
                products = table_data['products']
                category = table_data['category']
                subcategory = table_data['subcategory']
                
                if products:
                    # Создаем DataFrame с правильными заголовками
                    df = pd.DataFrame(products)
                    
                    # Убираем служебные колонки
                    columns_to_remove = ['data_type', 'table_headers']
                    df = df.drop(columns=[col for col in columns_to_remove if col in df.columns], errors='ignore')
                    
                    # Переупорядочиваем колонки: основные поля в начале
                    basic_columns = ['name', 'article', 'url', 'image_url', 'category', 'subcategory']
                    other_columns = [col for col in df.columns if col not in basic_columns]
                    ordered_columns = [col for col in basic_columns if col in df.columns] + other_columns
                    
                    # Отладочная информация о колонках
                    missing_basic = [col for col in basic_columns if col not in df.columns]
                    if missing_basic:
                        print(f"   ⚠️ Отсутствующие базовые колонки: {missing_basic}")
                    
                    # Убеждаемся что все базовые колонки присутствуют (даже если пустые)
                    for col in basic_columns:
                        if col not in df.columns:
                            df[col] = ''  # Добавляем пустую колонку если её нет
                    
                    # Переупорядочиваем с учетом всех базовых колонок
                    other_columns = [col for col in df.columns if col not in basic_columns]
                    ordered_columns = basic_columns + other_columns
                    df = df[ordered_columns]
                    
                    # Формируем название листа
                    if subcategory:
                        sheet_name = f"{category}_{subcategory}"[:31]
                    else:
                        sheet_name = f"{category}_таблица_{sheet_counter}"[:31]
                    
                    # Убираем недопустимые символы из имени листа
                    invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
                    for char in invalid_chars:
                        sheet_name = sheet_name.replace(char, '_')
                    
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"   ✓ Создан лист '{sheet_name}' ({len(df)} товаров, {len(df.columns)} колонок)")
                    print(f"       Колонки: {list(df.columns)}")
                    
                    sheet_counter += 1
            
            # Лист: Все товары (объединенный)
            if excel_data_collector["all_products"]:
                all_products_df = pd.DataFrame(excel_data_collector["all_products"])
                # Убираем служебные колонки
                columns_to_remove = ['table_headers']
                all_products_df = all_products_df.drop(columns=[col for col in columns_to_remove if col in all_products_df.columns], errors='ignore')
                all_products_df.to_excel(writer, sheet_name='Все товары (общий)', index=False)
                print(f"   ✓ Создан лист 'Все товары (общий)' ({len(all_products_df)} строк)")
            
            # Лист: Сводка по категориям
            if excel_data_collector["categories_summary"]:
                summary_df = pd.DataFrame(excel_data_collector["categories_summary"])
                summary_df.to_excel(writer, sheet_name='Сводка по категориям', index=False)
                print(f"   ✓ Создан лист 'Сводка по категориям' ({len(summary_df)} строк)")
            
            # Лист: Лог парсинга
            if excel_data_collector["parsing_log"]:
                log_df = pd.DataFrame(excel_data_collector["parsing_log"])
                log_df.to_excel(writer, sheet_name='Лог парсинга', index=False)
                print(f"   ✓ Создан лист 'Лог парсинга' ({len(log_df)} строк)")
            
            # Лист: Статистика по типам данных
            if data_types_count:
                stats_data = []
                for data_type, count in data_types_count.items():
                    stats_data.append({
                        'Тип данных': data_type,
                        'Количество товаров': count,
                        'Процент от общего': f"{count / len(excel_data_collector['all_products']) * 100:.1f}%"
                    })
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='Статистика типов', index=False)
                print(f"   ✓ Создан лист 'Статистика типов' ({len(stats_df)} строк)")
        
        print(f"🎉 Excel файл успешно создан: {filepath}")
        print(f"📁 Размер файла: {os.path.getsize(filepath) / 1024 / 1024:.2f} МБ")
        
        # Очищаем накопитель для следующего использования
        excel_data_collector = {
            "all_products": [],
            "categories_summary": [],
            "parsing_log": []
        }
        
        return filepath
        
    except Exception as e:
        print(f"❌ Ошибка создания Excel файла: {e}")
        return None

def create_csv_filename(category_name):
    """Создает имя файла CSV для категории"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = "".join(c for c in category_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    return f"parsed_data_{safe_name}_{timestamp}.csv"

def save_to_csv(data, filename, category_name="", subcategory_name=""):
    """Сохраняет данные в CSV файл с поддержкой русских символов и добавляет в Excel накопитель"""
    if not data:
        print(f"   → Нет данных для сохранения в {filename}")
        return
    
    # Добавляем в Excel накопитель (для обычных товаров)
    if category_name and subcategory_name:
        add_to_excel_collector(data, category_name, subcategory_name, "regular_products")
    
    try:
        # Создаем директорию если её нет
        os.makedirs("results", exist_ok=True)
        filepath = os.path.join("results", filename)
        
        # Используем UTF-8 с BOM для корректного отображения в Excel
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as csvfile:
            # Определяем заголовки на основе первого элемента
            if isinstance(data, list) and data:
                first_item = data[0]
                if isinstance(first_item, dict):
                    fieldnames = list(first_item.keys())
                else:
                    fieldnames = ['category', 'subcategory', 'item_name', 'url', 'type']
            else:
                fieldnames = ['category', 'subcategory', 'item_name', 'url', 'type']
            
            # Добавляем метаданные
            if 'category' not in fieldnames:
                fieldnames.insert(0, 'category')
            if 'subcategory' not in fieldnames:
                fieldnames.insert(1, 'subcategory')
            if 'timestamp' not in fieldnames:
                fieldnames.append('timestamp')
            
            # Используем точку с запятой как разделитель для Excel
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            
            timestamp = datetime.now().isoformat()
            
            # Записываем данные
            for item in data:
                if isinstance(item, dict):
                    row = item.copy()
                    row['category'] = category_name
                    row['timestamp'] = timestamp
                    
                    # Очищаем данные от проблемных символов
                    for key, value in row.items():
                        if isinstance(value, str):
                            # Удаляем символы, которые могут вызвать проблемы в CSV
                            row[key] = value.replace('\n', ' ').replace('\r', ' ').replace(';', ',')
                    
                    writer.writerow(row)
                
        print(f"   ✓ Сохранено {len(data)} записей в {filepath}")
        print(f"   📊 Файл готов для Excel (UTF-8 с BOM, разделитель ';')")
        
    except Exception as e:
        print(f"   ✗ Ошибка сохранения в CSV {filename}: {e}")

def save_structured_blocks_to_csv(blocks_data, filename, category_name, subcategory_name):
    """Сохраняет структурированные блоки в CSV и добавляет в Excel накопитель"""
    if not blocks_data:
        return
    
    # Добавляем в Excel накопитель
    add_to_excel_collector(blocks_data, category_name, subcategory_name, "structured_blocks")
        
    csv_data = []
    timestamp = datetime.now().isoformat()
    
    for block in blocks_data:
        block_title = block.get('block_title', 'Неизвестный блок')
        block_image = block.get('block_image', '')
        table_headers = ', '.join(block.get('table_headers', []))
        
        for product in block.get('products', []):
            row = {
                'category': category_name,
                'subcategory': subcategory_name,
                'block_title': block_title,
                'block_image': block_image,
                'table_headers': table_headers,
                'product_name': product.get('name', ''),
                'product_url': product.get('url', ''),
                'product_article': product.get('article', ''),
                'timestamp': timestamp
            }
            
            # Добавляем все дополнительные параметры товара
            for key, value in product.items():
                if key not in ['name', 'url', 'article']:
                    row[f'param_{key}'] = str(value)
            
            csv_data.append(row)
    
    if csv_data:
        save_to_csv(csv_data, filename, category_name, subcategory_name)

def save_custom_list_to_csv(products_data, filename, category_name, subcategory_name):
    """Сохраняет данные custom_list в CSV и добавляет в Excel накопитель"""
    if not products_data:
        return
    
    # Добавляем в Excel накопитель
    add_to_excel_collector(products_data, category_name, subcategory_name, "custom_list")
        
    csv_data = []
    timestamp = datetime.now().isoformat()
    
    for product in products_data:
        row = {
            'category': category_name,
            'subcategory': subcategory_name,
            'product_name': product.get('name', ''),
            'product_url': product.get('url', ''),
            'image_url': product.get('image_url', ''),
            'price': product.get('price', ''),
            'preorder_price': product.get('preorder_price', ''),
            'is_preorder': product.get('is_preorder', False),
            'timestamp': timestamp
        }
        csv_data.append(row)
    
    if csv_data:
        save_to_csv(csv_data, filename, category_name, subcategory_name)

def fix_existing_csv_files():
    """Исправляет существующие CSV файлы: кодировка + разделители для Excel"""
    if not os.path.exists("results"):
        print("Папка results не найдена")
        return
    
    csv_files = [f for f in os.listdir("results") if f.endswith('.csv')]
    if not csv_files:
        print("CSV файлы не найдены")
        return
    
    print(f"🔧 Исправление {len(csv_files)} CSV файлов для корректного отображения в Excel...")
    print("   - Исправление кодировки (UTF-8 с BOM)")
    print("   - Замена разделителей на точки с запятой")
    
    for filename in csv_files:
        try:
            filepath = os.path.join("results", filename)
            
            # Читаем с различными кодировками
            content = None
            for encoding in ['utf-8', 'utf-8-sig', 'cp1251', 'windows-1251']:
                try:
                    with open(filepath, 'r', encoding=encoding) as file:
                        content = file.read()
                    break
                except UnicodeDecodeError:
                    continue
            
            if content is None:
                print(f"   ✗ Не удалось прочитать {filename} - проблема с кодировкой")
                continue
            
            # Заменяем запятые на точки с запятой (если есть)
            if ',' in content and ';' not in content:
                content = content.replace(',', ';')
            
            # Записываем с правильной кодировкой для Excel
            with open(filepath, 'w', encoding='utf-8-sig', newline='') as file:
                file.write(content)
            
            print(f"   ✓ Исправлен: {filename}")
            
        except Exception as e:
            print(f"   ✗ Ошибка исправления {filename}: {e}")
    
    print("✅ Исправление завершено!")
    print("📋 Рекомендации для открытия в Excel:")
    print("   1. Используйте 'Данные' → 'Из текста/CSV'")
    print("   2. Выберите кодировку UTF-8")
    print("   3. Разделитель: точка с запятой (;)")

def create_excel_compatible_csv():
    """Создает дополнительные Excel-совместимые версии CSV файлов"""
    if not os.path.exists("results"):
        print("Папка results не найдена")
        return
    
    csv_files = [f for f in os.listdir("results") if f.endswith('.csv')]
    if not csv_files:
        print("CSV файлы не найдены")
        return
    
    print(f"📊 Создание Excel-совместимых версий {len(csv_files)} файлов...")
    
    for filename in csv_files:
        try:
            if filename.startswith('excel_'):
                continue  # Пропускаем уже обработанные файлы
                
            filepath = os.path.join("results", filename)
            excel_filename = f"excel_{filename}"
            excel_filepath = os.path.join("results", excel_filename)
            
            # Читаем оригинальный файл
            rows = []
            with open(filepath, 'r', encoding='utf-8-sig') as file:
                reader = csv.reader(file, delimiter=';')
                rows = list(reader)
            
            if not rows:
                continue
            
            # Записываем Excel-версию
            with open(excel_filepath, 'w', newline='', encoding='utf-8-sig') as file:
                writer = csv.writer(file, delimiter=';', quoting=csv.QUOTE_ALL)
                writer.writerows(rows)
            
            print(f"   ✓ Создан Excel-файл: {excel_filename}")
            
        except Exception as e:
            print(f"   ✗ Ошибка создания Excel-версии для {filename}: {e}")
    
    print("✅ Excel-совместимые файлы созданы!")

class AsyncWebDriver:
    """Обертка для WebDriver с поддержкой параллельной работы"""
    def __init__(self):
        self.driver = None
        self.lock = threading.Lock()
    
    def create_driver(self):
        """Создает новый экземпляр WebDriver"""
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--headless")  # Фоновый режим для параллельной работы
        
        self.driver = webdriver.Chrome(options=chrome_options)
        return self.driver
    
    def close(self):
        """Закрывает WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None

def process_category_async(category_data, results_queue):
    """Асинхронно обрабатывает одну категорию"""
    category_name = category_data["name"]
    subcategories = category_data["subcategories"]
    
    print(f"\n🔄 Начинается обработка категории: {category_name} ({len(subcategories)} подкатегорий)")
    
    # Создаем отдельный WebDriver для этой категории
    async_driver = AsyncWebDriver()
    driver = async_driver.create_driver()
    
    try:
        category_results = []
        
        for sub in subcategories:
            sub_name = sub["name"]
            sub_url = sub["url"]
            
            print(f"  🔍 Обработка: {sub_name}")
            
            try:
                driver.get(sub_url)
                time.sleep(1.5)
                
                # Обновляем глобальную переменную driver для функций парсинга
                globals()['driver'] = driver
                
                items = get_products()
                
                # Создаем имя файла для этой подкатегории
                filename = create_csv_filename(f"{category_name}_{sub_name}")
                
                # Обрабатываем результаты в зависимости от типа
                if isinstance(items, dict) and "structured_blocks" in items:
                    save_structured_blocks_to_csv(items["blocks"], filename, category_name, sub_name)
                    all_products = []
                    for block in items["blocks"]:
                        all_products.extend(block.get("products", []))
                    sub["products"] = all_products
                    sub["product_blocks"] = items["blocks"]
                    
                elif isinstance(items, dict) and "products" in items:
                    save_to_csv(items["products"], filename, category_name, sub_name)
                    sub["products"] = items["products"]
                    sub["table_headers"] = items.get("table_headers", [])
                    
                elif items and isinstance(items[0], dict) and "name" in items[0]:
                    if "article" not in items[0]:  # Это подкатегории
                        sub["grandchildren"] = items
                        # Обрабатываем каждую подподкатегорию
                        for grand in items:
                            try:
                                driver.get(grand["url"])
                                time.sleep(1.5)
                                grand_result = parse_structured_products()
                                
                                if isinstance(grand_result, dict) and "structured_blocks" in grand_result:
                                    grand_filename = create_csv_filename(f"{category_name}_{sub_name}_{grand['name']}")
                                    save_structured_blocks_to_csv(grand_result["blocks"], grand_filename, category_name, f"{sub_name}_{grand['name']}")
                                    grand["product_blocks"] = grand_result["blocks"]
                                    
                            except Exception as e:
                                print(f"    ✗ Ошибка обработки {grand['name']}: {e}")
                    else:  # Это товары custom_list
                        save_custom_list_to_csv(items, filename, category_name, sub_name)
                        sub["products"] = items
                        
                print(f"  ✓ Завершено: {sub_name}")
                
            except Exception as e:
                print(f"  ✗ Ошибка обработки {sub_name}: {e}")
                sub["products"] = []
        
        # Добавляем результат в очередь
        results_queue.put({
            "category": category_data,
            "status": "completed",
            "message": f"Обработано {len(subcategories)} подкатегорий"
        })
        
        print(f"✅ Завершена обработка категории: {category_name}")
        
    except Exception as e:
        results_queue.put({
            "category": category_data,
            "status": "error", 
            "message": f"Ошибка: {e}"
        })
        print(f"❌ Ошибка обработки категории {category_name}: {e}")
        
    finally:
        async_driver.close()

def get_category_name(category_element):
    try:
        name_span = category_element.find_element(By.CSS_SELECTOR, 'span.name')
        return driver.execute_script("return arguments[0].textContent;", name_span).strip()
    except:
        return "Unknown Category"
    
def get_subcategories(main_cat):
    """Находит все подкатегории у главной категории""" 
    try:
        dropdown = main_cat.find_element(
            By.XPATH,
            "./following-sibling::ul[contains(@class, 'dropdown') and contains(@class, 'scrollblock')]"
        )
        links = dropdown.find_elements(By.CSS_SELECTOR, 'a.section.option-font-bold')
        
        subcategories = []
        for link in links:
            href = link.get_attribute("href")
            text = driver.execute_script("return arguments[0].textContent;", link).strip()
            subcategories.append({"name": text, "url": href})
        return subcategories
    except Exception as e:
        print(f"Нет подкатегорий или ошибка: {e}")
        return []

# Функция парсинга под-подкатегорий(работает не всегда, т.к. иногда встречаются другие ссылки на под-подкатегории)
def get_products():
    """
    Проверяет, есть ли на странице таблица товаров.
    Если есть — парсит товары.
    Если нет — парсит под-подкатегории (внуки).
    :return: Список словарей с 'name' и 'url'
    """
    try:
        # Ждём загрузки (можно заменить на WebDriverWait)
        time.sleep(1.5)
        
        # Проверяем наличие под-подкатегорий
        tabel_warper = driver.find_elements(
            By.CSS_SELECTOR,
            "div.sections_wrapper.block"
        )

        # Проверяем наличие старой структуры товаров
        display_list = driver.find_elements(
            By.CSS_SELECTOR,
            "div.display_list.custom_list.show_un_props"
        )
        
        # Проверяем наличие новой структуры товаров
        new_structure_items = driver.find_elements(
            By.CSS_SELECTOR,
            "div.list_item.item_info.catalog-adaptive.flexbox.flexbox--row"
        )
        
        # Проверяем наличие любых товаров в списке
        any_list_items = driver.find_elements(
            By.CSS_SELECTOR,
            ".list_item.item_info.catalog-adaptive, .list_item_wrapp"
        )
        
        # Проверяем, является ли это страницей отдельного товара
        product_detail_indicators = [
            ".product-detail-gallery__container",
            ".product-main", 
            ".product-info",
            "h1[itemprop='name']"
        ]
        
        is_single_product = any(driver.find_elements(By.CSS_SELECTOR, indicator) for indicator in product_detail_indicators)

        if tabel_warper:
            print("Парсим под-подкатегории")
            return parse_grandchildren()
        elif is_single_product:
            print("🔍 Обнаружена страница отдельного товара")
            return parse_custom_list()  # parse_custom_list умеет обрабатывать отдельные товары
        elif display_list or new_structure_items or any_list_items:
            print(f"Найден список товаров (новая структура: {len(new_structure_items)}, старая: {len(display_list)}, общая: {len(any_list_items)})")
            return parse_custom_list()
        else:
            print("Найдена таблица товаров")
            return parse_structured_products()

    except Exception as e:
        print(f" Ошибка при парсинге товаров: {e}")
        return []


def parse_custom_list():
    """
    Парсит товары из custom_list с детальной информацией:
    - изображения, цены, ссылки
    - поддержка предзаказных цен
    - поддержка страниц отдельных товаров (product-detail)
    """
    products = []

    # Проверяем, является ли это страницей отдельного товара
    is_product_detail_page = False
    try:
        # Проверяем наличие элементов, характерных для страницы товара
        product_detail_indicators = [
            ".product-detail-gallery__container",
            ".product-main",
            ".product-info",
            "div[class*='product-detail']"
        ]
        
        for indicator in product_detail_indicators:
            if driver.find_elements(By.CSS_SELECTOR, indicator):
                is_product_detail_page = True
                print("🔍 Обнаружена страница отдельного товара")
                break
    except:
        pass

    if is_product_detail_page:
        # Парсим страницу отдельного товара
        return parse_single_product_page()
    
    # Ищем товары по разным селекторам (обычный режим)
    item_selectors = [
        "div.list_item.item_info.catalog-adaptive.flexbox.flexbox--row",  # Новая структура
        "div.list_item_wrapp.item_wrapp.item.item-parent.clearfix",  # Основной селектор по скриншоту
        "div.list_item_info.catalog-adaptive.flexbox",  # Альтернативный
        ".list_item.item_info.catalog-adaptive",  # Упрощенный селектор для новой структуры
        ".list_item_wrapp",
        "div.list_item", 
        "a.thumb",
        ".catalog-adaptive"
    ]
    
    list_items = []
    for selector in item_selectors:
        try:
            list_items = driver.find_elements(By.CSS_SELECTOR, selector)
            if list_items:
                print(f"✅ Найдены товары с селектором: {selector} ({len(list_items)} элементов)")
                break
            else:
                print(f"⚠️ Селектор {selector} не дал результатов")
        except Exception as e:
            print(f"❌ Ошибка с селектором {selector}: {e}")
            continue
    
    print(f"Найдено элементов custom_list: {len(list_items)}")
    
    if not list_items:
        print("❌ Товары не найдены! Попробуем диагностику...")
        # Диагностическая информация
        page_content_indicators = [
            "div.sections_wrapper.block",
            "table",
            ".catalog-adaptive",
            ".list_item",
            ".item_info"
        ]
        
        for indicator in page_content_indicators:
            elements = driver.find_elements(By.CSS_SELECTOR, indicator)
            print(f"   🔍 {indicator}: найдено {len(elements)} элементов")
        
        return []
    
    for i, item in enumerate(list_items):
        try:
            product_data = {
                "name": "Не указано",
                "url": None,
                "image_url": None,
                "price": None,
                "preorder_price": None,
                "is_preorder": False
            }
            
            # Извлекаем название и ссылку
            try:
                # Пробуем разные селекторы для ссылки на товар
                link_selectors = [
                    "a.dark_link.js-notice-block__title",  # Старая структура
                    ".list_item_wrap a[href*='/catalog/']",  # Новая структура
                    ".list_item_info a[href*='/catalog/']",  # Новая структура альтернатива
                    "a[href*='/catalog/']",
                    "a.product-link",
                    "a"
                ]
                
                product_link = None
                for selector in link_selectors:
                    try:
                        product_link = item.find_element(By.CSS_SELECTOR, selector)
                        print(f"   🔗 Ссылка найдена с селектором: {selector}")
                        break
                    except:
                        continue
                
                if product_link:
                    product_data["url"] = product_link.get_attribute('href')
                    
                    # Извлекаем название
                    try:
                        # Пробуем разные способы извлечения названия
                        name_selectors = [
                            "span.font_md",  # Старая структура
                            "span",  # Универсальный
                            ".js-notice-block__title span",  # Альтернатива
                        ]
                        
                        name_found = False
                        for name_sel in name_selectors:
                            try:
                                name_elem = product_link.find_element(By.CSS_SELECTOR, name_sel)
                                product_data["name"] = name_elem.text.strip()
                                name_found = True
                                break
                            except:
                                continue
                        
                        if not name_found:
                            product_data["name"] = product_link.text.strip() or "Без названия"
                    except:
                        product_data["name"] = product_link.text.strip() or "Без названия"
                        
            except Exception as e:
                print(f"   → Ошибка извлечения ссылки для товара {i+1}: {e}")
            
            # Извлекаем изображение из section-gallery-wrapper flexbox
            try:
                image_found = False
                
                # Ищем изображения в span элементах с data-src (ленивая загрузка)
                try:
                    span_selectors = [
                        "span.section-gallery-wrapper__item",
                        ".section-gallery-wrapper span[data-src]",
                        "span[data-src*='.jpg']",
                        "span[data-src*='.png']",
                        "span[data-src*='.jpeg']"
                    ]
                    
                    for selector in span_selectors:
                        try:
                            span_elem = item.find_element(By.CSS_SELECTOR, selector)
                            image_url = span_elem.get_attribute('data-src')
                            
                            if image_url:
                                if not image_url.startswith('http'):
                                    if image_url.startswith('//'):
                                        image_url = 'https:' + image_url
                                    elif image_url.startswith('/'):
                                        image_url = 'https://cnc1.ru' + image_url
                                product_data["image_url"] = image_url
                                image_found = True
                                break
                        except:
                            continue
                except:
                    pass
                
                # Если не найдено в span с data-src, ищем обычные img теги
                if not image_found:
                    image_selectors = [
                        ".image_block img",  # Новая структура - основной селектор
                        ".list_item_wrap .image_block img",  # Новая структура - детализированный
                        ".section-gallery-wrapper.flexbox img",  # Старая структура
                        "div.section-gallery-wrapper img", 
                        ".section-gallery-wrapper img",
                        ".item_info img",  # Новая структура альтернатива
                        "img"
                    ]
                    
                    for selector in image_selectors:
                        try:
                            image_elem = item.find_element(By.CSS_SELECTOR, selector)
                            # Проверяем и data-src и src
                            image_url = image_elem.get_attribute('data-src') or image_elem.get_attribute('src')
                            
                            if image_url:
                                if not image_url.startswith('http'):
                                    if image_url.startswith('//'):
                                        image_url = 'https:' + image_url
                                    elif image_url.startswith('/'):
                                        image_url = 'https://cnc1.ru' + image_url
                                product_data["image_url"] = image_url
                                image_found = True
                                print(f"   🖼️ Изображение найдено с селектором: {selector}")
                                break
                        except:
                            continue
                
                if not image_found:
                    print(f"   → Изображение не найдено для товара {i+1}")
                        
            except Exception as e:
                print(f"   → Ошибка извлечения изображения для товара {i+1}: {e}")
            
            # Извлекаем цену
            try:
                # Ищем обычную цену с учетом структуры со скриншота
                price_selectors = [
                    ".price_matrix_wrapper .price",  # Новая структура
                    ".cost.price.clearfix",  # Новая структура альтернатива  
                    ".information_wrap .cost.price",  # Новая структура детализированная
                    "span.values_wrapper",  # Основной селектор со скриншота (старая структура)
                    "span.price_measure",   # Альтернативный
                    ".price.font-bold.font_mxs",
                    ".values_wrapper",
                    ".price_measure", 
                    ".price",
                    "[data-currency]",
                    "[data-value*='RUB']"
                ]
                
                for selector in price_selectors:
                    try:
                        price_elem = item.find_element(By.CSS_SELECTOR, selector)
                        price_text = price_elem.text.strip()
                        if price_text and any(char.isdigit() for char in price_text):
                            product_data["price"] = price_text
                            print(f"   → Найдена цена с селектором {selector}: {price_text}")
                            break
                    except:
                        continue
                        
            except Exception as e:
                print(f"   → Ошибка извлечения цены для товара {i+1}: {e}")
            
            # Если обычной цены нет, ищем предзаказную цену
            if not product_data["price"]:
                try:
                    preorder_selectors = [
                        ".preorder_button",
                        "[data-name*='preorder']",
                        ".btn-default[href*='order']",
                        ".to-order"
                    ]
                    
                    for selector in preorder_selectors:
                        try:
                            preorder_elem = item.find_element(By.CSS_SELECTOR, selector)
                            preorder_text = preorder_elem.text.strip()
                            if preorder_text:
                                product_data["preorder_price"] = preorder_text
                                product_data["is_preorder"] = True
                                break
                        except:
                            continue
                            
                except Exception as e:
                    print(f"   → Ошибка извлечения предзаказной цены для товара {i+1}: {e}")
            
            products.append(product_data)
            
            # Логируем найденную информацию
            print(f"   → Товар {i+1}: {product_data['name']}")
            if product_data["image_url"]:
                print(f"     ├── Изображение: {product_data['image_url']}")
            if product_data["price"]:
                print(f"     ├── Цена: {product_data['price']}")
            elif product_data["preorder_price"]:
                print(f"     ├── Предзаказ: {product_data['preorder_price']}")
            if product_data["url"]:
                print(f"     └── Ссылка: {product_data['url']}")
                
        except Exception as e:
            print(f"   → Пропущен товар {i+1}: {e}")
    
    print(f"Найдено товаров в custom_list: {len(products)}")
    return products

def parse_single_product_page():
    """
    Парсит страницу отдельного товара с новой структурой CSS
    """
    try:
        product_data = {
            "name": "Не указано",
            "url": driver.current_url,
            "image_url": None,
            "price": None,
            "preorder_price": None,
            "is_preorder": False,
            "characteristics": {}
        }
        
        # Извлекаем название товара
        try:
            title_selectors = [
                "h1.product-main__title",
                "h1[itemprop='name']",
                ".product-main h1",
                ".product-info h1",
                "h1"
            ]
            
            for selector in title_selectors:
                try:
                    title_elem = driver.find_element(By.CSS_SELECTOR, selector)
                    product_data["name"] = title_elem.text.strip()
                    print(f"   ✅ Название: {product_data['name']}")
                    break
                except:
                    continue
        except Exception as e:
            print(f"   ❌ Ошибка извлечения названия: {e}")
        
        # Извлекаем изображение из product-detail-gallery__container
        try:
            image_found = False
            
            # Новые селекторы согласно вашему описанию
            image_selectors = [
                ".product-detail-gallery__container--vertical link[href]",  # В теге link, в href
                ".product-detail-gallery__container link[href]",
                ".product-detail-gallery__container a[href*='.jpg']",
                ".product-detail-gallery__container a[href*='.png']",
                ".product-detail-gallery__container a[href*='.jpeg']",
                ".product-detail-gallery__container a.fancy.popup_link",
                ".product-detail-gallery__container .fancy[href]"
            ]
            
            for selector in image_selectors:
                try:
                    image_elem = driver.find_element(By.CSS_SELECTOR, selector)
                    image_url = image_elem.get_attribute('href')
                    
                    if image_url and any(ext in image_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif']):
                        if not image_url.startswith('http'):
                            if image_url.startswith('//'):
                                image_url = 'https:' + image_url
                            elif image_url.startswith('/'):
                                image_url = 'https://cnc1.ru' + image_url
                        
                        product_data["image_url"] = image_url
                        image_found = True
                        print(f"   ✅ Изображение найдено: {image_url}")
                        break
                except Exception as e:
                    continue
            
            # Если не найдено в link/a тегах, ищем в img
            if not image_found:
                img_selectors = [
                    ".product-detail-gallery__container img[src]",
                    ".product-detail-gallery__container img[data-src]",
                    ".product-detail-gallery img"
                ]
                
                for selector in img_selectors:
                    try:
                        img_elem = driver.find_element(By.CSS_SELECTOR, selector)
                        image_url = img_elem.get_attribute('data-src') or img_elem.get_attribute('src')
                        
                        if image_url:
                            if not image_url.startswith('http'):
                                if image_url.startswith('//'):
                                    image_url = 'https:' + image_url
                                elif image_url.startswith('/'):
                                    image_url = 'https://cnc1.ru' + image_url
                            
                            product_data["image_url"] = image_url
                            image_found = True
                            print(f"   ✅ Изображение (img): {image_url}")
                            break
                    except:
                        continue
            
            if not image_found:
                print("   ❌ Изображение не найдено")
                
        except Exception as e:
            print(f"   ❌ Ошибка извлечения изображения: {e}")
        
        # Извлекаем цену из .price.font-bold.font_mxs
        try:
            price_found = False
            price_selectors = [
                ".price.font-bold.font_mxs",
                ".price.font-bold",
                ".price_detail",
                ".cost.font-bold",
                "[data-currency='RUB']",
                ".price"
            ]
            
            for selector in price_selectors:
                try:
                    price_elem = driver.find_element(By.CSS_SELECTOR, selector)
                    price_text = price_elem.text.strip()
                    
                    if price_text and any(char.isdigit() for char in price_text):
                        product_data["price"] = price_text
                        price_found = True
                        print(f"   ✅ Цена найдена: {price_text}")
                        break
                except:
                    continue
            
            # Если обычной цены нет, ищем предзаказную
            if not price_found:
                preorder_selectors = [
                    ".preorder_button",
                    "[data-name*='preorder']",
                    ".btn[href*='order']",
                    ".to-order",
                    ".order-button"
                ]
                
                for selector in preorder_selectors:
                    try:
                        preorder_elem = driver.find_element(By.CSS_SELECTOR, selector)
                        preorder_text = preorder_elem.text.strip()
                        if preorder_text:
                            product_data["preorder_price"] = preorder_text
                            product_data["is_preorder"] = True
                            print(f"   ✅ Предзаказная цена: {preorder_text}")
                            break
                    except:
                        continue
            
            if not price_found and not product_data["preorder_price"]:
                print("   ❌ Цена не найдена")
                
        except Exception as e:
            print(f"   ❌ Ошибка извлечения цены: {e}")
        
        # Извлекаем характеристики товара
        try:
            characteristics_table = driver.find_elements(By.CSS_SELECTOR, ".characteristics table tr")
            for row in characteristics_table:
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) >= 2:
                        key = cells[0].text.strip()
                        value = cells[1].text.strip()
                        if key and value:
                            product_data["characteristics"][key] = value
                except:
                    continue
        except:
            pass
        
        print(f"   📦 Товар обработан: {product_data['name']}")
        return [product_data]
        
    except Exception as e:
        print(f"   ❌ Ошибка парсинга страницы товара: {e}")
        return []

def get_table_headers():
    """
    Извлекает заголовки таблицы товаров.
    Возвращает список заголовков.
    """
    headers = []
    try:
        # Пробуем разные варианты селекторов для заголовков таблицы
        header_selectors = [
            "tr.table-view__item-wrapper--head th",
            "thead tr th",
            "tr:first-child th", 
            ".table-view__item-wrapper--head th",
            "div.razdel.table_all tr:first-child th",
            "table tr:first-child th"
        ]
        
        header_cells = []
        for selector in header_selectors:
            try:
                header_cells = driver.find_elements(By.CSS_SELECTOR, selector)
                if header_cells:
                    print(f" → Найдены заголовки с селектором: {selector}")
                    break
            except:
                continue
        
        if not header_cells:
            # Если не нашли заголовки, пробуем найти любые th элементы
            header_cells = driver.find_elements(By.CSS_SELECTOR, "th")
        
        for cell in header_cells:
            header_text = driver.execute_script("return arguments[0].textContent;", cell).strip()
            if header_text:
                headers.append(header_text)
        
        print(f" → Найдено заголовков таблицы: {len(headers)}")
        print(f" → Заголовки: {headers}")
        
    except Exception as e:
        print(f" → Ошибка при извлечении заголовков таблицы: {e}")
        headers = ["Артикул", "Название", "Система ЧПУ", "Макс. диаметр над станиной", "Макс. диаметр над суппортом", "Макс. длина точения", "Мощность двигателя шпинделя", "Цена", "Наличие"]
    
    return headers

def parse_structured_products():
    """
    Парсит товары по отдельным блокам, каждый с своей таблицей, заголовками и изображением.
    Возвращает список блоков товаров.
    """
    
    # Шаг 1: Проверяем, есть ли "Полный список"
    try:
        full_list_link = driver.find_element(By.CSS_SELECTOR, "div.module-pagination a.link")
        href = full_list_link.get_attribute("href")
        print(f" → Найдена пагинация. Переходим на полный список: {href}")
        driver.get(href)
        time.sleep(1.5)  # Ждём загрузки
    except:
        print(" → Ссылка 'Полный список' не найдена. Парсим текущую страницу.")

    product_blocks = []
    
    try:
        # Ищем все основные блоки с товарами по классу razdel table_all
        main_blocks = driver.find_elements(By.CSS_SELECTOR, "div.razdel.table_all")
        
        if not main_blocks:
            # Пробуем альтернативные селекторы
            main_blocks = driver.find_elements(By.CSS_SELECTOR, "div.section_info_wrapper")
            
        if not main_blocks:
            # Пробуем ещё один вариант
            main_blocks = driver.find_elements(By.CSS_SELECTOR, "div.item_block_href")
            
        if not main_blocks:
            # Если ничего не найдено, проверяем другие типы контента
            print(" → Не найдены отдельные блоки, проверяем другие типы контента")
            
            # Проверяем, является ли это страницей отдельного товара
            product_detail_indicators = [
                ".product-detail-gallery__container",
                ".product-main",
                ".product-info",
                "div[class*='product-detail']",
                "h1[itemprop='name']"
            ]
            
            is_single_product = False
            for indicator in product_detail_indicators:
                if driver.find_elements(By.CSS_SELECTOR, indicator):
                    is_single_product = True
                    print(f" → Обнаружена страница отдельного товара (индикатор: {indicator})")
                    break
            
            if is_single_product:
                # Парсим отдельный товар и возвращаем в формате structured_products
                single_product_data = parse_single_product_page()
                if single_product_data:
                    print(f" → Распарсен отдельный товар: {single_product_data[0].get('name', 'Без названия')}")
                    return {
                        "structured_blocks": [{
                            "block_title": "Отдельный товар",
                            "block_image": single_product_data[0].get('image_url', ''),
                            "table_headers": [],
                            "products": single_product_data
                        }]
                    }
            
            # Проверяем наличие товаров в новой структуре
            list_items = driver.find_elements(By.CSS_SELECTOR, "div.list_item.item_info.catalog-adaptive")
            if list_items:
                print(f" → Найдены товары в новой структуре ({len(list_items)}), используем parse_custom_list")
                return parse_custom_list()
            
            # Проверяем наличие старых таблиц
            table_elements = driver.find_elements(By.CSS_SELECTOR, "tr.main_item_wrapper")
            if table_elements:
                print(f" → Найдена таблица товаров ({len(table_elements)} строк), используем parse_table_products")
                return parse_table_products()
            
            print(" → Нет товаров для парсинга")
            return {"products": [], "table_headers": []}
            
        print(f" → Найдено блоков товаров: {len(main_blocks)}")
        
        for i, block in enumerate(main_blocks):
            try:
                block_data = {
                    "block_index": i + 1,
                    "block_title": "",
                    "block_image": None,
                    "table_headers": [],
                    "products": []
                }
                
                # Извлекаем заголовок блока (ищем в предыдущих элементах)
                try:
                    # Ищем заголовок в предыдущих siblings или в самом блоке
                    title_found = False
                    
                    # Попробуем найти заголовок в предыдущем элементе
                    try:
                        prev_element = driver.execute_script("return arguments[0].previousElementSibling;", block)
                        if prev_element:
                            title_selectors = ["h1", "h2", "h3", ".section_title", "span.font_md", ".title"]
                            for selector in title_selectors:
                                try:
                                    title_elem = prev_element.find_element(By.CSS_SELECTOR, selector)
                                    block_data["block_title"] = title_elem.text.strip()
                                    title_found = True
                                    break
                                except:
                                    continue
                    except:
                        pass
                    
                    # Если не найден в предыдущем элементе, ищем в самом блоке
                    if not title_found:
                        title_selectors = [
                            "h1", "h2", "h3", "h4", "h5",
                            ".section_title",
                            "span.font_md",
                            ".title",
                            ".item_name"
                        ]
                        for selector in title_selectors:
                            try:
                                title_elem = block.find_element(By.CSS_SELECTOR, selector)
                                block_data["block_title"] = title_elem.text.strip()
                                title_found = True
                                break
                            except:
                                continue
                    
                    if not title_found:
                        block_data["block_title"] = f"Блок товаров {i + 1}"
                        
                except:
                    block_data["block_title"] = f"Блок товаров {i + 1}"
                
                # Извлекаем изображение блока (ищем fancy popup_link href в section_img)
                try:
                    image_found = False
                    
                    # Сначала ищем изображение в самом блоке (внутри section_img)
                    try:
                        # Ищем ссылку с классом fancy popup_link внутри section_img
                        image_link = block.find_element(By.CSS_SELECTOR, "div.section_img a.fancy.popup_link")
                        image_url = image_link.get_attribute('href')
                        if image_url:
                            if not image_url.startswith('http'):
                                if image_url.startswith('//'):
                                    image_url = 'https:' + image_url
                                elif image_url.startswith('/'):
                                    image_url = 'https://cnc1.ru' + image_url
                            block_data["block_image"] = image_url
                            image_found = True
                            print(f"   → Найдено изображение блока: {image_url}")
                    except:
                        pass
                    
                    # Если не найдена ссылка, пробуем альтернативные селекторы для ссылок
                    if not image_found:
                        link_selectors = [
                            ".section_img a.fancy.popup_link",
                            ".section_img a[href*='.jpg']",
                            ".section_img a[href*='.png']",
                            ".section_img a[href*='.gif']",
                            "a.fancy.popup_link"
                        ]
                        for selector in link_selectors:
                            try:
                                image_link = block.find_element(By.CSS_SELECTOR, selector)
                                image_url = image_link.get_attribute('href')
                                if image_url:
                                    if not image_url.startswith('http'):
                                        if image_url.startswith('//'):
                                            image_url = 'https:' + image_url
                                        elif image_url.startswith('/'):
                                            image_url = 'https://cnc1.ru' + image_url
                                    block_data["block_image"] = image_url
                                    image_found = True
                                    print(f"   → Найдено изображение блока: {image_url}")
                                    break
                            except:
                                continue
                    
                    # Если не найдены ссылки, пробуем искать обычные img теги как fallback
                    if not image_found:
                        image_selectors = [
                            "div.section_img img",
                            ".section_img img",
                            "img"
                        ]
                        for selector in image_selectors:
                            try:
                                image_elem = block.find_element(By.CSS_SELECTOR, selector)
                                image_url = image_elem.get_attribute('src')
                                if image_url and not image_url.startswith('http'):
                                    if image_url.startswith('//'):
                                        image_url = 'https:' + image_url
                                    elif image_url.startswith('/'):
                                        image_url = 'https://cnc1.ru' + image_url
                                block_data["block_image"] = image_url
                                image_found = True
                                print(f"   → Найдено изображение блока (img): {image_url}")
                                break
                            except:
                                continue
                    
                    if not image_found:
                        print(f"   → Изображение блока не найдено")
                        
                except Exception as e:
                    print(f"   → Ошибка при поиске изображения блока: {e}")
                
                # Извлекаем заголовки таблицы этого блока
                try:
                    table_elem = block.find_element(By.CSS_SELECTOR, "table")
                    header_cells = table_elem.find_elements(By.CSS_SELECTOR, "th")
                    
                    for cell in header_cells:
                        header_text = driver.execute_script("return arguments[0].textContent;", cell).strip()
                        if header_text and header_text not in block_data["table_headers"]:
                            block_data["table_headers"].append(header_text)
                except:
                    # Если таблица не найдена в блоке, используем общие заголовки
                    block_data["table_headers"] = ["Артикул", "Система ЧПУ", "Характеристики", "Цена"]
                
                # Парсим товары в этом блоке
                try:
                    rows = block.find_elements(By.CSS_SELECTOR, "tr.main_item_wrapper")
                    
                    for row in rows:
                        try:
                            # Артикул и ссылка
                            try:
                                article_link = row.find_element(By.CSS_SELECTOR, "a.dark_link.js-notice-block__title")
                                article = article_link.find_element(By.TAG_NAME, "span").text.strip()
                                url = article_link.get_attribute('href')
                            except:
                                article = "Не указан"
                                url = None

                            # Название
                            try:
                                name_elem = row.find_element(By.CSS_SELECTOR, "span.font_md")
                                name = name_elem.text.strip()
                            except:
                                name = "Название не найдено"

                            # Параметры (соответствуют заголовкам таблицы этого блока)
                            props = {}
                            props_cells = row.find_elements(By.CSS_SELECTOR, "td.table-view__item-wrapper-prop")
                            for j, cell in enumerate(props_cells):
                                cell_text = cell.text.strip()
                                if j < len(block_data["table_headers"]) - 2:
                                    header_name = block_data["table_headers"][j + 2] if j + 2 < len(block_data["table_headers"]) else f"param_{j+1}"
                                    props[header_name] = cell_text
                                else:
                                    props[f"param_{j+1}"] = cell_text

                            # Добавляем товар
                            product_data = {
                                "name": name,
                                "url": url,
                                "article": article,
                                **props
                            }
                            block_data["products"].append(product_data)

                        except Exception as e:
                            print(f" → Пропущен товар в блоке {i+1}: {e}")
                            continue
                            
                except:
                    print(f" → Не найдены товары в блоке {i+1}")
                
                if block_data["products"] or block_data["block_title"]:
                    product_blocks.append(block_data)
                    print(f" → Блок {i+1}: '{block_data['block_title']}' - {len(block_data['products'])} товаров")
                    
                    # Показываем детальную информацию о блоке
                    if block_data.get("block_image"):
                        print(f"   ├── Изображение: {block_data['block_image']}")
                    else:
                        print(f"   ├── Изображение: не найдено")
                    
                    if block_data.get("table_headers"):
                        headers_str = ", ".join(block_data["table_headers"][:5])  # Показываем первые 5 заголовков
                        if len(block_data["table_headers"]) > 5:
                            headers_str += f" и ещё {len(block_data['table_headers']) - 5}"
                        print(f"   ├── Заголовки таблицы ({len(block_data['table_headers'])}): {headers_str}")
                    else:
                        print(f"   ├── Заголовки таблицы: не найдены")
                    
                    if block_data["products"]:
                        print(f"   └── Товары:")
                        for j, product in enumerate(block_data["products"][:3]):  # Показываем первые 3 товара
                            article = product.get("article", "—")
                            name = product.get("name", "Без названия")
                            print(f"       {j+1}. {name} (арт: {article})")
                        if len(block_data["products"]) > 3:
                            print(f"       ... и ещё {len(block_data['products']) - 3} товаров")
                    else:
                        print(f"   └── Товары: не найдены")
                
            except Exception as e:
                print(f" → Ошибка при обработке блока {i+1}: {e}")
                continue
    
    except Exception as e:
        print(f" → Ошибка при поиске блоков товаров: {e}")
        return parse_table_products()
    
    if not product_blocks:
        print(" → Блоки не найдены, используем общий парсинг")
        return parse_table_products()
    
    return {
        "structured_blocks": True,
        "blocks": product_blocks
    }

def parse_table_products():
    """
    Парсит товары из таблицы.
    Если есть ссылка 'Полный список' — переходит туда.
    Возвращает словарь с заголовками таблицы и списком товаров
    """
    products = []

    # Шаг 1: Проверяем, есть ли "Полный список"
    try:
        full_list_link = driver.find_element(By.CSS_SELECTOR, "div.module-pagination a.link")
        href = full_list_link.get_attribute("href")
        print(f" → Найдена пагинация. Переходим на полный список: {href}")
        driver.get(href)
        time.sleep(1.5)  # Ждём загрузки
    except:
        print(" → Ссылка 'Полный список' не найдена. Парсим текущую страницу.")

    # Шаг 1.5: Получаем заголовки таблицы
    table_headers = get_table_headers()

    # Шаг 2: Парсим товары (в любом случае — с полной страницы или текущей)
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "tr.main_item_wrapper")
        print(f" → Найдено строк с товарами: {len(rows)}")
    except Exception as e:
        print(f" → Ошибка при поиске строк товаров: {e}")
        rows = []

    for row in rows:
        try:
            # Изображение товара
            image_url = None
            try:
                # Пробуем разные варианты селекторов для изображений
                image_selectors = [
                    "div.section_img img",
                    ".section_img img", 
                    "img.preview_picture",
                    ".preview_picture",
                    "td img",
                    "img"
                ]
                
                image_elem = None
                for selector in image_selectors:
                    try:
                        image_elem = row.find_element(By.CSS_SELECTOR, selector)
                        if image_elem:
                            break
                    except:
                        continue
                
                if image_elem:
                    image_url = image_elem.get_attribute('src')
                    # Если src относительный, делаем его абсолютным
                    if image_url and not image_url.startswith('http'):
                        if image_url.startswith('//'):
                            image_url = 'https:' + image_url
                        elif image_url.startswith('/'):
                            image_url = 'https://cnc1.ru' + image_url
                else:
                    print("   → Изображение не найдено")
            except Exception as e:
                print(f"   → Ошибка при поиске изображения: {e}")

            # Артикул и ссылка
            try:
                article_link = row.find_element(By.CSS_SELECTOR, "a.dark_link.js-notice-block__title")
                article = article_link.find_element(By.TAG_NAME, "span").text.strip()
                url = article_link.get_attribute('href')
            except:
                article = "Не указан"
                url = None

            # Название
            try:
                name_elem = row.find_element(By.CSS_SELECTOR, "span.font_md")
                name = name_elem.text.strip()
            except:
                name = "Название не найдено"

            # Параметры (соответствуют заголовкам таблицы)
            props = {}
            props_cells = row.find_elements(By.CSS_SELECTOR, "td.table-view__item-wrapper-prop")
            for i, cell in enumerate(props_cells):
                cell_text = cell.text.strip()
                if i < len(table_headers) - 2:  # -2 потому что первые 2 колонки это артикул и название
                    header_name = table_headers[i + 2] if i + 2 < len(table_headers) else f"param_{i+1}"
                    props[header_name] = cell_text
                else:
                    props[f"param_{i+1}"] = cell_text

            # Добавляем товар с изображением
            product_data = {
                "name": name,
                "url": url,
                "article": article,
                "image_url": image_url,
                **props
            }
            
            # Логируем найденную информацию о товаре
            print(f"     → Товар: {name}")
            if image_url:
                print(f"       ├── Изображение: ✅")
            else:
                print(f"       ├── Изображение: ❌")
            if article and article != "Не указан":
                print(f"       ├── Артикул: {article}")
            if url:
                print(f"       └── Ссылка: ✅")
                
            products.append(product_data)

        except Exception as e:
            print(f" → Пропущен товар: {e}")
            continue

    print(f" → Найдено товаров: {len(products)}")
    
    # Возвращаем словарь с заголовками и товарами
    return {
        "table_headers": table_headers,
        "products": products
    }

def parse_grandchildren():
    """
    Парсит под-подкатегории (внуки), если нет таблицы товаров.
    Ищет ul.dropdown и собирает a.section.option-font-bold
    """
    try:

        links = driver.find_elements(By.CSS_SELECTOR, 'a.item_block_href')

        grandchildren = []
        for link in links:
            title_elem = link.find_element(By.CSS_SELECTOR, 'span.font_md')
            product_name = title_elem.text.strip()
            product_url = link.get_attribute('href')

            if product_name and product_url:
                grandchildren.append({
                    "name": product_name,
                    "url": product_url
                })

        print(f" Найдено под-подкатегорий: {len(grandchildren)}")
        return grandchildren

    except Exception as e:
        print(f" Нет под-подкатегорий или ошибка: {e}")
        return []

def parse_sub_subcategories():
    """
    Парсит под-под-подкатегории (4-й уровень вложенности)
    Ищет ссылки в catalog_section_list count_section_list_6 row items margin0 flexbox type_sections_4
    """
    try:
        # Селекторы для поиска дополнительных подкатегорий
        section_selectors = [
            ".catalog_section_list.count_section_list_6.row.items.margin0.flexbox.type_sections_4 a",
            ".catalog_section_list a.item_block_href",
            ".count_section_list_6 a",
            ".type_sections_4 a.item_block_href",
            ".catalog_section_list a"
        ]
        
        sub_subcategories = []
        
        for selector in section_selectors:
            try:
                links = driver.find_elements(By.CSS_SELECTOR, selector)
                if links:
                    print(f"🔍 Найдены под-под-подкатегории с селектором: {selector}")
                    
                    for link in links:
                        try:
                            # Пытаемся извлечь название
                            name = None
                            name_selectors = [
                                "span.font_md",
                                ".section_name",
                                "span",
                                ".name"
                            ]
                            
                            for name_sel in name_selectors:
                                try:
                                    name_elem = link.find_element(By.CSS_SELECTOR, name_sel)
                                    name = name_elem.text.strip()
                                    if name:
                                        break
                                except:
                                    continue
                            
                            # Если название не найдено, берем текст ссылки
                            if not name:
                                name = link.text.strip()
                            
                            url = link.get_attribute('href')
                            
                            if name and url and '/catalog/' in url:
                                sub_subcategories.append({
                                    "name": name,
                                    "url": url
                                })
                                
                        except Exception as e:
                            continue
                    
                    if sub_subcategories:
                        break
                        
            except Exception as e:
                continue
        
        # Удаляем дубликаты
        unique_subs = []
        seen_urls = set()
        for sub in sub_subcategories:
            if sub['url'] not in seen_urls:
                unique_subs.append(sub)
                seen_urls.add(sub['url'])
        
        print(f"🎯 Найдено уникальных под-под-подкатегорий: {len(unique_subs)}")
        return unique_subs

    except Exception as e:
        print(f"❌ Ошибка парсинга под-под-подкатегорий: {e}")
        return []

# === Ввод и запуск драйвера ===
print("Выберите режим работы:")
print("1. Полный парсинг с сохранением в Excel 📊")
print("2. Тест таблицы товаров (structured_products)")
print("3. Тест списка товаров (custom_list)")
print("4. Тест страницы отдельного товара (product-detail) 🆕")
print("5. Тест парсинга под-под-подкатегорий 🔗")
print("6. Исправить существующие CSV файлы для Excel 🔧")
print("7. Создать консолидированный Excel из CSV файлов 📊")
print("8. Тестовый парсинг одной категории 🧪")

mode_choice = input("Введите номер режима (1-8) или нажмите Enter для полного парсинга: ").strip()

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-plugins")

if mode_choice == "2":
    # Режим теста structured_products (таблицы)
    driver = webdriver.Chrome(options=chrome_options)
    test_url = input("Введите URL для теста таблиц товаров: ")
    print(f"\n→ Переход на: {test_url}")
    driver.get(test_url)
    time.sleep(2)

    # Прямо вызываем parse_structured_products
    result = parse_structured_products()
    if isinstance(result, dict) and "structured_blocks" in result:
        blocks = result["blocks"]
        total_products = sum(len(block.get("products", [])) for block in blocks)
        print(f"\nРЕЗУЛЬТАТ ТЕСТА: найдено {len(blocks)} блоков с {total_products} товарами")
        for i, block in enumerate(blocks):
            block_products = block.get("products", [])
            print(f"\nБлок {i+1}: {block['block_title']} ({len(block_products)} товаров)")
            if block.get("block_image"):
                print(f"  Изображение блока: {block['block_image']}")
            if block.get("table_headers"):
                print(f"  Заголовки: {block['table_headers']}")
            for prod in block_products[:3]:
                print(f"  • {prod['name']} (артикул: {prod.get('article', '—')})")
            if len(block_products) > 3:
                print(f"  ... и ещё {len(block_products) - 3}")
    elif isinstance(result, dict) and "products" in result:
        products = result["products"]
        headers = result.get("table_headers", [])
        print(f"\nРЕЗУЛЬТАТ ТЕСТА: найдено {len(products)} товаров")
        print(f"Заголовки таблицы: {headers}")
        for prod in products[:5]:
            print(f" • {prod['name']} (артикул: {prod.get('article', '—')}, изображение: {prod.get('image_url', 'нет')})")
    else:
        print(f"\nРЕЗУЛЬТАТ ТЕСТА: найдено {len(result)} товаров")
        for prod in result[:5]:
            print(f" • {prod['name']} (артикул: {prod.get('article', '—')})")

elif mode_choice == "3":
    # Режим теста custom_list (списки товаров)
    driver = webdriver.Chrome(options=chrome_options)
    test_url = input("Введите URL для теста списка товаров (custom_list): ")
    print(f"\n→ Переход на: {test_url}")
    driver.get(test_url)
    time.sleep(2)

    # Прямо вызываем parse_custom_list
    result = parse_custom_list()
    print(f"\nРЕЗУЛЬТАТ ТЕСТА CUSTOM_LIST: найдено {len(result)} товаров")
    
    for i, product in enumerate(result):
        print(f"\n=== Товар {i+1} ===")
        print(f"Название: {product['name']}")
        print(f"Ссылка: {product.get('url', 'не найдена')}")
        
        if product.get('image_url'):
            print(f"Изображение: {product['image_url']}")
        else:
            print("Изображение: не найдено")
            
        if product.get('price'):
            print(f"Цена: {product['price']}")
        elif product.get('preorder_price'):
            print(f"Предзаказная цена: {product['preorder_price']}")
        else:
            print("Цена: не найдена")

elif mode_choice == "4":
    # Режим теста страницы отдельного товара
    driver = webdriver.Chrome(options=chrome_options)
    test_url = input("Введите URL страницы товара для тестирования: ")
    print(f"\n🧪 ТЕСТ СТРАНИЦЫ ТОВАРА")
    print(f"→ Переход на: {test_url}")
    driver.get(test_url)
    time.sleep(3)

    # Прямо вызываем parse_single_product_page
    result = parse_single_product_page()
    
    if result:
        product = result[0]
        print(f"\n✅ РЕЗУЛЬТАТ ТЕСТА СТРАНИЦЫ ТОВАРА:")
        print(f"─" * 50)
        print(f"📦 Название: {product['name']}")
        print(f"🔗 URL: {product['url']}")
        
        if product.get('image_url'):
            print(f"🖼️ Изображение: ✅ {product['image_url']}")
        else:
            print(f"🖼️ Изображение: ❌ не найдено")
            
        if product.get('price'):
            print(f"💰 Цена: ✅ {product['price']}")
        elif product.get('preorder_price'):
            print(f"📋 Предзаказная цена: ✅ {product['preorder_price']}")
        else:
            print(f"💰 Цена: ❌ не найдена")
            
        if product.get('characteristics'):
            print(f"📋 Характеристики: найдено {len(product['characteristics'])}")
            for key, value in list(product['characteristics'].items())[:5]:
                print(f"   • {key}: {value}")
        else:
            print(f"📋 Характеристики: не найдены")
    else:
        print("\n❌ Не удалось распарсить страницу товара")

elif mode_choice == "5":
    # Режим теста под-под-подкатегорий
    driver = webdriver.Chrome(options=chrome_options)
    test_url = input("Введите URL страницы с под-под-подкатегориями: ")
    print(f"\n🧪 ТЕСТ ПАРСИНГА ПОД-ПОД-ПОДКАТЕГОРИЙ")
    print(f"→ Переход на: {test_url}")
    driver.get(test_url)
    time.sleep(3)

    # Прямо вызываем parse_sub_subcategories
    result = parse_sub_subcategories()
    
    if result:
        print(f"\n✅ РЕЗУЛЬТАТ ТЕСТА ПОД-ПОД-ПОДКАТЕГОРИЙ:")
        print(f"─" * 60)
        print(f"🔗 Найдено: {len(result)} под-под-подкатегорий")
        
        for i, sub_sub in enumerate(result):
            print(f"\n📂 {i+1}. {sub_sub['name']}")
            print(f"   🔗 URL: {sub_sub['url']}")
            
        print(f"\n🎯 Теперь можно протестировать парсинг товаров из каждой:")
        choice = input("Хотите протестировать одну из них? (введите номер 1-{} или Enter для пропуска): ".format(len(result))).strip()
        
        if choice.isdigit() and 1 <= int(choice) <= len(result):
            selected = result[int(choice) - 1]
            print(f"\n🔍 Тестируем парсинг товаров из: {selected['name']}")
            driver.get(selected['url'])
            time.sleep(3)
            
            # Парсим товары
            products_result = parse_structured_products()
            
            if isinstance(products_result, dict) and "structured_blocks" in products_result:
                blocks = products_result["blocks"]
                total_products = sum(len(block.get("products", [])) for block in blocks)
                print(f"📦 Найдено {len(blocks)} блоков с {total_products} товарами")
                
                for i, block in enumerate(blocks[:3]):  # Показываем первые 3 блока
                    block_products = block.get("products", [])
                    print(f"   Блок {i+1}: {block['block_title']} ({len(block_products)} товаров)")
                    
            elif isinstance(products_result, dict) and "products" in products_result:
                products = products_result["products"]
                print(f"📦 Найдено {len(products)} товаров")
                for prod in products[:3]:  # Показываем первые 3 товара
                    print(f"   • {prod['name']}")
                    
            else:
                print(f"📦 Найдено {len(products_result)} товаров")
                for prod in products_result[:3]:  # Показываем первые 3 товара
                    print(f"   • {prod['name']}")
    else:
        print("\n❌ Под-под-подкатегории не найдены")
        print("💡 Возможные причины:")
        print("   • Страница не содержит дополнительных подкатегорий")
        print("   • Изменилась структура CSS")
        print("   • Ошибка загрузки страницы")

elif mode_choice == "6":
    # Режим исправления CSV файлов
    fix_existing_csv_files()
    create_excel_compatible_csv() # Добавляем вызов для создания Excel-совместимых файлов
    exit()

elif mode_choice == "7":
    # Режим создания консолидированного Excel из CSV
    print("\n📊 СОЗДАНИЕ КОНСОЛИДИРОВАННОГО EXCEL ФАЙЛА")
    
    if not os.path.exists("results"):
        print("❌ Папка 'results' не найдена")
        exit()
    
    csv_files = [f for f in os.listdir("results") if f.endswith('.csv')]
    if not csv_files:
        print("❌ CSV файлы не найдены в папке 'results'")
        exit()
    
    print(f"📋 Найдено {len(csv_files)} CSV файлов")
    
    # Загружаем все CSV файлы в накопитель
    for filename in csv_files:
        try:
            filepath = os.path.join("results", filename)
            df = pd.read_csv(filepath, delimiter=';', encoding='utf-8-sig')
            
            for _, row in df.iterrows():
                excel_data_collector["all_products"].append(row.to_dict())
                
            print(f"   ✓ Загружен: {filename} ({len(df)} строк)")
            
        except Exception as e:
            print(f"   ✗ Ошибка загрузки {filename}: {e}")
    
    # Создаем консолидированный Excel
    excel_file = save_consolidated_excel()
    
    if excel_file:
        print(f"\n🎉 Консолидированный Excel файл создан: {os.path.basename(excel_file)}")
    else:
        print("\n❌ Ошибка создания Excel файла")
    
    exit()

elif mode_choice == "8":
    # Тестовый режим: парсинг одной категории
    driver = webdriver.Chrome(options=chrome_options)
    url = input("Введите URL главной страницы: ")
    driver.get(url)
    time.sleep(2)

    print("\n🧪 ТЕСТОВЫЙ РЕЖИМ: ПАРСИНГ ОДНОЙ КАТЕГОРИИ")
    print("="*60)

    # Получаем список категорий
    main_categories = driver.find_elements(By.CSS_SELECTOR, 'a.icons_fa.parent.rounded2.bordered')
    print(f'📂 Найдено категорий: {len(main_categories)}')
    
    # Показываем список категорий для выбора
    categories_list = []
    for i, main_cat in enumerate(main_categories):
        try:
            cat_name = get_category_name(main_cat)
            categories_list.append((cat_name, main_cat))
            print(f"{i+1}. {cat_name}")
        except Exception as e:
            print(f"{i+1}. Ошибка получения названия категории: {e}")
            
    # Выбор категории
    try:
        choice = input(f"\nВыберите категорию для тестирования (1-{len(categories_list)}): ").strip()
        category_index = int(choice) - 1
        
        if 0 <= category_index < len(categories_list):
            selected_category_name, selected_category_element = categories_list[category_index]
            print(f"\n🎯 Выбрана категория: {selected_category_name}")
            
            # Получаем подкатегории выбранной категории
            subcategories = get_subcategories(selected_category_element)
            print(f"📁 Найдено подкатегорий: {len(subcategories)}")
            
            # Очищаем накопитель данных
            clear_category_collector()
            
            # Парсим выбранную категорию
            parsing_state["start_time"] = datetime.now()
            
            for sub_index, sub in enumerate(subcategories):
                try:
                    sub_name = sub["name"]
                    sub_url = sub["url"]
                    print(f"\n🔍 Обработка подкатегории: {sub_name} ({sub_index + 1}/{len(subcategories)})")
                    
                    # Безопасный переход на страницу
                    if not safe_get_page(sub_url):
                        print(f"  ❌ Пропускаем {sub_name} - не удалось загрузить страницу")
                        continue
                    
                    # Безопасный парсинг
                    items = safe_parse_with_retry(get_products, f"{selected_category_name} -> {sub_name}")
                    
                    if isinstance(items, dict) and "structured_blocks" in items:
                        # Обрабатываем структурированные блоки
                        for block in items["blocks"]:
                            block_info = {
                                "block_title": block.get("block_title", ""),
                                "block_image": block.get("block_image", ""),
                                "table_headers": block.get("table_headers", [])
                            }
                            add_to_category_collector(
                                selected_category_name, 
                                sub_name, 
                                block.get("products", []), 
                                block_info
                            )
                        print(f"  ✅ Обработано {len(items['blocks'])} блоков")
                        
                    elif isinstance(items, dict) and "products" in items:
                        # Обычные товары с заголовками
                        add_to_category_collector(selected_category_name, sub_name, items["products"])
                        print(f"  ✅ Обработано {len(items['products'])} товаров")
                        
                    elif items and isinstance(items[0], dict) and "name" in items[0]:
                        if "article" not in items[0]:
                            # Это подподкатегории
                            print(f"  🔗 Найдено {len(items)} подподкатегорий")
                            for grand in items[:3]:  # Ограничиваем тестирование 3 подподкатегориями
                                try:
                                    print(f"    🔍 Парсим подподкатегорию: {grand['name']}")
                                    
                                    if not safe_get_page(grand["url"]):
                                        continue
                                    
                                    grand_result = safe_parse_with_retry(
                                        parse_structured_products, 
                                        f"{selected_category_name} -> {sub_name} -> {grand['name']}"
                                    )
                                    
                                    if isinstance(grand_result, dict) and "structured_blocks" in grand_result:
                                        for block in grand_result["blocks"]:
                                            block_info = {
                                                "block_title": block.get("block_title", ""),
                                                "block_image": block.get("block_image", ""),
                                                "table_headers": block.get("table_headers", [])
                                            }
                                            add_to_category_collector(
                                                selected_category_name, 
                                                [sub_name, grand['name']], 
                                                block.get("products", []), 
                                                block_info
                                            )
                                        print(f"    ✅ Обработано {len(grand_result['blocks'])} блоков")
                                        
                                except Exception as e:
                                    print(f"    ❌ Ошибка обработки {grand['name']}: {e}")
                        else:
                            # Это товары custom_list
                            add_to_category_collector(selected_category_name, sub_name, items)
                            print(f"  ✅ Обработано {len(items)} товаров (custom_list)")
                    else:
                        print(f"  ⚠️ Неопознанный формат данных или пустой результат")
                        
                except Exception as e:
                    print(f"  ❌ Ошибка обработки подкатегории {sub_name}: {e}")
            
            # Создаем Excel файл
            print(f"\n📊 СОЗДАНИЕ ТЕСТОВОГО EXCEL ФАЙЛА")
            print("="*50)
            
            excel_file = save_category_based_excel()
            
            if excel_file:
                print(f"\n🎉 Тестовый Excel файл создан: {os.path.basename(excel_file)}")
                print(f"📁 Файл находится в папке: results/")
                
                # Показываем статистику
                if selected_category_name in category_data_collector:
                    stats = category_data_collector[selected_category_name]["statistics"]
                    print(f"\n📈 СТАТИСТИКА ПО КАТЕГОРИИ '{selected_category_name}':")
                    print(f"   📦 Всего товаров: {stats['total_products']}")
                    print(f"   📁 Подкатегорий: {stats['total_subcategories']}")
                    print(f"   🗂️ Блоков товаров: {stats['total_blocks']}")
            else:
                print(f"\n❌ Ошибка создания Excel файла")
            
        else:
            print("❌ Неверный номер категории")
            
    except ValueError:
        print("❌ Введите корректный номер категории")
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    
    exit()

else:
    # Основной режим: парсинг всей иерархии
    driver = webdriver.Chrome(options=chrome_options)
    url = input("Введите URL главной страницы: ")
    driver.get(url)
    time.sleep(2)

    # === Шаг 1: Сбор категорий и подкатегорий ===
    main_categories = driver.find_elements(By.CSS_SELECTOR, 'a.icons_fa.parent.rounded2.bordered')
    print(f'Найдено категорий: {len(main_categories)}')

    categories_data = []

    for main_cat in main_categories:
        try:
            cat_name = get_category_name(main_cat)
            subcategories = get_subcategories(main_cat)
            
            categories_data.append({
                "name": cat_name,
                "subcategories": subcategories
            })
            print(f" Подготовлено: {cat_name} → {len(subcategories)} подкатегорий")
            
        except Exception as e:
            print(f" Ошибка при подготовке категории: {e}")

    # === Шаг 2: Переход и сбор "внуков" и товаров ===
    parsing_state["start_time"] = datetime.now()
    parsing_state["total_categories"] = len(categories_data)
    
    for cat_index, cat_data in enumerate(categories_data):
        cat_name = cat_data["name"]
        print(f"\n🏷️ Обработка категории: {cat_name} ({cat_index + 1}/{len(categories_data)})")
        
        # Перезапускаем браузер каждые 5 категорий для предотвращения проблем с памятью
        if cat_index > 0 and cat_index % 5 == 0:
            print(f"🔄 Профилактический перезапуск браузера после {cat_index} категорий")
            restart_browser()
            time.sleep(3)
        
        for sub_index, sub in enumerate(cat_data["subcategories"]):
            try:
                sub_name = sub["name"]
                sub_url = sub["url"]
                print(f"  🔍 Переход: {sub_name} ({sub_index + 1}/{len(cat_data['subcategories'])})")
                
                # Обновляем прогресс
                update_parsing_progress(cat_index, sub_index, len(categories_data))
                
                # Безопасный переход на страницу
                if not safe_get_page(sub_url):
                    print(f"  ❌ Пропускаем {sub_name} - не удалось загрузить страницу")
                    continue
                
                # Безопасный парсинг с повторными попытками
                items = safe_parse_with_retry(get_products, f"{cat_name} -> {sub_name}")
                
                sub["products"] = []
                sub["grandchildren"] = []
                sub["table_headers"] = []
                sub["product_blocks"] = []
                
                # Если результат - это структурированные блоки
                if isinstance(items, dict) and "structured_blocks" in items:
                    # Добавляем в новый накопитель данных
                    for block in items["blocks"]:
                        block_info = {
                            "block_title": block.get("block_title", ""),
                            "block_image": block.get("block_image", ""),
                            "table_headers": block.get("table_headers", [])
                        }
                        add_to_category_collector(cat_name, sub_name, block.get("products", []), block_info)
                    
                    # Для обратной совместимости
                    add_to_excel_collector(items["blocks"], cat_name, sub_name, "structured_blocks")
                    sub["product_blocks"] = items["blocks"]
                    all_products = []
                    for block in items["blocks"]:
                        all_products.extend(block.get("products", []))
                    sub["products"] = all_products
                    
                # Если результат - это словарь с заголовками и товарами
                elif isinstance(items, dict) and "products" in items:
                    # Добавляем в новый накопитель данных
                    add_to_category_collector(cat_name, sub_name, items["products"])
                    
                    # Для обратной совместимости
                    add_to_excel_collector(items["products"], cat_name, sub_name, "regular_products")
                    sub["products"] = items["products"]
                    sub["table_headers"] = items.get("table_headers", [])
                elif items and isinstance(items[0], dict) and "name" in items[0] and "url" in items[0]:
                    if "article" not in items[0]:
                        sub["grandchildren"] = items

                        for grand_index, grand in enumerate(items):
                            try:
                                print(f"      🔍 Парсим под-подкатегорию: {grand['name']} ({grand_index + 1}/{len(items)})")
                                
                                # Безопасный переход на страницу под-подкатегории
                                if not safe_get_page(grand["url"]):
                                    print(f"      ❌ Пропускаем {grand['name']} - не удалось загрузить страницу")
                                    continue
                                
                                # Проверяем наличие под-под-подкатегорий
                                sub_subcategories = safe_parse_with_retry(
                                    parse_sub_subcategories, 
                                    f"под-под-подкатегории для {grand['name']}"
                                )
                                
                                if sub_subcategories:
                                    print(f"        🔗 Найдено {len(sub_subcategories)} под-под-подкатегорий")
                                    grand["sub_subcategories"] = sub_subcategories
                                    
                                    # Парсим каждую под-под-подкатегорию
                                    for sub_sub_index, sub_sub in enumerate(sub_subcategories):
                                        try:
                                            print(f"        🔍 Парсим под-под-подкатегорию: {sub_sub['name']} ({sub_sub_index + 1}/{len(sub_subcategories)})")
                                            
                                            # Безопасный переход на страницу под-под-подкатегории
                                            if not safe_get_page(sub_sub["url"]):
                                                print(f"        ❌ Пропускаем {sub_sub['name']} - не удалось загрузить страницу")
                                                continue
                                            
                                            # Парсим товары из под-под-подкатегории
                                            sub_sub_result = safe_parse_with_retry(
                                                parse_structured_products, 
                                                f"{cat_name} -> {sub_name} -> {grand['name']} -> {sub_sub['name']}"
                                            )
                                            
                                            if isinstance(sub_sub_result, dict) and "structured_blocks" in sub_sub_result:
                                                # Добавляем в новый накопитель данных
                                                for block in sub_sub_result["blocks"]:
                                                    block_info = {
                                                        "block_title": block.get("block_title", ""),
                                                        "block_image": block.get("block_image", ""),
                                                        "table_headers": block.get("table_headers", [])
                                                    }
                                                    add_to_category_collector(cat_name, [sub_name, grand['name'], sub_sub['name']], block.get("products", []), block_info)
                                                
                                                # Для обратной совместимости
                                                add_to_excel_collector(sub_sub_result["blocks"], cat_name, f"{sub_name}_{grand['name']}_{sub_sub['name']}", "structured_blocks")
                                                sub_sub["product_blocks"] = sub_sub_result["blocks"]
                                                all_products = []
                                                for block in sub_sub_result["blocks"]:
                                                    all_products.extend(block.get("products", []))
                                                sub_sub["products"] = all_products
                                                
                                            elif isinstance(sub_sub_result, dict) and "products" in sub_sub_result:
                                                # Добавляем в новый накопитель данных
                                                add_to_category_collector(cat_name, [sub_name, grand['name'], sub_sub['name']], sub_sub_result["products"])
                                                
                                                # Для обратной совместимости
                                                add_to_excel_collector(sub_sub_result["products"], cat_name, f"{sub_name}_{grand['name']}_{sub_sub['name']}", "regular_products")
                                                sub_sub["products"] = sub_sub_result["products"]
                                                sub_sub["table_headers"] = sub_sub_result.get("table_headers", [])
                                            else:
                                                # Добавляем в новый накопитель данных (если это список товаров)
                                                if sub_sub_result:
                                                    add_to_category_collector(cat_name, [sub_name, grand['name'], sub_sub['name']], sub_sub_result)
                                                    add_to_excel_collector(sub_sub_result, cat_name, f"{sub_name}_{grand['name']}_{sub_sub['name']}", "regular_products")
                                                
                                                sub_sub["products"] = sub_sub_result or []
                                                sub_sub["table_headers"] = []
                                                
                                        except Exception as e:
                                            print(f"        ❌ Ошибка при парсинге {sub_sub['name']}: {e}")
                                            sub_sub["products"] = []
                                            sub_sub["table_headers"] = []
                                    
                                    # Если есть под-под-подкатегории, не парсим основную страницу
                                    grand["products"] = []
                                    grand["table_headers"] = []
                                    continue
                                
                                # Если нет под-под-подкатегорий, парсим обычным способом
                                grand_result = safe_parse_with_retry(
                                    parse_structured_products, 
                                    f"{cat_name} -> {sub_name} -> {grand['name']}"
                                )
                                
                                print(f"        📋 Результат парсинга: тип={type(grand_result)}, ключи={list(grand_result.keys()) if isinstance(grand_result, dict) else 'не словарь'}")
                                
                                if isinstance(grand_result, dict) and "structured_blocks" in grand_result:
                                    # Добавляем в новый накопитель данных
                                    blocks = grand_result["blocks"]
                                    for block in blocks:
                                        block_info = {
                                            "block_title": block.get("block_title", ""),
                                            "block_image": block.get("block_image", ""),
                                            "table_headers": block.get("table_headers", [])
                                        }
                                        add_to_category_collector(cat_name, [sub_name, grand['name']], block.get("products", []), block_info)
                                    
                                    # Для обратной совместимости
                                    add_to_excel_collector(blocks, cat_name, f"{sub_name}_{grand['name']}", "structured_blocks")
                                    grand["product_blocks"] = blocks
                                    all_products = []
                                    for block in blocks:
                                        all_products.extend(block.get("products", []))
                                    grand["products"] = all_products
                                    print(f"        ✅ Обработано как structured_blocks: {len(blocks)} блоков, {len(all_products)} товаров")
                                elif isinstance(grand_result, dict) and "products" in grand_result:
                                    # Добавляем в новый накопитель данных
                                    products = grand_result["products"]
                                    add_to_category_collector(cat_name, [sub_name, grand['name']], products)
                                    
                                    # Для обратной совместимости
                                    add_to_excel_collector(products, cat_name, f"{sub_name}_{grand['name']}", "regular_products")
                                    grand["products"] = products
                                    grand["table_headers"] = grand_result.get("table_headers", [])
                                    print(f"        ✅ Обработано как products: {len(products)} товаров")
                                else:
                                    # Добавляем в новый накопитель данных (если это список товаров)
                                    if grand_result:
                                        add_to_category_collector(cat_name, [sub_name, grand['name']], grand_result)
                                        add_to_excel_collector(grand_result, cat_name, f"{sub_name}_{grand['name']}", "regular_products")
                                        print(f"        ✅ Обработано как список: {len(grand_result) if isinstance(grand_result, list) else 'не список'} товаров")
                                    else:
                                        print(f"        ❌ Пустой результат")
                                    
                                    grand["products"] = grand_result or []
                                    grand["table_headers"] = []
                                    
                            except Exception as e:
                                print(f"      Ошибка при парсинге товаров из {grand['name']}: {e}")
                                grand["products"] = []
                                grand["table_headers"] = []

                    else:
                        # Это товары custom_list
                        add_to_category_collector(cat_name, sub_name, items)
                        add_to_excel_collector(items, cat_name, sub_name, "custom_list")
                        sub["products"] = items
                else:
                    sub["products"] = []

            except Exception as e:
                print(f" Ошибка при обработке подкатегории {sub_name}: {e}")
                sub["products"] = []

    # === Вывод результата ===
    print("\n" + "="*60)
    print("РЕЗУЛЬТАТ: Иерархия с товарами")
    print("="*60)

    for item in categories_data:
        print(f" Категория: {item['name']}")
        for sub in item["subcategories"]:
            grandchildren = sub.get("grandchildren", [])
            products = sub.get("products", [])
            table_headers = sub.get("table_headers", [])
            product_blocks = sub.get("product_blocks", [])

            if grandchildren:
                print(f"  ├── {sub['name']} → {len(grandchildren)} под-подкатегорий")
                for grand in grandchildren:
                    grand_products = grand.get("products", [])
                    grand_headers = grand.get("table_headers", [])
                    grand_blocks = grand.get("product_blocks", [])
                    
                    if grand_blocks:
                        print(f"  │    ├── {grand['name']} → {len(grand_blocks)} блоков товаров")
                        for block in grand_blocks:
                            block_products = block.get("products", [])
                            print(f"  │    │    ├── {block['block_title']} ({len(block_products)} товаров)")
                            if block.get("block_image"):
                                print(f"  │    │    │    Изображение блока: {block['block_image']}")
                            if block.get("table_headers"):
                                print(f"  │    │    │    Заголовки: {block['table_headers']}")
                            for prod in block_products[:3]:
                                print(f"  │    │    │    • {prod['name']}")
                            if len(block_products) > 3:
                                print(f"  │    │    │    ... и ещё {len(block_products) - 3}")
                    else:
                        print(f"  │    ├── {grand['name']} ({len(grand_products)} товаров)")
                        if grand_headers:
                            print(f"  │    │      Заголовки: {grand_headers}")
                        for prod in grand_products[:3]:
                            image_info = f" [изображение: {prod.get('image_url', 'нет')}]" if prod.get('image_url') else ""
                            print(f"  │    │      • {prod['name']}{image_info}")
                        if len(grand_products) > 3:
                            print(f"  │    │      ... и ещё {len(grand_products) - 3}")

            elif product_blocks:
                print(f"  ├── {sub['name']} → {len(product_blocks)} блоков товаров")
                for block in product_blocks:
                    block_products = block.get("products", [])
                    print(f"  │    ├── {block['block_title']} ({len(block_products)} товаров)")
                    if block.get("block_image"):
                        print(f"  │    │    Изображение блока: {block['block_image']}")
                    if block.get("table_headers"):
                        print(f"  │    │    Заголовки: {block['table_headers']}")
                    for prod in block_products[:3]:
                        print(f"  │    │    • {prod['name']}")
                    if len(block_products) > 3:
                        print(f"  │    │    ... и ещё {len(block_products) - 3}")
                        
            elif products:
                print(f"  ├── {sub['name']} ({len(products)} товаров)")
                if table_headers:
                    print(f"  │      Заголовки: {table_headers}")
                for prod in products[:3]:
                    image_info = f" [изображение: {prod.get('image_url', 'нет')}]" if prod.get('image_url') else ""
                    print(f"  │      • {prod['name']}{image_info}")
                if len(products) > 3:
                    print(f"  │      ... и ещё {len(products) - 3}")
            else:
                print(f"  ├── {sub['name']} (нет товаров)")
        print()

    # === Финальное сохранение и статистика ===
    print("\n" + "="*60)
    print("ЗАВЕРШЕНИЕ ПАРСИНГА")
    print("="*60)
    
    # Сохраняем финальный прогресс
    save_progress_checkpoint()
    
    # Показываем статистику
    end_time = datetime.now()
    total_time = end_time - parsing_state["start_time"] if parsing_state["start_time"] else "неизвестно"
    
    print(f"⏱️ Время парсинга: {total_time}")
    print(f"📊 Обработано элементов: {parsing_state['processed_items']}")
    print(f"📦 Собрано товаров: {len(excel_data_collector['all_products'])}")
    
    # === Создание Excel файлов ===
    print("\n" + "="*60)
    print("СОЗДАНИЕ EXCEL ФАЙЛОВ")
    print("="*60)
    
    # Создаем новый структурированный Excel файл по категориям
    category_excel_file = save_category_based_excel()
    
    # Создаем также старый консолидированный файл для совместимости
    consolidated_excel_file = save_consolidated_excel()
    
    if category_excel_file or consolidated_excel_file:
        print(f"\n🎉 Парсинг успешно завершен!")
        if category_excel_file:
            print(f"📊 Структурированные данные по категориям: {os.path.basename(category_excel_file)}")
        if consolidated_excel_file:
            print(f"📊 Консолидированный файл: {os.path.basename(consolidated_excel_file)}")
        print(f"📁 Файлы находятся в папке: results/")
        print(f"⏱️ Общее время работы: {total_time}")
        
        # Показываем статистику по категориям
        if category_data_collector:
            print(f"\n📈 ИТОГОВАЯ СТАТИСТИКА:")
            total_products = 0
            total_blocks = 0
            for cat_name, cat_data in category_data_collector.items():
                stats = cat_data["statistics"]
                total_products += stats["total_products"]
                total_blocks += stats["total_blocks"]
                print(f"   📂 {cat_name}: {stats['total_products']} товаров в {stats['total_blocks']} блоках")
            print(f"   🎯 ИТОГО: {total_products} товаров в {total_blocks} блоках по {len(category_data_collector)} категориям")
    else:
        print(f"\n⚠️ Excel файлы не были созданы")
        print(f"📁 Проверьте промежуточные файлы в папке 'results/'")
        print(f"💾 Данные сохранены в checkpoint файлах")

# === Завершение ===
driver.quit() 

def add_to_category_collector(category_name, subcategory_path, product_data, block_info=None):
    """
    Добавляет данные в структурированный накопитель по категориям
    
    Args:
        category_name: Название основной категории
        subcategory_path: Путь подкатегорий (список или строка)
        product_data: Данные товара/товаров
        block_info: Информация о блоке (заголовок, изображение, заголовки таблицы)
    """
    global category_data_collector
    
    if category_name not in category_data_collector:
        category_data_collector[category_name] = {
            "products": [],
            "subcategories": {},
            "blocks": [],
            "statistics": {
                "total_products": 0,
                "total_subcategories": 0,
                "total_blocks": 0
            }
        }
    
    # Преобразуем путь подкатегорий в строку
    if isinstance(subcategory_path, list):
        subcategory_key = " → ".join(subcategory_path)
    else:
        subcategory_key = str(subcategory_path)
    
    timestamp = datetime.now().isoformat()
    
    # Если это блок товаров (structured_blocks)
    if block_info and isinstance(product_data, list):
        block_data = {
            "block_title": block_info.get("block_title", "Неизвестный блок"),
            "block_image": block_info.get("block_image", ""),
            "table_headers": block_info.get("table_headers", []),
            "subcategory_path": subcategory_key,
            "timestamp": timestamp,
            "products": []
        }
        
        for product in product_data:
            enhanced_product = {
                "category": category_name,
                "subcategory_path": subcategory_key,
                "block_title": block_info.get("block_title", ""),
                "block_image": block_info.get("block_image", ""),
                "name": product.get("name", ""),
                "article": product.get("article", ""),
                "url": product.get("url", ""),
                "image_url": product.get("image_url", ""),
                "timestamp": timestamp
            }
            
            # Добавляем все остальные характеристики товара
            for key, value in product.items():
                if key not in ["name", "article", "url", "image_url"]:
                    enhanced_product[key] = value
            
            block_data["products"].append(enhanced_product)
            category_data_collector[category_name]["products"].append(enhanced_product)
        
        category_data_collector[category_name]["blocks"].append(block_data)
        category_data_collector[category_name]["statistics"]["total_blocks"] += 1
        
    # Если это обычные товары
    elif isinstance(product_data, list):
        for product in product_data:
            enhanced_product = {
                "category": category_name,
                "subcategory_path": subcategory_key,
                "block_title": "",
                "block_image": "",
                "name": product.get("name", ""),
                "article": product.get("article", ""),
                "url": product.get("url", ""),
                "image_url": product.get("image_url", ""),
                "price": product.get("price", ""),
                "timestamp": timestamp
            }
            
            # Добавляем все остальные характеристики товара
            for key, value in product.items():
                if key not in ["name", "article", "url", "image_url", "price"]:
                    enhanced_product[key] = value
                    
            category_data_collector[category_name]["products"].append(enhanced_product)
    
    # Обновляем статистику
    if subcategory_key not in category_data_collector[category_name]["subcategories"]:
        category_data_collector[category_name]["subcategories"][subcategory_key] = 0
        category_data_collector[category_name]["statistics"]["total_subcategories"] += 1
    
    products_count = len(product_data) if isinstance(product_data, list) else 1
    category_data_collector[category_name]["subcategories"][subcategory_key] += products_count
    category_data_collector[category_name]["statistics"]["total_products"] += products_count

def save_category_based_excel():
    """
    Сохраняет данные в Excel файл с отдельными листами для каждой категории
    """
    global category_data_collector
    
    if not category_data_collector:
        print("❌ Нет данных для сохранения")
        return None
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"parsed_data_by_categories_{timestamp}.xlsx"
        filepath = os.path.join("results", filename)
        
        # Создаем директорию если её нет
        os.makedirs("results", exist_ok=True)
        
        print(f"📊 Создание Excel файла по категориям: {filename}")
        print(f"   → Категорий: {len(category_data_collector)}")
        
        # Создаем Excel книгу
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            
            # Создаем сводный лист
            summary_data = []
            total_products = 0
            total_blocks = 0
            
            for cat_name, cat_data in category_data_collector.items():
                stats = cat_data["statistics"]
                total_products += stats["total_products"]
                total_blocks += stats["total_blocks"]
                
                summary_data.append({
                    "Категория": cat_name,
                    "Всего товаров": stats["total_products"],
                    "Подкатегорий": stats["total_subcategories"],
                    "Блоков товаров": stats["total_blocks"],
                    "Подкатегории": ", ".join(list(cat_data["subcategories"].keys())[:3]) + 
                                  (f" и ещё {len(cat_data['subcategories']) - 3}" if len(cat_data["subcategories"]) > 3 else "")
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="📊 Сводка", index=False)
            print(f"   ✓ Создан сводный лист ({len(summary_data)} категорий)")
            
            # Создаем лист для каждой категории
            for cat_name, cat_data in category_data_collector.items():
                if not cat_data["products"]:
                    continue
                
                # Создаем DataFrame из товаров категории
                df = pd.DataFrame(cat_data["products"])
                
                # Переупорядочиваем колонки: основные поля в начале
                basic_columns = ["name", "article", "url", "image_url", "subcategory_path", "block_title", "block_image"]
                other_columns = [col for col in df.columns if col not in basic_columns + ["category", "timestamp"]]
                ordered_columns = [col for col in basic_columns if col in df.columns] + other_columns
                
                # Добавляем категорию и timestamp в конец
                if "category" in df.columns:
                    ordered_columns.append("category")
                if "timestamp" in df.columns:
                    ordered_columns.append("timestamp")
                
                df = df[ordered_columns]
                
                # Переименовываем колонки для удобства
                column_mapping = {
                    "name": "Название товара",
                    "article": "Артикул",
                    "url": "Ссылка",
                    "image_url": "Изображение",
                    "subcategory_path": "Путь подкатегорий",
                    "block_title": "Название блока",
                    "block_image": "Изображение блока",
                    "category": "Категория",
                    "timestamp": "Время парсинга"
                }
                
                df = df.rename(columns=column_mapping)
                
                # Формируем название листа (ограничиваем 31 символом)
                sheet_name = cat_name[:27] + "..." if len(cat_name) > 27 else cat_name
                
                # Убираем недопустимые символы из имени листа
                invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
                for char in invalid_chars:
                    sheet_name = sheet_name.replace(char, '_')
                
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"   ✓ Создан лист '{sheet_name}' ({len(df)} товаров)")
            
            # Создаем лист с блоками товаров (если есть)
            all_blocks = []
            for cat_name, cat_data in category_data_collector.items():
                for block in cat_data["blocks"]:
                    block_summary = {
                        "Категория": cat_name,
                        "Путь подкатегорий": block["subcategory_path"],
                        "Название блока": block["block_title"],
                        "Изображение блока": block["block_image"],
                        "Заголовки таблицы": ", ".join(block["table_headers"]),
                        "Количество товаров": len(block["products"]),
                        "Время парсинга": block["timestamp"]
                    }
                    all_blocks.append(block_summary)
            
            if all_blocks:
                blocks_df = pd.DataFrame(all_blocks)
                blocks_df.to_excel(writer, sheet_name="🗂️ Блоки товаров", index=False)
                print(f"   ✓ Создан лист 'Блоки товаров' ({len(all_blocks)} блоков)")
        
        print(f"🎉 Excel файл успешно создан: {filepath}")
        print(f"📁 Размер файла: {os.path.getsize(filepath) / 1024 / 1024:.2f} МБ")
        print(f"📊 Итого: {total_products} товаров в {total_blocks} блоках")
        
        return filepath
        
    except Exception as e:
        print(f"❌ Ошибка создания Excel файла: {e}")
        return None

def clear_category_collector():
    """Очищает накопитель данных по категориям"""
    global category_data_collector
    category_data_collector = {}

