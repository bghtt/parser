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
                    'table_headers': table_headers,
                    'product_name': product.get('name', ''),
                    'product_url': product.get('url', ''),
                    'product_article': product.get('article', ''),
                    'timestamp': timestamp,
                    'data_type': 'table_product'
                }
                
                # Добавляем все дополнительные параметры товара
                for key, value in product.items():
                    if key not in ['name', 'url', 'article']:
                        row[f'param_{key}'] = str(value)
                
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
                'product_name': product.get('name', ''),
                'product_url': product.get('url', ''),
                'image_url': product.get('image_url', ''),
                'price': product.get('price', ''),
                'preorder_price': product.get('preorder_price', ''),
                'is_preorder': product.get('is_preorder', False),
                'timestamp': timestamp,
                'data_type': 'custom_list_product'
            }
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
                'product_name': product.get('name', ''),
                'product_url': product.get('url', ''),
                'timestamp': timestamp,
                'data_type': 'regular_product'
            }
            # Добавляем все остальные поля
            for key, value in product.items():
                if key not in ['name', 'url']:
                    row[key] = str(value)
            
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
        
        # Создаем Excel книгу
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            
            # Лист 1: Все товары
            if excel_data_collector["all_products"]:
                products_df = pd.DataFrame(excel_data_collector["all_products"])
                products_df.to_excel(writer, sheet_name='Все товары', index=False)
                print(f"   ✓ Создан лист 'Все товары' ({len(products_df)} строк)")
            
            # Лист 2: Сводка по категориям
            if excel_data_collector["categories_summary"]:
                summary_df = pd.DataFrame(excel_data_collector["categories_summary"])
                summary_df.to_excel(writer, sheet_name='Сводка по категориям', index=False)
                print(f"   ✓ Создан лист 'Сводка по категориям' ({len(summary_df)} строк)")
            
            # Лист 3: Лог парсинга
            if excel_data_collector["parsing_log"]:
                log_df = pd.DataFrame(excel_data_collector["parsing_log"])
                log_df.to_excel(writer, sheet_name='Лог парсинга', index=False)
                print(f"   ✓ Создан лист 'Лог парсинга' ({len(log_df)} строк)")
            
            # Лист 4: Товары по типам данных
            for data_type, count in data_types_count.items():
                if count > 0:
                    type_products = [p for p in excel_data_collector["all_products"] if p.get('data_type') == data_type]
                    if type_products:
                        type_df = pd.DataFrame(type_products)
                        safe_sheet_name = f"{data_type}"[:31]  # Excel ограничение на длину имени листа
                        type_df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                        print(f"   ✓ Создан лист '{safe_sheet_name}' ({len(type_df)} товаров)")
            
            # Лист 5: Товары по категориям (разделенные)
            categories = set(product.get('category', 'Неизвестная') for product in excel_data_collector["all_products"])
            for category in list(categories)[:5]:  # Максимум 5 листов категорий
                cat_products = [p for p in excel_data_collector["all_products"] if p.get('category') == category]
                if cat_products:
                    cat_df = pd.DataFrame(cat_products)
                    safe_sheet_name = f"Кат_{category}"[:31]  # Excel ограничение на длину имени листа
                    cat_df.to_excel(writer, sheet_name=safe_sheet_name, index=False)
                    print(f"   ✓ Создан лист '{safe_sheet_name}' ({len(cat_df)} товаров)")
        
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
        tabel_warper = driver.find_elements(
            By.CSS_SELECTOR,
            "div.sections_wrapper.block"
        )

        display_list = driver.find_elements(
            By.CSS_SELECTOR,
            "div.display_list.custom_list.show_un_props"
        )

        if tabel_warper:
            print("Парсим под-подкатегории")
            return parse_grandchildren()
        elif display_list:
            print("Найден список товаров")
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
    """
    products = []

    # Ищем товары по разным селекторам
    item_selectors = [
        "div.list_item_wrapp.item_wrapp.item.item-parent.clearfix",  # Основной селектор по скриншоту
        "div.list_item_info.catalog-adaptive.flexbox",  # Альтернативный
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
                print(f"Найдены товары с селектором: {selector}")
                break
        except:
            continue
    
    print(f"Найдено элементов custom_list: {len(list_items)}")
    
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
                    "a.dark_link.js-notice-block__title",
                    "a[href*='/catalog/']",
                    "a.product-link",
                    "a"
                ]
                
                product_link = None
                for selector in link_selectors:
                    try:
                        product_link = item.find_element(By.CSS_SELECTOR, selector)
                        break
                    except:
                        continue
                
                if product_link:
                    product_data["url"] = product_link.get_attribute('href')
                    
                    # Извлекаем название
                    try:
                        name_elem = product_link.find_element(By.TAG_NAME, "span")
                        product_data["name"] = name_elem.text.strip()
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
                        ".section-gallery-wrapper.flexbox img",
                        "div.section-gallery-wrapper img", 
                        ".section-gallery-wrapper img",
                        ".image_block img",
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
                    "span.values_wrapper",  # Основной селектор со скриншота
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
            # Если ничего не найдено, возвращаемся к старому методу
            print(" → Не найдены отдельные блоки, используем общий парсинг")
            return parse_table_products()
            
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

# === Ввод и запуск драйвера ===
print("Выберите режим работы:")
print("1. Полный парсинг иерархии (по умолчанию)")
print("2. Асинхронный парсинг с сохранением в CSV 🚀")
print("3. Тест таблицы товаров (structured_products)")
print("4. Тест списка товаров (custom_list)")
print("5. Исправить существующие CSV файлы для Excel 🔧")
print("6. Создать консолидированный Excel из CSV файлов 📊")

mode_choice = input("Введите номер режима (1-6) или нажмите Enter для полного парсинга: ").strip()

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-plugins")

if mode_choice == "2":
    # === АСИНХРОННЫЙ РЕЖИМ С CSV ===
    print("\n🚀 АСИНХРОННЫЙ РЕЖИМ ПАРСИНГА")
    print("Каждая категория будет обработана параллельно")
    print("Результаты будут сохранены в CSV файлы в папке 'results/'")
    
    # Спрашиваем количество потоков
    max_workers = input("Количество параллельных потоков (по умолчанию 3): ").strip()
    try:
        max_workers = int(max_workers) if max_workers else 3
    except:
        max_workers = 3
    
    url = input("Введите URL главной страницы: ")
    
    # Создаем один драйвер для сбора структуры категорий
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        print(f"\n→ Переход на: {url}")
        driver.get(url)
        time.sleep(2)

        # === Шаг 1: Сбор категорий и подкатегорий ===
        main_categories = driver.find_elements(By.CSS_SELECTOR, 'a.icons_fa.parent.rounded2.bordered')
        print(f'📋 Найдено категорий: {len(main_categories)}')

        categories_data = []
        for main_cat in main_categories:
            try:
                cat_name = get_category_name(main_cat)
                subcategories = get_subcategories(main_cat)
                
                categories_data.append({
                    "name": cat_name,
                    "subcategories": subcategories
                })
                print(f" ✓ Подготовлено: {cat_name} → {len(subcategories)} подкатегорий")
                
            except Exception as e:
                print(f" ✗ Ошибка при подготовке категории: {e}")
        
        # Закрываем основной драйвер
        driver.quit()
        
        # === Шаг 2: Асинхронная обработка ===
        print(f"\n🔥 Запуск параллельной обработки {len(categories_data)} категорий в {max_workers} потоков")
        
        results_queue = queue.Queue()
        
        # Используем ThreadPoolExecutor для параллельной обработки
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Запускаем обработку каждой категории в отдельном потоке
            futures = []
            for category_data in categories_data:
                future = executor.submit(process_category_async, category_data, results_queue)
                futures.append(future)
            
            # Отслеживаем прогресс
            completed = 0
            total = len(categories_data)
            
            print(f"\n📊 Прогресс обработки:")
            while completed < total:
                try:
                    result = results_queue.get(timeout=30)
                    completed += 1
                    
                    category_name = result["category"]["name"]
                    status = result["status"]
                    message = result["message"]
                    
                    if status == "completed":
                        print(f"✅ [{completed}/{total}] {category_name}: {message}")
                    else:
                        print(f"❌ [{completed}/{total}] {category_name}: {message}")
                        
                except queue.Empty:
                    print("⏳ Ожидание завершения обработки...")
                    continue
            
            # Дожидаемся завершения всех задач
            for future in futures:
                future.result()
        
        print(f"\n🎉 ПАРСИНГ ЗАВЕРШЕН!")
        print(f"✅ Обработано {len(categories_data)} категорий")
        
        # Создаем консолидированный Excel файл
        print(f"\n📊 Создание консолидированного Excel файла...")
        excel_file = save_consolidated_excel()
        
        if excel_file:
            print(f"📄 Результаты сохранены в единый Excel файл: {os.path.basename(excel_file)}")
        else:
            print(f"📁 Результаты сохранены в папке 'results/' (отдельные CSV файлы)")
            
            # Показываем список созданных файлов только если Excel не создался
            if os.path.exists("results"):
                files = os.listdir("results")
                csv_files = [f for f in files if f.endswith('.csv')]
                print(f"📄 Создано {len(csv_files)} CSV файлов:")
                for f in sorted(csv_files)[:10]:  # Показываем первые 10
                    print(f"   • {f}")
                if len(csv_files) > 10:
                    print(f"   ... и ещё {len(csv_files) - 10} файлов")
                
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    finally:
        if 'driver' in locals():
            driver.quit()

elif mode_choice == "3":
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

elif mode_choice == "4":
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
            
        if product.get('is_preorder'):
            print("Статус: предзаказ")
        else:
            print("Статус: в наличии")

elif mode_choice == "5":
    # Режим исправления CSV файлов
    fix_existing_csv_files()
    create_excel_compatible_csv() # Добавляем вызов для создания Excel-совместимых файлов
    exit()

elif mode_choice == "6":
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
    for cat_data in categories_data:
        cat_name = cat_data["name"]
        print(f"\n Обработка категории: {cat_name}")
        
        for sub in cat_data["subcategories"]:
            try:
                sub_name = sub["name"]
                sub_url = sub["url"]
                print(f"  → Переход: {sub_name} → {sub_url}")
                
                driver.get(sub_url)
                items = get_products()
                
                sub["products"] = []
                sub["grandchildren"] = []
                sub["table_headers"] = []
                sub["product_blocks"] = []
                
                # Если результат - это структурированные блоки
                if isinstance(items, dict) and "structured_blocks" in items:
                    sub["product_blocks"] = items["blocks"]
                    # Для обратной совместимости, также заполняем products
                    all_products = []
                    for block in items["blocks"]:
                        all_products.extend(block.get("products", []))
                    sub["products"] = all_products
                    
                # Если результат - это словарь с заголовками и товарами
                elif isinstance(items, dict) and "products" in items:
                    sub["products"] = items["products"]
                    sub["table_headers"] = items.get("table_headers", [])
                elif items and isinstance(items[0], dict) and "name" in items[0] and "url" in items[0]:
                    if "article" not in items[0]:
                        sub["grandchildren"] = items

                        for grand in items:
                            try:
                                print(f"      → Парсим товары из под-подкатегории: {grand['name']} → {grand['url']}")
                                driver.get(grand["url"])
                                time.sleep(1.5)
                                grand_result = parse_structured_products()
                                
                                if isinstance(grand_result, dict) and "structured_blocks" in grand_result:
                                    grand["product_blocks"] = grand_result["blocks"]
                                    # Для обратной совместимости
                                    all_products = []
                                    for block in grand_result["blocks"]:
                                        all_products.extend(block.get("products", []))
                                    grand["products"] = all_products
                                elif isinstance(grand_result, dict) and "products" in grand_result:
                                    grand["products"] = grand_result["products"]
                                    grand["table_headers"] = grand_result.get("table_headers", [])
                                else:
                                    grand["products"] = grand_result
                                    grand["table_headers"] = []
                                    
                            except Exception as e:
                                print(f"      Ошибка при парсинге товаров из {grand['name']}: {e}")
                                grand["products"] = []
                                grand["table_headers"] = []

                    else:
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

# === Завершение ===
driver.quit() 