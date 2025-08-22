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

def clear_category_collector():
    """Очищает накопитель данных по категориям"""
    global category_data_collector
    category_data_collector = {}

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

# Глобальные переменные для восстановления
parsing_state = {
    "current_category": 0,
    "current_subcategory": 0,
    "total_categories": 0,
    "processed_items": 0,
    "last_successful_url": "",
    "start_time": None
}

# === РЕЖИМЫ ЗАПУСКА ===
print("Выберите режим работы:")
print("8. Тестовый парсинг одной категории 🧪")

mode_choice = input("Введите номер режима (8): ").strip()

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-plugins")

if mode_choice == "8":
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
            # Извлекаем название категории
            name_span = main_cat.find_element(By.CSS_SELECTOR, 'span.name')
            cat_name = driver.execute_script("return arguments[0].textContent;", name_span).strip()
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
            try:
                dropdown = selected_category_element.find_element(
                    By.XPATH,
                    "./following-sibling::ul[contains(@class, 'dropdown') and contains(@class, 'scrollblock')]"
                )
                links = dropdown.find_elements(By.CSS_SELECTOR, 'a.section.option-font-bold')
                
                subcategories = []
                for link in links:
                    href = link.get_attribute("href")
                    text = driver.execute_script("return arguments[0].textContent;", link).strip()
                    subcategories.append({"name": text, "url": href})
                    
            except Exception as e:
                print(f"Нет подкатегорий или ошибка: {e}")
                subcategories = []
                
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
                    
                    # Простейший переход на страницу
                    print(f"   🌐 Переход на: {sub_url}")
                    driver.get(sub_url)
                    time.sleep(3)
                    
                    # Простейший парсинг - просто ищем товары
                    print(f"   🔍 Поиск товаров...")
                    
                    # Пробуем найти любые товары на странице
                    product_selectors = [
                        "div.list_item.item_info.catalog-adaptive",
                        "tr.main_item_wrapper",
                        "a.item_block_href"
                    ]
                    
                    found_products = []
                    for selector in product_selectors:
                        try:
                            elements = driver.find_elements(By.CSS_SELECTOR, selector)
                            if elements:
                                print(f"   ✅ Найдено {len(elements)} элементов с селектором: {selector}")
                                for i, elem in enumerate(elements[:5]):  # Ограничиваем 5 товарами для теста
                                    try:
                                        name = elem.text.strip()[:100] if elem.text.strip() else f"Товар {i+1}"
                                        url = elem.get_attribute('href') if elem.tag_name == 'a' else ""
                                        
                                        product = {
                                            "name": name,
                                            "url": url,
                                            "article": f"TEST_{i+1}",
                                            "image_url": ""
                                        }
                                        found_products.append(product)
                                        print(f"     → {name[:50]}...")
                                    except:
                                        continue
                                break
                        except:
                            continue
                    
                    if found_products:
                        add_to_category_collector(selected_category_name, sub_name, found_products)
                        print(f"  ✅ Обработано {len(found_products)} товаров")
                    else:
                        print(f"  ⚠️ Товары не найдены")
                        
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

# === Завершение ===
driver.quit() 