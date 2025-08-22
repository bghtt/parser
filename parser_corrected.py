from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import csv
import os
from datetime import datetime
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill, Alignment

# === Глобальные переменные ===

# Структурированный накопитель данных по категориям
category_data_collector = {}

def clear_category_collector():
    """Очищает накопитель данных по категориям"""
    global category_data_collector
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

def add_to_category_collector(category_name, subcategory_path, product_data, block_info=None, is_structured_block=False):
    """
    Добавляет данные в структурированный накопитель по категориям
    
    Args:
        category_name: Название основной категории
        subcategory_path: Путь подкатегорий (список или строка)
        product_data: Данные товара/товаров
        block_info: Информация о блоке (заголовок, изображение, заголовки таблицы)
        is_structured_block: True если это структурированный блок товаров с изображением
    """
    global category_data_collector
    
    if category_name not in category_data_collector:
        category_data_collector[category_name] = {
            "structured_data": [],  # Структурированные данные: блоки + таблицы
            "statistics": {
                "total_products": 0,
                "total_blocks": 0
            }
        }
    
    # Преобразуем путь подкатегорий в строку
    if isinstance(subcategory_path, list):
        subcategory_key = " → ".join(subcategory_path)
    else:
        subcategory_key = str(subcategory_path)
    
    timestamp = datetime.now().isoformat()
    
    if is_structured_block and block_info:
        # Это структурированный блок с изображением и таблицей товаров
        
        # 1. Добавляем строку с информацией о блоке (изображение)
        block_row = {
            "Тип записи": "БЛОК_ИЗОБРАЖЕНИЕ",
            "Категория": category_name,
            "Путь подкатегорий": subcategory_key,
            "Название блока": block_info.get("block_title", ""),
            "Изображение блока": block_info.get("block_image", ""),
            "Время парсинга": timestamp
        }
        category_data_collector[category_name]["structured_data"].append(block_row)
        
        # 2. Добавляем заголовки таблицы
        if block_info.get("table_headers"):
            headers_row = {
                "Тип записи": "ЗАГОЛОВКИ_ТАБЛИЦЫ",
                "Категория": category_name,
                "Путь подкатегорий": subcategory_key,
                "Название блока": block_info.get("block_title", ""),
            }
            # Добавляем каждый заголовок как отдельную колонку
            for i, header in enumerate(block_info["table_headers"]):
                headers_row[f"Колонка_{i+1}"] = header
            
            category_data_collector[category_name]["structured_data"].append(headers_row)
        
        # 3. Добавляем товары из таблицы
        if isinstance(product_data, list):
            for product in product_data:
                product_row = {
                    "Тип записи": "ТОВАР",
                    "Категория": category_name,
                    "Путь подкатегорий": subcategory_key,
                    "Название блока": block_info.get("block_title", ""),
                    "Артикул": product.get("article", ""),
                    "Название товара": product.get("name", ""),
                    "Ссылка на товар": product.get("url", ""),
                }
                
                # Добавляем характеристики товара в соответствующие колонки заголовков
                if block_info.get("table_headers"):
                    for i, header in enumerate(block_info["table_headers"]):
                        # Пытаемся найти значение для этого заголовка
                        value = ""
                        possible_keys = [
                            header,
                            header.lower(),
                            header.replace(" ", "_"),
                            f"param_{i+1}",
                            f"param_{i}",
                            list(product.keys())[i+3] if i+3 < len(product.keys()) else None  # +3 потому что первые 3 - name, article, url
                        ]
                        
                        for key in possible_keys:
                            if key and key in product:
                                value = str(product[key])
                                break
                        
                        product_row[f"Колонка_{i+1}"] = value
                
                # Добавляем все остальные характеристики товара
                for key, value in product.items():
                    if key not in ["name", "article", "url", "image_url"] and not key.startswith("_"):
                        if f"Доп_{key}" not in product_row:  # Избегаем дублирования
                            product_row[f"Доп_{key}"] = str(value) if value is not None else ""
                
                category_data_collector[category_name]["structured_data"].append(product_row)
        
        # Обновляем статистику
        category_data_collector[category_name]["statistics"]["total_blocks"] += 1
        category_data_collector[category_name]["statistics"]["total_products"] += len(product_data) if isinstance(product_data, list) else 0
        
    else:
        # Это обычные товары (custom_list или отдельные товары)
        if isinstance(product_data, list):
            for product in product_data:
                product_row = {
                    "Тип записи": "ТОВАР_ОТДЕЛЬНЫЙ",
                    "Категория": category_name,
                    "Путь подкатегорий": subcategory_key,
                    "Артикул": product.get("article", ""),
                    "Название товара": product.get("name", ""),
                    "Ссылка на товар": product.get("url", ""),
                    "Изображение товара": product.get("image_url", ""),  # Для отдельных товаров сохраняем изображение
                    "Цена": product.get("price", ""),
                    "Время парсинга": timestamp
                }
                
                # Добавляем все остальные характеристики товара
                for key, value in product.items():
                    if key not in ["name", "article", "url", "image_url", "price"] and not key.startswith("_"):
                        product_row[f"Доп_{key}"] = str(value) if value is not None else ""
                
                category_data_collector[category_name]["structured_data"].append(product_row)
        
        # Обновляем статистику
        category_data_collector[category_name]["statistics"]["total_products"] += len(product_data) if isinstance(product_data, list) else 1

def save_category_based_excel():
    """
    Сохраняет данные в Excel файл с правильной структурой как на сайте
    """
    global category_data_collector
    
    if not category_data_collector:
        print("❌ Нет данных для сохранения")
        return None
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"parsed_data_structured_{timestamp}.xlsx"
        filepath = os.path.join("results", filename)
        
        # Создаем директорию если её нет
        os.makedirs("results", exist_ok=True)
        
        print(f"📊 Создание структурированного Excel файла: {filename}")
        print(f"   → Категорий: {len(category_data_collector)}")
        
        # Создаем Excel книгу
        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            
            # Создаем сводный лист
            summary_data = []
            total_products = 0
            
            for cat_name, cat_data in category_data_collector.items():
                stats = cat_data["statistics"]
                total_products += stats["total_products"]
                
                summary_data.append({
                    "Категория": cat_name,
                    "Всего товаров": stats["total_products"],
                    "Блоков товаров": stats["total_blocks"]
                })
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="📊 Сводка", index=False)
            print(f"   ✓ Создан сводный лист ({len(summary_data)} категорий)")
            
            # Создаем лист для каждой категории
            for cat_name, cat_data in category_data_collector.items():
                if not cat_data["structured_data"]:
                    continue
                
                # Создаем DataFrame из структурированных данных
                df = pd.DataFrame(cat_data["structured_data"])
                
                # Формируем название листа (ограничиваем 31 символом)
                sheet_name = cat_name[:27] + "..." if len(cat_name) > 27 else cat_name
                
                # Убираем недопустимые символы из имени листа
                invalid_chars = ['\\', '/', '*', '[', ']', ':', '?']
                for char in invalid_chars:
                    sheet_name = sheet_name.replace(char, '_')
                
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                print(f"   ✓ Создан лист '{sheet_name}' ({len(df)} записей)")
                
                # Показываем типы записей в этой категории
                if "Тип записи" in df.columns:
                    type_counts = df["Тип записи"].value_counts().to_dict()
                    print(f"       Типы записей: {type_counts}")
        
        print(f"🎉 Excel файл успешно создан: {filepath}")
        print(f"📁 Размер файла: {os.path.getsize(filepath) / 1024 / 1024:.2f} МБ")
        print(f"📊 Итого: {total_products} товаров")
        
        return filepath
        
    except Exception as e:
        print(f"❌ Ошибка создания Excel файла: {e}")
        return None

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

def get_products():
    """
    Проверяет, есть ли на странице таблица товаров.
    Если есть — парсит товары.
    Если нет — парсит под-подкатегории (внуки).
    :return: Список словарей с 'name' и 'url'
    """
    try:
        # Ждём загрузки
        time.sleep(1.5)
        
        # Проверяем наличие под-подкатегорий
        tabel_warper = driver.find_elements(By.CSS_SELECTOR, "div.sections_wrapper.block")
        
        # Проверяем наличие товаров в списке
        display_list = driver.find_elements(By.CSS_SELECTOR, "div.display_list.custom_list.show_un_props")
        new_structure_items = driver.find_elements(By.CSS_SELECTOR, "div.list_item.item_info.catalog-adaptive.flexbox.flexbox--row")
        any_list_items = driver.find_elements(By.CSS_SELECTOR, ".list_item.item_info.catalog-adaptive, .list_item_wrapp")
        
        # Проверяем, является ли это страницей отдельного товара
        product_detail_indicators = [".product-detail-gallery__container", ".product-main", ".product-info", "h1[itemprop='name']"]
        is_single_product = any(driver.find_elements(By.CSS_SELECTOR, indicator) for indicator in product_detail_indicators)

        if tabel_warper:
            print("Парсим под-подкатегории")
            return parse_grandchildren()
        elif is_single_product:
            print("🔍 Обнаружена страница отдельного товара")
            return parse_custom_list()
        elif display_list or new_structure_items or any_list_items:
            print(f"Найден список товаров (новая структура: {len(new_structure_items)}, старая: {len(display_list)}, общая: {len(any_list_items)})")
            return parse_custom_list()
        else:
            print("Найдена таблица товаров")
            return parse_structured_products()

    except Exception as e:
        print(f" Ошибка при парсинге товаров: {e}")
        return []

def get_table_headers():
    """Извлекает заголовки таблицы товаров"""
    headers = []
    try:
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
            header_cells = driver.find_elements(By.CSS_SELECTOR, "th")
        
        for cell in header_cells:
            header_text = driver.execute_script("return arguments[0].textContent;", cell).strip()
            if header_text:
                headers.append(header_text)
        
        print(f" → Найдено заголовков таблицы: {len(headers)}")
        if headers:
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
        # Ищем все основные блоки с товарами
        main_blocks = driver.find_elements(By.CSS_SELECTOR, "div.razdel.table_all")
        
        if not main_blocks:
            main_blocks = driver.find_elements(By.CSS_SELECTOR, "div.section_info_wrapper")
            
        if not main_blocks:
            main_blocks = driver.find_elements(By.CSS_SELECTOR, "div.item_block_href")
            
        if not main_blocks:
            print(" → Не найдены отдельные блоки, проверяем другие типы контента")
            
            # Проверяем, является ли это страницей отдельного товара
            product_detail_indicators = [".product-detail-gallery__container", ".product-main", ".product-info", "div[class*='product-detail']", "h1[itemprop='name']"]
            
            is_single_product = False
            for indicator in product_detail_indicators:
                if driver.find_elements(By.CSS_SELECTOR, indicator):
                    is_single_product = True
                    print(f" → Обнаружена страница отдельного товара (индикатор: {indicator})")
                    break
            
            if is_single_product:
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
                
                # Извлекаем заголовок блока
                try:
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
                        title_selectors = ["h1", "h2", "h3", "h4", "h5", ".section_title", "span.font_md", ".title", ".item_name"]
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
                
                # Извлекаем изображение блока
                try:
                    image_found = False
                    
                    # Ищем ссылку с изображением
                    try:
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
                    
                    # Если не найдена ссылка, пробуем альтернативные селекторы
                    if not image_found:
                        link_selectors = [".section_img a.fancy.popup_link", ".section_img a[href*='.jpg']", ".section_img a[href*='.png']", ".section_img a[href*='.gif']", "a.fancy.popup_link"]
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
                            # Изображение товара
                            image_url = None
                            try:
                                image_selectors = ["div.section_img img", ".section_img img", "img.preview_picture", ".preview_picture", "td img", "img"]
                                
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
                                    if image_url and not image_url.startswith('http'):
                                        if image_url.startswith('//'):
                                            image_url = 'https:' + image_url
                                        elif image_url.startswith('/'):
                                            image_url = 'https://cnc1.ru' + image_url
                            except Exception as e:
                                print(f"   → Ошибка при поиске изображения товара: {e}")

                            # Артикул и ссылка
                            article = "Не указан"
                            url = None
                            try:
                                # Ищем ссылку с артикулом
                                article_link = row.find_element(By.CSS_SELECTOR, "a.dark_link.js-notice-block__title")
                                url = article_link.get_attribute('href')
                                
                                # Пробуем разные способы извлечения артикула
                                try:
                                    # Способ 1: span внутри ссылки
                                    article_span = article_link.find_element(By.TAG_NAME, "span")
                                    article = article_span.text.strip()
                                except:
                                    try:
                                        # Способ 2: текст всей ссылки
                                        article = article_link.text.strip()
                                    except:
                                        # Способ 3: ищем в первой ячейке строки
                                        try:
                                            first_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                                            article = first_cell.text.strip()
                                        except:
                                            article = "Не указан"
                            except:
                                # Если не найдена основная ссылка, ищем альтернативные способы
                                try:
                                    # Ищем в первой ячейке любую ссылку или текст
                                    first_cell = row.find_element(By.CSS_SELECTOR, "td:first-child")
                                    cell_text = first_cell.text.strip()
                                    if cell_text:
                                        article = cell_text
                                    
                                    # Ищем ссылку в этой ячейке
                                    try:
                                        cell_link = first_cell.find_element(By.TAG_NAME, "a")
                                        url = cell_link.get_attribute('href')
                                    except:
                                        pass
                                except:
                                    pass

                            # Название
                            name = "Название не найдено"
                            try:
                                # Способ 1: span.font_md
                                name_elem = row.find_element(By.CSS_SELECTOR, "span.font_md")
                                name = name_elem.text.strip()
                            except:
                                try:
                                    # Способ 2: ищем во второй ячейке
                                    second_cell = row.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                                    name = second_cell.text.strip()
                                except:
                                    try:
                                        # Способ 3: любой span в строке
                                        name_span = row.find_element(By.TAG_NAME, "span")
                                        name = name_span.text.strip()
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
                                "image_url": image_url,
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
                        headers_str = ", ".join(block_data["table_headers"][:5])
                        if len(block_data["table_headers"]) > 5:
                            headers_str += f" и ещё {len(block_data['table_headers']) - 5}"
                        print(f"   ├── Заголовки таблицы ({len(block_data['table_headers'])}): {headers_str}")
                    else:
                        print(f"   ├── Заголовки таблицы: не найдены")
                    
                    if block_data["products"]:
                        print(f"   └── Товары:")
                        for j, product in enumerate(block_data["products"][:3]):
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
    """Парсит товары из таблицы с проверкой пагинации"""
    products = []

    # Шаг 1: Проверяем, есть ли "Полный список"
    try:
        full_list_link = driver.find_element(By.CSS_SELECTOR, "div.module-pagination a.link")
        href = full_list_link.get_attribute("href")
        print(f" → Найдена пагинация. Переходим на полный список: {href}")
        driver.get(href)
        time.sleep(1.5)
    except:
        print(" → Ссылка 'Полный список' не найдена. Парсим текущую страницу.")

    # Получаем заголовки таблицы
    table_headers = get_table_headers()

    # Парсим товары
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
                image_selectors = ["div.section_img img", ".section_img img", "img.preview_picture", ".preview_picture", "td img", "img"]
                
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
                    if image_url and not image_url.startswith('http'):
                        if image_url.startswith('//'):
                            image_url = 'https:' + image_url
                        elif image_url.startswith('/'):
                            image_url = 'https://cnc1.ru' + image_url
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
                if i < len(table_headers) - 2:
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
    
    return {
        "table_headers": table_headers,
        "products": products
    }

def parse_custom_list():
    """Парсит товары из custom_list с проверкой пагинации"""
    products = []

    # Проверяем, является ли это страницей отдельного товара
    is_product_detail_page = False
    try:
        product_detail_indicators = [".product-detail-gallery__container", ".product-main", ".product-info", "div[class*='product-detail']"]
        
        for indicator in product_detail_indicators:
            if driver.find_elements(By.CSS_SELECTOR, indicator):
                is_product_detail_page = True
                print("🔍 Обнаружена страница отдельного товара")
                break
    except:
        pass

    if is_product_detail_page:
        return parse_single_product_page()
    
    # Проверяем пагинацию
    try:
        full_list_link = driver.find_element(By.CSS_SELECTOR, "div.module-pagination a.link")
        href = full_list_link.get_attribute("href")
        print(f" → Найдена пагинация. Переходим на полный список: {href}")
        driver.get(href)
        time.sleep(1.5)
    except:
        print(" → Пагинация не найдена. Парсим текущую страницу.")
    
    # Ищем товары
    item_selectors = [
        "div.list_item.item_info.catalog-adaptive.flexbox.flexbox--row",
        "div.list_item_wrapp.item_wrapp.item.item-parent.clearfix",
        "div.list_item_info.catalog-adaptive.flexbox",
        ".list_item.item_info.catalog-adaptive",
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
        except Exception as e:
            continue
    
    print(f"Найдено элементов custom_list: {len(list_items)}")
    
    if not list_items:
        print("❌ Товары не найдены!")
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
                link_selectors = [
                    "a.dark_link.js-notice-block__title",
                    ".list_item_wrap a[href*='/catalog/']",
                    ".list_item_info a[href*='/catalog/']",
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
                        name_selectors = ["span.font_md", "span", ".js-notice-block__title span"]
                        
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
            
            # Извлекаем изображение
            try:
                image_found = False
                
                # Ищем изображения в span элементах с data-src
                try:
                    span_selectors = ["span.section-gallery-wrapper__item", ".section-gallery-wrapper span[data-src]", "span[data-src*='.jpg']", "span[data-src*='.png']", "span[data-src*='.jpeg']"]
                    
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
                    image_selectors = [".image_block img", ".list_item_wrap .image_block img", ".section-gallery-wrapper.flexbox img", "div.section-gallery-wrapper img", ".section-gallery-wrapper img", ".item_info img", "img"]
                    
                    for selector in image_selectors:
                        try:
                            image_elem = item.find_element(By.CSS_SELECTOR, selector)
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
                        
            except Exception as e:
                print(f"   → Ошибка извлечения изображения для товара {i+1}: {e}")
            
            # Извлекаем цену
            try:
                price_selectors = [".price_matrix_wrapper .price", ".cost.price.clearfix", ".information_wrap .cost.price", "span.values_wrapper", "span.price_measure", ".price.font-bold.font_mxs", ".values_wrapper", ".price_measure", ".price", "[data-currency]", "[data-value*='RUB']"]
                
                for selector in price_selectors:
                    try:
                        price_elem = item.find_element(By.CSS_SELECTOR, selector)
                        price_text = price_elem.text.strip()
                        if price_text and any(char.isdigit() for char in price_text):
                            product_data["price"] = price_text
                            break
                    except:
                        continue
                        
            except Exception as e:
                print(f"   → Ошибка извлечения цены для товара {i+1}: {e}")
            
            products.append(product_data)
                
        except Exception as e:
            print(f"   → Пропущен товар {i+1}: {e}")
    
    print(f"Найдено товаров в custom_list: {len(products)}")
    return products

def parse_single_product_page():
    """Парсит страницу отдельного товара"""
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
            title_selectors = ["h1.product-main__title", "h1[itemprop='name']", ".product-main h1", ".product-info h1", "h1"]
            
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
        
        # Извлекаем изображение
        try:
            image_found = False
            
            image_selectors = [".product-detail-gallery__container--vertical link[href]", ".product-detail-gallery__container link[href]", ".product-detail-gallery__container a[href*='.jpg']", ".product-detail-gallery__container a[href*='.png']", ".product-detail-gallery__container a[href*='.jpeg']", ".product-detail-gallery__container a.fancy.popup_link", ".product-detail-gallery__container .fancy[href]"]
            
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
            
            if not image_found:
                img_selectors = [".product-detail-gallery__container img[src]", ".product-detail-gallery__container img[data-src]", ".product-detail-gallery img"]
                
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
        
        # Извлекаем цену
        try:
            price_found = False
            price_selectors = [".price.font-bold.font_mxs", ".price.font-bold", ".price_detail", ".cost.font-bold", "[data-currency='RUB']", ".price"]
            
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
            
            if not price_found:
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

def parse_grandchildren():
    """Парсит под-подкатегории (внуки)"""
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
                                block_info,
                                is_structured_block=True
                            )
                        print(f"  ✅ Обработано {len(items['blocks'])} блоков")
                        
                    elif isinstance(items, dict) and "products" in items:
                        # Обычные товары с заголовками
                        block_info = {
                            "block_title": f"Таблица товаров - {sub_name}",
                            "block_image": "",
                            "table_headers": items.get("table_headers", [])
                        }
                        add_to_category_collector(selected_category_name, sub_name, items["products"], block_info, is_structured_block=True)
                        print(f"  ✅ Обработано {len(items['products'])} товаров")
                        
                    elif items and isinstance(items[0], dict) and "name" in items[0]:
                        if "article" not in items[0]:
                            # Это подподкатегории - ограничиваем для теста
                            print(f"  🔗 Найдено {len(items)} подподкатегорий (тестируем первые 2)")
                            for grand in items[:2]:
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
                                                block_info,
                                                is_structured_block=True
                                            )
                                        print(f"    ✅ Обработано {len(grand_result['blocks'])} блоков")
                                        
                                except Exception as e:
                                    print(f"    ❌ Ошибка обработки {grand['name']}: {e}")
                        else:
                            # Это товары custom_list (отдельные товары с изображениями)
                            add_to_category_collector(selected_category_name, sub_name, items, is_structured_block=False)
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