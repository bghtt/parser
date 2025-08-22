from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

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
            return parse_table_products()

    except Exception as e:
        print(f" Ошибка при парсинге товаров: {e}")
        return []


def parse_custom_list():
    products = []

    list_items = driver.find_elements(By.CSS_SELECTOR, "a.thumb")
    for item in list_items:
        try:
            try:
                product_link_tag = item.find_element(By.CSS_SELECTOR, "a.dark_link.js-notice-block__title")
                url = product_link_tag.get_attribute('href')
                name = product_link_tag.find_element(By.TAG_NAME, "span").text.strip()
            except:
                name = "Не указано"
                url = None
            products.append({
                "name": name,
                "url": url
            })
        except Exception as e:
            print(f"Пропущен товар {e}")
    print(f"Найдено товаров в custom_list: {len(products)}")
    return products

def parse_table_products():
    """
    Парсит товары из таблицы.
    Если есть ссылка 'Полный список' — переходит туда.
    Возвращает список словарей с name, url, article, props
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

    # Шаг 2: Парсим товары (в любом случае — с полной страницы или текущей)
    try:
        rows = driver.find_elements(By.CSS_SELECTOR, "tr.main_item_wrapper")
        print(f" → Найдено строк с товарами: {len(rows)}")
    except Exception as e:
        print(f" → Ошибка при поиске строк товаров: {e}")
        rows = []

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

            # Параметры
            props = {}
            props_cells = row.find_elements(By.CSS_SELECTOR, "td.table-view__item-wrapper-prop")
            for i, cell in enumerate(props_cells):
                props[f"param_{i+1}"] = cell.text.strip()

            # Добавляем товар (с правильным ключом!)
            products.append({
                "name": name,
                "url": url,           # ← исправлено: без пробелов
                "article": article,
                **props
            })

        except Exception as e:
            print(f" → Пропущен товар: {e}")
            continue

    print(f" → Найдено товаров: {len(products)}")
    return products

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
test_mode = input("Режим тестирования пагинации? (y/n): ").lower().strip() == 'y'

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-plugins")

driver = webdriver.Chrome(options=chrome_options)

if test_mode:
    # Режим теста: вводим любую ссылку
    test_url = input("Введите URL для теста (например, с пагинацией): ")
    print(f"\n→ Переход на: {test_url}")
    driver.get(test_url)
    time.sleep(2)

    # Прямо вызываем parse_table_products
    result = parse_table_products()
    print(f"\nРЕЗУЛЬТАТ ТЕСТА: найдено {len(result)} товаров")
    for prod in result[:5]:
        print(f" • {prod['name']} (артикул: {prod.get('article', '—')})")

else:
    # Основной режим: парсинг всей иерархии
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
                
                if items and isinstance(items[0], dict) and "name" in items[0] and "url" in items[0]:
                    if "article" not in items[0]:
                        sub["grandchildren"] = items

                        for grand in items:
                            try:
                                print(f"      → Парсим товары из под-подкатегории: {grand['name']} → {grand['url']}")
                                driver.get(grand["url"])
                                time.sleep(1.5)
                                grand_products = parse_table_products()
                                grand["products"] = grand_products
                            except Exception as e:
                                print(f"      Ошибка при парсинге товаров из {grand['name']}: {e}")
                                grand["products"] = []

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

            if grandchildren:
                print(f"  ├── {sub['name']} → {len(grandchildren)} под-подкатегорий")
                for grand in grandchildren:
                    grand_products = grand.get("products", [])
                    print(f"  │    ├── {grand['name']} ({len(grand_products)} товаров)")
                    for prod in grand_products[:3]:
                        print(f"  │    │      • {prod['name']}")
                    if len(grand_products) > 3:
                        print(f"  │    │      ... и ещё {len(grand_products) - 3}")

            elif products:
                print(f"  ├── {sub['name']} ({len(products)} товаров)")
                for prod in products[:3]:
                    print(f"  │      • {prod['name']}")
                if len(products) > 3:
                    print(f"  │      ... и ещё {len(products) - 3}")
            else:
                print(f"  ├── {sub['name']} (нет товаров)")
        print()

# === Завершение ===
driver.quit()