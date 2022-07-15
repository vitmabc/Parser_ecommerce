import requests
from bs4 import BeautifulSoup
import shop


def piramida24(url_page):
    """
    Парсинг товаров на странице и запись в таблицу
    :param url_page:
    :return:
    """
    r = requests.get(url_page, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')

    for div in soup.find_all(class_='catalogCard-view'):
        link_item = url_main + div.find('a').get('href')

        try:
            item = requests.get(link_item, headers=headers)
            soup_product = BeautifulSoup(item.text, 'html.parser')
            name = soup_product.find("meta", property="og:title").attrs['content']
            article = soup_product.find("meta", itemprop="sku").attrs['content']
            try:
                description = soup_product.find("div", itemprop="description").text
            except:
                description = ''
            url = soup_product.find("meta", property="og:url").attrs['content']
            category = soup_product.find("meta", property="product:category").attrs['content']
            image = soup_product.find("meta", property="og:image").attrs['content']
            price = soup_product.find("meta", property="product:price:amount").attrs['content']
            currency = soup_product.find("meta", property="product:price:currency").attrs['content']
            availible = soup_product.find("meta", property="product:availability").attrs['content'] \
                .replace('instock', 'В наличии') \
                .replace('oos', 'Нет в наличии')
            sku = ''

            db.insert_to_catalog_db(name, category, article, sku, url, image, description, price, currency,
                                    availible)
        except:
            pass


time_to_start = shop.start_time()
category_list = []
headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/50.0.2661.102 Safari/537.36'}
url_main = 'https://piramida24.com.ua'
table_bd_category = 'piramida_category'
table_bd_catalog = 'piramida_catalog'

r = requests.get(url_main, headers=headers)
soup = BeautifulSoup(r.content, 'html.parser')

db = shop.ConnectToDb(table_bd_category, table_bd_catalog)
db.connect_to_db()
db.sequence_id_change()  # Получаем последнее ID в таблице и устанавливаем начало последовательности в Postrgres

# Получаем и записываем в таблицу название и ссылки пунктов меню
menu = soup.find_all(class_='productsMenu-tabs-list__tab')
for submenu in soup.find_all(class_='productsMenu-submenu-i'):
    menu.append(submenu)

for menu_name in menu:
    title = menu_name.find('a').text.strip()
    link = url_main + menu_name.find('a')['href'].replace(url_main, '')
    category_list.append((title, link))
    db.insert_to_category_db(title, link)

# Проходимся по каждому пункту меню
for z in range(1, len(category_list)):

    url_category = category_list[z][1]
    r = requests.get(url_category, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    # Проходимся по каждой странице
    if soup.find_all(class_='pager__item-text'):
        max_page = soup.find_all(class_='pager__item-text')
        if max_page[-1].text == 'Показати все':
            url_page = url_category + 'filter/page=all/'
            piramida24(url_page)
        else:
            for i in range(1, int(max_page[-2].text) + 1):
                url_page = url_category + 'filter/page=' + str(i) + '/'
                piramida24(url_page)
    else:
        url_page = url_category
        piramida24(url_page)

db.sequence_id_change()  # Получаем последнее ID в таблице и устанавливаем начало последовательности в Postrgres
db.close_db()
shop.end_time("Piramida24", time_to_start)
