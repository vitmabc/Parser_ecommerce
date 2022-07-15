import json
import requests
from bs4 import BeautifulSoup
from shops import shop
import time

def start():
    # time.sleep(3)
    starttime = shop.start_time()
    category_list = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/50.0.2661.102 Safari/537.36'}
    url_main = 'https://kotel.kr.ua'
    table_bd_category = 'kotel_kr_category'
    table_bd_catalog = 'kotel_kr_catalog'

    r = requests.get(url_main, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')

    res = soup.find_all(class_='box-category accordeon_category')[0]
    menu = res.find_all(class_='first jul-li ic-top')

    db = shop.ConnectToDb(table_bd_category, table_bd_catalog)
    db.connect_to_db()
    db.sequence_id_change()  # Получаем последнее ID в таблице и устанавливаем начало последовательности в Postrgres
    for name_menu in menu:
        title = name_menu.find('a').text.strip()
        link = url_main + name_menu.find('a')['href'].replace(url_main, '')
        category_list.append((title, link))
        db.insert_to_category_db(title, link)

    for z in range(len(category_list)):
        url_category = category_list[z][1]
        category = category_list[z][0]
        for j in range(1, 25):

            par = {'sort=p.viewed&order=DESC&page': j}
            try:
                r = requests.get(url_category, params=par, headers=headers)
            except ConnectionError:
                print(f"{url_main} Соединение было внезапно прервано")
            soup = BeautifulSoup(r.text, 'html.parser')
            for i in range(100):
                try:
                    product_url = soup.find_all('h4')[i].a.get('href')
                    item = requests.get(product_url, headers=headers)
                    soup_product = BeautifulSoup(item.text, 'html.parser')
                    data = json.loads(soup_product.find_all('script', type='application/ld+json')[1].text)
                    name = data['name']
                    sku = data['model']
                    article = data['sku']
                    image = data['image']
                    url = data['url']
                    description = data['description']
                    price = data['offers']['price']
                    currency = data['offers']['priceCurrency']
                    availible = data['offers']['availability'].replace('http://schema.org/InStock', 'В наличии') \
                        .replace('http://schema.org/OutOfStock', 'Нет в наличии') \
                        .replace('http://schema.org/Discontinued', 'Снят с производства') \
                        .replace('http://schema.org/PreOrder', 'Предзаказ')

                    db.insert_to_catalog_db(name, category, article, sku, url, image, description, price, currency,
                                            availible)

                except:
                    pass

    db.sequence_id_change()  # Получаем последнее ID в таблице и устанавливаем начало последовательности в Postrgres
    db.close_db()
    shop.end_time("kotel_kr", starttime)
