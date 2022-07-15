import json
import multiprocessing
from multiprocessing.pool import ThreadPool
import requests
from bs4 import BeautifulSoup
from shops import shop
import time


def start():
    def carditem(url_page_item):
        """
        Парсинг товаров на странице и запись в таблицу
        :param url_page_item:
        :return:
        """
        database = shop.ConnectToDb(table_bd_category, table_bd_catalog)
        database.connect_to_db()
        link_item = url_page_item.find('a').get('href')
        try:
            item = requests.get(link_item, headers=headers)
            soup_product = BeautifulSoup(item.text, 'html.parser')
            data_for_category = json.loads(soup_product.find_all('script', type='application/ld+json')[0].text)
            # category = data_for_category['itemListElement'][1]['item']['name']
            subcategory = data_for_category['itemListElement'][2]['item']['name']

            data = json.loads(soup_product.find_all('script', type='application/ld+json')[1].text)
            name = data['name']
            article = data['model']
            sku = data['sku']
            image = data['image']
            url = data['url']
            description = data['description']
            price = data['offers']['price']
            currency = data['offers']['priceCurrency']
            availible = data['offers']['availability'].replace('http://schema.org/InStock', 'В наличии') \
                .replace('http://schema.org/OutOfStock', 'Нет в наличии') \
                .replace('http://schema.org/Discontinued', 'Снят с производства') \
                .replace('http://schema.org/PreOrder', 'Предзаказ')

            database.insert_to_catalog_db(name, subcategory, article, sku, url, image, description, price, currency,
                                          availible)
        except:
            pass

        finally:
            database.close_db()

    starttime = shop.start_time()
    category_list = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/50.0.2661.102 Safari/537.36'}
    url_main = 'https://teplozapchast.com.ua/'
    table_bd_category = 'teplozapchast_category'
    table_bd_catalog = 'teplozapchast_catalog'

    r = requests.get(url_main, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')

    db = shop.ConnectToDb(table_bd_category, table_bd_catalog)
    db.connect_to_db()
    # Получаем последнее ID в таблице и устанавливаем начало последовательности в Postrgres

    # Получаем и записываем в таблицу название и ссылки пунктов меню
    for menu in soup.find_all(class_='oct-menu-child-ul oct-menu-third-child-ul'):
        for submenu in menu.find_all(class_='oct-menu-li'):
            link_category = submenu.find('a').get('href')
            title_category = submenu.find(class_='oct-menu-item-name').text.strip()
            db.insert_to_category_db(title_category, link_category)
            category_list.append((title_category, link_category))
        db.close_db()
    # Проходимся по каждому пункту меню
    for z in range(0, len(category_list)):
        url_category = category_list[z][1]

        r = requests.get(url_category, headers=headers)
        soup2 = BeautifulSoup(r.text, 'html.parser')
        pagination = soup2.find(class_='pagination')
        if pagination:
            par = pagination.find_all('a')[-3].get_text()
        else:
            par = 1
        # print(par)
        # all_instances = parse.urlparse(par)
        # print(all_instances)
        # Проходимся по каждой странице
        for j in range(1, int(par) + 1):
            page = 'page=' + str(j)
            r = requests.get(url_category, params=page, headers=headers)
            soup = BeautifulSoup(r.text, 'html.parser')
            h = multiprocessing.cpu_count()
            pool = ThreadPool(10)
            cards = soup.find_all(class_='fm-module-title')
            results = pool.map_async(carditem, cards).get()

            # for url_page in soup.find_all(class_='fm-module-title'):
            #     th = threading.Thread (target=carditem, args=(url_page,))
            #     th.start()

    shop.end_time("Teplozapchast", starttime)
    # time.sleep(5)


if __name__ == '__main__':
    start()
