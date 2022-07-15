import requests
from bs4 import BeautifulSoup
from shops import shop
from multiprocessing.pool import ThreadPool
import multiprocessing
import time

def start():
    def carditem(url_page):
        """
        Парсинг товаров на странице и запись в таблицу
        :param url_page:
        :return:
        """
        link_item = url_page.find('a').get('href')

        try:
            item = requests.get(link_item, headers=headers)
            soup_product = BeautifulSoup(item.text, 'html.parser')
            # data_for_category = json.loads(soup_product.find_all('script', type='application/ld+json')[0].text)
            # category = data_for_category['itemListElement'][1]['item']['name']
            # subcategory = data_for_category['itemListElement'][2]['item']['name']
            # data = soup_product.find_all('script', type='application/ld+json')
            # data = json.loads(soup_product.find_all('script', type='application/ld+json')[1].text.strip())
            name = soup_product.find("h1").text
            article = name
            sku = ''
            image = soup_product.find("meta", property="og:image").attrs['content']
            url = soup_product.find("meta", property="url").attrs['content']
            try:
                description = soup_product.find("div", itemprop="description").text.strip()
            except:
                description = ''
            price = soup_product.find(class_="price").text.split('\n')[3].strip()
            currency = soup_product.find(class_="price").text.split('\n')[5].strip()
            availible = soup_product.find(class_="price").text.split('\n')[7].strip().replace('In stockF', 'В наличии')

            db.insert_to_catalog_db(name, category, article, sku, url, image, description, price, currency,
                                    availible)
        except:
            pass


    starttime = shop.start_time()
    category_list = []
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/50.0.2661.102 Safari/537.36'
    }

    url_main = 'https://gazkomplekt.com.ua'
    table_bd_category = 'gazkomplekt_category'
    table_bd_catalog = 'gazkomplekt_catalog'

    r = requests.get(url_main, headers=headers)
    r.encoding = 'utf-8'
    soup = BeautifulSoup(r.text, 'html.parser')

    db = shop.ConnectToDb(table_bd_category, table_bd_catalog)
    db.connect_to_db()


    # Получаем и записываем в таблицу название и ссылки пунктов меню
    for menu in soup.find_all(class_='col_4'):
        title = menu.find(class_='caption').find('a').text.strip()
        link = url_main + menu.find('a')['href'].replace(url_main, '')
        category_list.append((title, link))
        db.insert_to_category_db(title, link)

    # Проходимся по каждому пункту меню
    for z in range(len(category_list)):
        url_category = category_list[z][1]
        category = category_list[z][0]
        r = requests.get(url_category, headers=headers)
        soup2 = BeautifulSoup(r.text, 'html.parser')
        pagination = soup2.find(class_='pagination')
        try:
            if pagination:
                par = pagination.find_all('a')[-3].get_text()
            else:
                par = 1
        except IndexError:
            par = 1
        # print(par)
        # all_instances = parse.urlparse(par)
        # print(all_instances)

        # Проходимся по каждой странице
        for j in range(1, int(par) + 1):
            page = 'page=' + str(j)
            r = requests.get(url_category, params=page, headers=headers)
            soup = BeautifulSoup(r.text, 'html.parser')
            for url_page in soup.find_all(class_='name'):
                # threading.Thread(target=carditem, args=[url_page]).start()
                carditem(url_page)

    db.close_db()
    shop.end_time("Gazkomplekt", starttime)
    # time.sleep(4)
