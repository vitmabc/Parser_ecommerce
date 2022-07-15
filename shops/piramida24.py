import requests
from bs4 import BeautifulSoup
from shops import shop
import time
from datetime import datetime
from multiprocessing.pool import ThreadPool
from peewee import *

domain = 'https://piramida24.com.ua'
table_bd_category = 'piramida_category'
table_bd_catalog = 'piramida_catalog'

dbp = SqliteDatabase('database.db')


class PiramidaCategory(Model):
    catalog_name = TextField()
    catalog_link = TextField(primary_key=True)

    class Meta:
        db_table = 'piramida_category'
        database = dbp

class PiramidaCatalog(Model):
    id = IntegerField(primary_key=True)
    name = TextField()
    category = TextField()
    sku = TextField()
    sku_temp = TextField()
    article = TextField()
    description = TextField()
    price = IntegerField()
    currency = TextField()
    availible = TextField()
    url = TextField()
    image = TextField()


    class Meta:
        indexes = ((('sku', 'article'), True),)
        db_table = 'piramida_catalog'
        database = dbp

def get_html(url):
    r = requests.get(url)
    return r.text


def get_catalog_link(html):
    def for_(links):
        for link in links:
            catalog_link = domain + link.find('a').get('href')
            catalog_name = link.find('a').text.strip()
            data = {'catalog_link': catalog_link,
                    'catalog_name': catalog_name}
            write_catalog_to_db(data)
            get_page_data(get_html(catalog_link))

    soup = BeautifulSoup(html, 'html.parser')
    links_menu = soup.find('div', class_='productsMenu-tabs-switch').find_all('li',
                                                                              class_='productsMenu-tabs-list__tab')
    for_(links_menu)
    links_submenu = soup.find('div', class_='productsMenu-tabs-content').find_all('li', class_='productsMenu-submenu-i')
    for_(links_submenu)


def get_page_data(html):
    while True:
        soup = BeautifulSoup(html, 'html.parser')
        try:
            cards = soup.find('ul', class_='catalogGrid').find_all('li', class_='catalog-grid__item')
            pool = ThreadPool(10)
            pool.map(pool_get_card_data, cards)
            pool.close()
            pool.join()

        except:
            pass

        try:
            pagination = soup.find('link', rel='next').get('href')
            html = get_html(pagination)
        except:
            break


def pool_get_card_data(cards):
    card_link = 'https://piramida24.com.ua' + cards.find('div', class_='catalogCard-view').find('a').get('href')
    card_html = get_html(card_link)
    soup_product = BeautifulSoup(card_html, 'html.parser')
    try:
        name = soup_product.find("meta", property="og:title").attrs['content']
    except:
        name = ''
    try:
        article = soup_product.find("meta", itemprop="sku").attrs['content']
    except:
        article = ''
    try:
        description = soup_product.find("div", itemprop="description").text
    except:
        description = ''
    url = soup_product.find("meta", property="og:url").attrs['content']
    category = soup_product.find("meta", property="product:category").attrs['content']
    try:
        image = soup_product.find("meta", property="og:image").attrs['content']
    except:
        image = ''
    price = soup_product.find("meta", property="product:price:amount").attrs['content']
    currency = soup_product.find("meta", property="product:price:currency").attrs['content']
    availible = soup_product.find("meta", property="product:availability").attrs['content'] \
        .replace('instock', 'В наличии') \
        .replace('oos', 'Нет в наличии')
    sku = ''
    # data = {'name': name,
    #         'article': article,
    #         'description': description,
    #         'url': url,
    #         'category': category,
    #         'image': image,
    #         'price': price,
    #         'currency': currency,
    #         'availible': availible,
    #         'sku': sku}
    data = {'name': name,
            'category': category,
            'sku': sku,
            'sku_temp': '',
            'article': article,
            'description': description,
            'price': price,
            'currency': currency,
            'availible': availible,
            'url': url,
            'image': image
            }
    write_data_to_db(data)


def write_catalog_to_db(data):
    # dd = list(data['catalog_link'],['catalog_name'])
    # print(data)
    with dbp.atomic():
        PiramidaCategory.create(**data)


def write_data_to_db(data):
    with dbp.atomic():
        PiramidaCatalog.create(**data)

def db():
    dbp.connect()
    dbp.create_tables([PiramidaCategory, PiramidaCatalog])
    PiramidaCategory.truncate_table()
    PiramidaCatalog.truncate_table()



def main():

    time_to_start = shop.start_time()
    # time.sleep(2)
    db()
    get_catalog_link(get_html(domain))
    dbp.close()
    shop.end_time("Piramida24", time_to_start)


if __name__ == '__main__':
    main()
