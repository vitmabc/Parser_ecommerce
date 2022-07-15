# !/usr/bin/python
import sqlite3
from configparser import ConfigParser
from datetime import datetime
import threading


def start_time():
    starttime = datetime.now()
    return starttime


def end_time(url_main, starttime):
    # print(f'{url_main} End:' + str(datetime.now()))
    print(f'  *** {url_main} *** парсинг завершен. Время работы: ' + (str(datetime.now() - starttime)))






def config(filename='shops/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db


class ConnectToDb():
    def __init__(self, table_bd_category, table_bd_catalog):
        self.cursor = None
        self.connection = None
        self.table_bd_category = table_bd_category
        self.table_bd_catalog = table_bd_catalog
        self.count = 0

    def connect_to_db(self):
        self.connection = sqlite3.connect('database.db', check_same_thread=False)

    # def connect_to_db(self):
    #     """ Connect to the PostgreSQL database server """
    #     # self.connection = None
    #     try:
    #         # read connection parameters
    #         params = config()
    #
    #         # connect to the PostgreSQL server
    #         # print('Connecting to the PostgreSQL database...')
    #         self.connection = psycopg2.connect(**params)
    #         self.connection.autocommit = True
    #
    #     except (Exception, psycopg2.DatabaseError) as error:
    #         print(error)


    def insert_to_category_db(self, title, link):
        try:
            insert_query = f'CREATE TABLE IF NOT EXISTS {self.table_bd_category}("category" TEXT NOT NULL,"href" TEXT NOT NULL, PRIMARY KEY ("href"))'
            self.cursor = self.connection.cursor()
            self.cursor.execute(insert_query)
            insert_query = f'INSERT INTO {self.table_bd_category} (category, href) VALUES (?,?)'
            record_to_insert = (title, link)
            self.cursor.execute(insert_query,record_to_insert)
            self.connection.commit()
            self.cursor.close()

        except:
            pass

    def insert_to_catalog_db(self, name, category, article, sku, url, image, description, price, currency, availible):

        try:
            self.cursor = self.connection.cursor()

            insert_query = f'CREATE TABLE IF NOT EXISTS {self.table_bd_catalog} \
                (id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL ,\
                name TEXT NULL ,\
                category TEXT NULL,\
                sku TEXT NULL,\
                sku_temp TEXT NULL,\
                article TEXT NULL,\
                description TEXT NULL,\
                price INTEGER NULL,\
                currency TEXT NULL,\
                availability TEXT NULL,\
                url TEXT NULL,\
                image TEXT NULL,\
                data TEXT NULL, UNIQUE(sku, article) ON CONFLICT REPLACE)'
            self.cursor.execute(insert_query)
            insert_query = f'INSERT INTO {self.table_bd_catalog} \
                           (name,category,article,sku,url,image,description,\
                           price,currency,availability) VALUES (?,?,?,?,?,?,?,?,?,?) '

            record_to_insert = (name, category, article, sku, url, image, description, price, currency, availible)

            self.cursor.execute(insert_query,record_to_insert)
            self.connection.commit()
            self.cursor.close()

        except():
            print(f'ЗАПИСЬ {name} НЕ ВНЕСЕНА В БАЗУ ДАННЫХ {self.table_bd_catalog}')

    def sequence_id_change(self):
        # self.cursor = self.connection.cursor()
        # self.cursor.execute(
        #     f"SELECT id FROM {self.table_bd_catalog} ORDER BY id DESC LIMIT 1")
        # result = self.cursor.fetchone()
        # try:
        #     number_of_rows = result[0]+1
        # except TypeError:
        #     number_of_rows = 1
        # insert_query = f"ALTER SEQUENCE public.{self.table_bd_catalog}_id_seq RESTART WITH {number_of_rows};"
        # with self.connection.cursor() as cursor:
        #     cursor.execute(insert_query)
        pass

    def close_db(self):
        self.connection.close()

#
# q=ConnectToDb('catalog','category')
# q.connect_to_db()
# q.insert_to_category_db('eeee','eee')
#
# print('r')
