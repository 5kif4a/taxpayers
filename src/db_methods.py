import os
import sqlalchemy as sa
import log
import csv
import pandas as pd


def create(user, password, db, host='localhost', port=5432):  # метод создает таблицы по sql запросу
    try:                                                      # потом идут запросы на список всех таблиц, их столбцов
        url = 'postgresql://{}:{}@{}:{}/{}'                   # типов данных столбцов
        url = url.format(user, password, host, port, db)      # потом магия pandas и sqlalchemy

        print('Connecting to postgres server')
        con = sa.create_engine(url, client_encoding='utf8')
        print('Connected')

        print('Creating tables')
        with open('create.sql', 'r', encoding='utf8') as f:
            scripts = f.read()

        commands = scripts.split(';')

        for c in commands:
            con.execute(c)

        print('Tables are created')

        print('Inserting data from CSV files into database tables')
        t = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()  # список всех таблиц в бд
        files = os.listdir('data/csv_edited/')
        tables = [el[0] for el in t]
        tf = dict({(k, v) for (k, v) in zip(tables, files) if k in v})  # связываем таблицу и файл по имени
        for table, file in tf.items():  # пихаем из CSV в таблицы бд
            with open('data/csv_edited/' + file, 'r', encoding='utf8') as f:
                cols_types = con.execute(  # получить столбцы и тип данных столбца каждой таблицы
                        "SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{}'".format(
                           table)).fetchall()
                cols = [el[0] for el in cols_types]
                dt_0 = dict(cols_types)
                dt_1 = dict()
                for col, t in dt_0.items():
                    if t == 'integer':
                        dt_1[col] = sa.types.Integer
                    if t == 'character':
                        dt_1[col] = sa.types.TEXT
                    if t == 'text':
                        dt_1[col] = sa.types.TEXT
                    if 'timestamp' in t:
                        dt_1[col] = sa.types.TIMESTAMP(timezone=False)
                    if t == 'real':
                        dt_1[col] = sa.types.REAL

                df = pd.read_csv('data/csv_edited/' + file, header=None, names=cols, dtype=dt_1)
                df.to_sql(table, con, index=None, index_label=cols, if_exists='replace', dtype=dt_1)

        print('Data inserted into database tables')
        log.info('Data inserted into database tables')
        con.dispose()
    except Exception as e:
        print('Error occured:' + str(e))
        #log.error(str(e))


def delete_tables(user, password, db, host='localhost', port=5432):  # удалить таблицы
    try:
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)

        print('Connecting to postgres server')
        con = sa.create_engine(url, client_encoding='utf8')
        print('Connected')

        t = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()  # список всех таблиц в бд
        tables = [el[0] for el in t]

        print('Deleting tables')
        for table in tables:
            con.execute('DROP TABLE "{}"'.format(table))
        print('Tables are deleted')
        log.info('Tables are deleted')
        con.dispose()
    except Exception as e:
        print('Error occured: ' + str(e))
        log.error(str(e))