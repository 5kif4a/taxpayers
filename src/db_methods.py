import os
import sqlalchemy as sa
import log
import csv
import pandas as pd


def create(user, password, db, host='localhost', port=5432):
    try:

        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)

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
                    # data = csv.reader(f)
                    cols = con.execute(  # получить столбцы каждой таблицы
                        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{}'".format(
                           table)).fetchall()
                    cols = [el[0] for el in cols]
                    # for row in data:  # ненавижу форматирование, распаковка через .format не работает
                    print(table)
                    q0 = 'COPY "' + table + '" (' + ','.join('"' + el + '"' for el in cols) + ')'
                    q1 = "FROM 'C:/Users/5kif4a_/PycharmProjects/taxpayers/src/data/csv_edited/" + file + "' DELIMITER ',' CSV ENCODING 'UTF8'"
                    query = q0 + q1
                    con.execute(sa.text(query))
                    # здесь ругался на 'dict' object does not support indexing
                    # не мог понять почему
                    # читая с csv по строке, записывает в таблицы очень долго, работает с косяками
                    # скрипт COPY FROM CSV работал, но по факту вставок не было(снова вернулся к этому методу)
                    # dataFrame.to_sql работал, но тупил с столбцами таблиц в бд
        print('Data inserted into database tables')
        log.info('Data inserted into database tables')
        con.dispose()
    except Exception as e:
        print('Error occured:' + str(e))
        log.error(str(e))


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