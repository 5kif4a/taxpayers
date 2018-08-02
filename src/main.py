import requests as rq
import pandas as pd
import sqlalchemy as sa
import schedule
import io
import config
import time
from functools import reduce
import logging


logging.basicConfig(filename='console.log', format='%(asctime)s - %(message)s')
log = logging.getLogger(__name__)


class Utility:
    __slots__ = ['user', 'password', 'database', 'frq', 'xlsx_files', 'csv_files', 'df_final']

    def __init__(self, user, password, database):  # constructor
        self.user = user
        self.password = password
        self.database = database
        self.frq = config.frq  # частота обновления
        self.xlsx_files = {}  # excel файлы скачанные с сервера
        self.csv_files = []  # преобразованные csv файлы
        self.df_final = None  # конечный датафрейм, который пойдет в базу данных

    def download(self):  # скачать данные с сервера
        for url in config.urls:  # тут скачиваем по одному
            filename = url.split('/')[-1]
            r = rq.get(url)
            f = io.BytesIO(r.content)
            self.xlsx_files[filename] = f

    def xlsx_to_csv(self):  # 1) BytesIO в read_excel -> 2) df - > to_csv
        for filename, f in self.xlsx_files.items():
            excel = pd.read_excel(f, sheet_name=None)  # байты в excel, словарь датафреймов

            for sheet_name, dataframe in excel.items():
                if not dataframe.empty:  # если лист в книге excel не пуст
                    df = pd.read_csv(io.StringIO(dataframe.to_csv(encoding='utf8',  # excel -> csv -> StringIO -> DataFrame
                                                                  na_rep='NULL', index=False, header=False)))
                    df = df.drop(0)  # удаление ненужных строк, столбцы

                    self.csv_files.append(df)  # list
                    # self.csv_files['csv/{0}_{1}.csv'.format(filename.split('.')[0], sheet_name)] = df  # dict

    def merge(self):  # объединение таблиц
        # сперва делаем объединение, добавляем в конец датафрейма след. датафрейм
        # потом удаляем нумерацию которая перешла от excel
        # прописываем столбцы (просто нумерую)
        self.df_final = reduce(lambda left, right: left.append(right, ignore_index=True, sort=False), self.csv_files)
        self.df_final = self.df_final.drop(self.df_final.columns[0], axis=1)
        self.df_final.columns = [str(el) for el in range(len(self.df_final.columns))]

    def data_to_db(self):  # добавление данных в базу
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(self.user, self.password, 'localhost', 5432, self.database)

        con = sa.create_engine(url, client_encoding='utf8')

        metadata = sa.MetaData(con)
        t = sa.Table('all', metadata)

        self.df_final.to_sql('all', con, if_exists='replace', index=None)

    # def delete_tables(self):  # удалить таблицы
    #     url = 'postgresql://{}:{}@{}:{}/{}'
    #     url = url.format(self.user, self.password, 'localhost', 5432, self.database)
    #
    #     con = sa.create_engine(url, client_encoding='utf8')
    #     # список всех таблиц в бд
    #     t = con.execute(
    #         "SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()
    #     tables = [el[0] for el in t]
    #
    #     for table in tables:
    #         con.execute('DROP TABLE "{}"'.format(table))
    #     con.dispose()

    def main(self):  # основной метод
        try:
            print('Updating started')
            log.info('Updating started')
            t = time.time()
            self.download()
            self.xlsx_to_csv()
            self.merge()
            self.data_to_db()
            print('Updating finished. Update time: {:.2f} sec.'.format(time.time()-t))
            log.info('Updating finished')
        except Exception as e:
            print('Error occured')
            log.error(e)

    def run(self):  # по расписанию
        h = int(24/self.frq)
        schedule.every(h).hours.do(self.main)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == '__main__':
    demon = Utility('postgres', '123456', 'postgres')
    demon.run()
