import getpass
import pandas as pd
import sqlalchemy as sa


def connect(user, password, db, host='localhost', port=5432):  # присоединиться к серверу
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)

    con = sa.create_engine(url, client_encoding='utf8')

    meta = sa.MetaData(bind=con, reflect=True)

    return con, meta


def insert_into_db():  # из .xlsx в базу данных
    #pseudo_company_xl = pd.ExcelFile()
    df = pd.read_excel('list_PSEUDO_COMPANY_KZ_ALL.xlsx', sheet_name='Лист1')
    df.to_csv('list_PSEUDO_COMPANY_KZ_ALL.csv', encoding='utf8')
    #print(df)


if __name__ == '__main__':
    user = input('User (default: postgres): ')  # вход на сервер
    password = getpass.getpass('Password: ')

    con, meta = connect(user, password, 'taxpayers')
    print(con)
    print(meta)
    input()
