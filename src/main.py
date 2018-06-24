import os
import time
import datetime
import file_methods
import db_methods
import log


def help():
    print('''Список комманд:
    create - создать таблицы в базе данных и внести информацию из CSV файлов в таблицы базы данных
    convert - конвертировать XLSX файлы в .csv
    prepare - подготовить CSV для импортирования в базу данных 
    delete files - удалить все XLSX и CSV файлы
    delete tables - удалить таблицы в базе данных
    download - скачать XLSX файлы с сервера
    update - обновление базы данных
    exit - закрыть программу''')


def main_method():  # не самый лучший метод однако)
    print('Updating started')
    log.info('Updating started')
    file_methods.delete_files('data/csv/')
    file_methods.delete_files('data/xlsx/')
    file_methods.delete_files('data/csv_edited/')
    file_methods.download()
    file_methods.xlsx_to_csv()
    file_methods.prepare_csv()
    db_methods.delete_tables('postgres', 'beat7boX', 'postgres')
    db_methods.create('postgres', 'beat7boX', 'postgres')
    print('Updating finished')
    log.info('Updating finished')


if __name__ == '__main__':
    if not os.path.exists('data/'):  # если нету папки data
        os.makedirs('data/')
    print('Напишите комманду help для вызова справки')
    command = ''
    while command != 'exit':
        command = input('>>> ')  # like python.exe
        if command == 'download':  # скачать .xlsx файлы
            file_methods.download()
        elif command == 'help':  # справка
            help()
        elif command == 'convert':
            file_methods.xlsx_to_csv()
        elif command == 'prepare':
            file_methods.prepare_csv()
        elif command == 'delete files':
            file_methods.delete_files('data/csv/')
            file_methods.delete_files('data/xlsx/')
            file_methods.delete_files('data/csv_edited/')
        elif command == 'delete tables':
            db_methods.delete_tables('postgres', 'beat7boX', 'postgres')  # палево с паролем
        elif command == 'create':
            db_methods.create('postgres', 'beat7boX', 'postgres')
        elif command == 'update':  # обновить вручную
            main_method()
        elif command == 'exit':
            break
        elif command == '':
            continue
        else:
            print('Invalid command. Use command \'help\' to get help')
