import requests as rq
import pandas as pd
import csv
import os
import stat
import re
import log


def download():  # скачать данные в .xlsx
    if not os.path.exists('data/xlsx/'):
        os.makedirs('data/xlsx/')
    cfg = []

    try:
        with open('config/settings.config', 'r', encoding='utf8') as f:
            for line in f:
                cfg.append(line.strip())

        frq = cfg[0]  # частота обновления в минутах
        urls = cfg[1:]  # ссылки на .xlsx

        print('Download started')
        for url in urls:  # тут скачиваем по одному
            filename = url.split('/')[-1]
            print('Downloading file: ' + filename)
            r = rq.get(url)
            with open('data/xlsx/' + filename, 'wb') as xlsx:
                xlsx.write(r.content)
            print(filename + ' downloaded')
        print('Files are downloaded')
        log.info('Files are downloaded from server')
        return frq
    except IOError:
        print('Configuration file not found!')


def xlsx_to_csv():  # конвертация в csv для удобства добавления в таблицы бд
    if not os.path.exists('data/csv/'):
        os.makedirs('data/csv/')

    xlsx_files = os.listdir('data/xlsx/')
    if len(xlsx_files) > 0:
        print('Convert .xlsx to .csv started')
        for xlsx_file in xlsx_files:
            print('Converting ' + xlsx_file)

            if xlsx_file == 'list_TAX_ARREARS_150_KZ_ALL.xlsx':  # в этом файле два листа, повторяющийся код
                xl = pd.ExcelFile('data/xlsx/' + xlsx_file)

                for _, sh_n in enumerate(xl.sheet_names):
                    df = pd.read_excel('data/xlsx/' + xlsx_file, sheet_name=sh_n)
                    csv_file = xlsx_file.split('.')[0] + str(_) + '.csv'
                    df.to_csv('data/csv/' + csv_file, encoding='utf8',
                              na_rep='NULL', index=False, header=False)

                print(xlsx_file + ' converted')
                continue

            df = pd.read_excel('data/xlsx/' + xlsx_file, sheet_name='Лист1')
            csv_file = xlsx_file.split('.')[0] + '.csv'
            df.to_csv('data/csv/' + csv_file, encoding='utf8',
                      na_rep='NULL', index=False, header=False)
            print(xlsx_file + ' converted')
        print('Files are converted')
        log.info('XLSX files are converted to CSV format')
    else:
        print('Folder with XLSX files is empty')


def prepare_csv():  # слишком большой метод, криворукие заполняли .xlsx файлы, много вложенных блоков :(
    if not os.path.exists('data/csv_edited/'):
        os.makedirs('data/csv_edited/')
    os.chmod('data/csv_edited/', stat.S_IRWXU)  # даем доступ для чтения ко всем файлам, чтобы COPY FROM CSV сработал
    files = os.listdir('data/csv/')
    if len(files) > 0:
        print('Process of preparation .csv files')

        for file in files:
            print('Processing ' + file)
            with open('data/csv/' + file, 'r', encoding='utf8') as inp, open('data/csv_edited/' +
                                                                             file.split('.')[0] +
                                                                             '_edited.csv',
                                                                             'w',
                                                                             encoding='utf8',
                                                                             newline='') as out:
                writer = csv.writer(out)
                for row in csv.reader(inp):
                    try:
                        if int(row[0]) >= 1:
                            if row[-1] == 'NULL' and 'VIOLATION' not in file:  # если дата приказа
                                row[-1] = '1970-01-01 00:00:00'                              # не заполнена
                            try:                                                             # postgres ругался на timestamp
                                for i in range(len(row)):
                                    if '"' in row[i] or '\'' in row[i]:  # убрать все кавычки из .csv
                                        row[i] = re.sub('\'|\"', '', row[i])
                                    if 'товарищество' in row[i].lower():  # замена на аббревиатуры
                                        row[i] = row[i].replace('Товарищество с ограниченной ответственностью', 'ТОО')
                                        row[i] = row[i].replace('товарищество с ограниченной ответственностью', 'ТОО')
                                    elif 'акционерное' in row[i].lower():
                                        row[i] = row[i].replace('Акционерное общество', 'АО')
                                        row[i] = row[i].replace('акционерное общество', 'АО')
                                    elif 'общество' == row[i].split()[0].lower():
                                        row[i] = row[i].replace('Общество с ограниченно ответственностью', 'ООО')
                                        row[i] = row[i].replace('общество с ограниченно ответственностью', 'ООО')
                            except (TypeError, IndexError):
                                continue
                            writer.writerow(row)
                    except (ValueError, TypeError):
                        continue
        print('Files are prepared')
        log.info('CSV files are prepared for inserting into db tables')
    else:
        print('Folder with CSV files is empty')


def delete_files(path):  # удалить файлы
    files = os.listdir(path)
    if len(files) > 0:
        print('Deleting files')
        for file in files:
            print('Deleting ' + file)
            os.remove(path + file)
            print(file + ' deleted')
        print('Files in {0} are deleted'.format(path))
        log.info('Files in {0} are deleted'.format(path))
    else:
        print('{} folder is empty'.format(path))