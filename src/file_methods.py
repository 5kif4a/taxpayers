import requests as rq


def download():  # скачать данные в .xlsx

    cfg = []

    with open("settings.config", 'r', encoding='utf8') as f:
        for line in f:
            cfg.append(line.strip())

    frq = cfg[0]  # частота обновления в минутах
    urls = cfg[1:]  # ссылки на .xlsx

    print('Download started')
    for url in urls:  # тут скачиваем по одному
        filename = url.split('/')[-1]
        print('Downloading file: ' + filename)
        r = rq.get(url)
        with open(filename, 'wb') as xlsx:
            xlsx.write(r.content)
        print(filename + ' downloaded')
    print('Files are downloaded')
