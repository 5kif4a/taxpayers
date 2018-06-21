import logging


def info(message):
    logging.basicConfig(filename='config/console.log', level=logging.INFO,
                        format='Time - %(asctime)s: %(levelname)s - %(message)s')
    logging.info(msg=message)


def error(message):
    logging.basicConfig(filename='config/console.log', level=logging.ERROR,
                        format='Time - %(asctime)s: %(levelname)s - %(message)s')
    logging.error(msg=message)
