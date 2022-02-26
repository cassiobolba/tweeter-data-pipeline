import logging

def logger_error(msg,today):
    logger_error = logging.getLogger(__name__)
    logger_error.setLevel(logging.ERROR)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'c:/Users/cassi/Desktop/Tweeter_Case/logs/log_{today}.txt')
    file_handler.setFormatter(formatter)
    logger_error.addHandler(file_handler)
    return logger_error.error(msg,exc_info=True)

def logger_info(msg,today):
    logger_info = logging.getLogger(__name__)
    logger_info.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')
    file_handler = logging.FileHandler(f'c:/Users/cassi/Desktop/Tweeter_Case/logs/log_{today}.txt')
    file_handler.setFormatter(formatter)
    logger_info.addHandler(file_handler)
    return logger_info.info(msg)


