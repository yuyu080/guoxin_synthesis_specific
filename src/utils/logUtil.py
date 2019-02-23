import os
import logging
from logging.handlers import RotatingFileHandler

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = BASE_DIR + '/logs/'


def create_logger(log_dir=LOG_DIR, logger_name='default', file_name='default.log'):
    fh = RotatingFileHandler(log_dir+file_name,
                             encoding='utf-8',
                             maxBytes=1024 * 1024, backupCount=40) #按照日志大小拆分
    logger = logging.getLogger(logger_name) #获得一个logger对象，默认是root
    logger.setLevel(logging.INFO)  #设置最低等级debug
    fm = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(fm)  # 把文件流添加写入格式
    logger.addHandler(fh) #把文件流添加进来，流向写入到文件
    return logger
