# _*_ coding:utf-8 _*_
import os
import sys
import logging
from logging.handlers import RotatingFileHandler


class LoggerManager(object):
    loggers = dict()
    fmt = '[%(asctime)s|%(name)s|%(filename)s|LN%(lineno)d] %(levelname)s %(message)s'
    path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    log_dir = os.path.join(path, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    max_bytes = 20 * 1024 * 1024
    backup_count = 10

    @ classmethod
    def get_logger(cls, name="root", level="INFO", max_bytes=max_bytes, backup_count=backup_count):
        if name in cls.loggers:
            return cls.loggers[name]
        logger = logging.getLogger(None if name == "root" else name)
        log_file = "{}/{}.log".format(cls.log_dir, name.split(".")[-1])
        formater = logging.Formatter(cls.fmt, datefmt='%Y-%m-%d %H:%M:%S')
        logger.setLevel(getattr(logging, level) if hasattr(logging, level) else logging.INFO)
        file_handler = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count, encoding='utf-8')
        file_handler.setFormatter(formater)
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formater)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        cls.loggers[name] = logger
        return logger


logger_manager = LoggerManager()
