# -*- encoding:utf-8 -*-
import logging
import re
from logging.handlers import TimedRotatingFileHandler
import sys

PROJECT_PATH = sys.path[0]

reload(sys)
sys.setdefaultencoding('utf8')

# create logger
logger_name = "db_schema_tracker"
logger = logging.getLogger(logger_name)
logger.setLevel(logging.DEBUG)
fmt = "%(asctime)s %(levelname)s %(filename)s %(lineno)4d %(process)d : %(message)s"
formatter = logging.Formatter(fmt)

# add handler to logger
fh = TimedRotatingFileHandler(filename=PROJECT_PATH + '/log/' + 'tracker', when="D", backupCount=30)
fh.suffix = "%Y-%m-%d.log"
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

# add handler to logger
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)
logger.addHandler(ch)