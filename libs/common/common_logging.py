# -*- coding: utf8 -*-

import os
import logging
import logrotate as LR
import common_definition as CMN_DEF
import common_class as CMN_CLS



LOG_FILE_FOLDER = "log"
LOG_FILE_NAME = "web_scrapy.log"
LOG_FILE_PATH = "%s/%s" % (LOG_FILE_FOLDER, LOG_FILE_NAME)
# LOG_WORKING_FOLDER = os.path.dirname(os.path.realpath(__file__)).rsplit('/', 2)[0]
LOG_WORKING_FOLDER = os.path.dirname(os.path.realpath(__file__)).split(CMN_DEF.PROJECT_NAME_PREFIX)[0] + "%s" % CMN_DEF.FINANCE_SCRAPY_PROJECT_NAME

logger_cfg = {
    "log_working_folder": LOG_WORKING_FOLDER,
    "log_file_folder": LOG_FILE_FOLDER,
    "log_file_name": LOG_FILE_NAME,
}

@CMN_CLS.Singleton
class CmnLogger(object):

    def __init__(self):
        self.logger = None


    def initialize(self, **cfg): 
        self.logger = LR.LogRotate(**cfg)


    @property
    def LoggerInstance(self):
        if self.logger is None:
            raise ValueError("Logger instance is NOT initialized")
        return self.logger


# def reset_web_scrapy_logger_content(filename=LOG_FILE_PATH):
#     if os.path.exists(filename):
#         with open(filename, "w") as fp:
#             fp.seek(0, 0)

def get_raw_logger(log_file_name=LOG_FILE_PATH, log_file_level=logging.DEBUG):
# Create the folder for log file if it does NOT exist
    logging.basicConfig(filename=log_file_name, level=log_file_level, format='%(asctime)-15s %(filename)s:%(lineno)d [%(levelname)s]: %(message)s')
# Log to console
    # console = logging.StreamHandler()
    # console.setLevel(logging.WARN)
    # console.setFormatter(logging.Formatter('%(asctime)-15s %(filename)s:%(lineno)d -- %(message)s'))
    # logging.getLogger().addHandler(console)
# Log to syslog
    from logging.handlers import SysLogHandler
    syslog = SysLogHandler(address='/dev/log')
    # syslog.setLevel(logging.INFO)
    syslog.setFormatter(logging.Formatter('%(filename)s:%(lineno)d [%(levelname)s]: %(message)s'))
    logging.getLogger().addHandler(syslog)
    return logging.getLogger()


def get_logger(need_rotate=True):
    if not os.path.exists(LOG_FILE_FOLDER):
        os.makedirs(LOG_FILE_FOLDER)
    return CmnLogger.Instance(logger_cfg).LoggerInstance if need_rotate else get_raw_logger()
