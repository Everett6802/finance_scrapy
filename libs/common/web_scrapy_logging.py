# -*- coding: utf8 -*-

import os
# import logging
import logrotate as LR
import common_class as CMN_CLS
# import common_definition as CMN_DEF


LOG_WORKING_FOLDER = os.path.dirname(os.path.realpath(__file__)).rsplit('/', 2)[0]
web_scrapy_logger_cfg = {
    "log_working_folder": LOG_WORKING_FOLDER,
}

@CMN_CLS.Singleton
class WebScrapyLogger(object):

    def __init__(self):
        self.logger = None


    def initialize(self, **cfg): 
        self.logger = LR.LogRotate(**cfg)


    @property
    def LoggerInstance(self):
        if self.logger is None:
            raise ValueError("Logger instance is NOT initialized")
        return self.logger


def get_web_scrapy_logger():
    return WebScrapyLogger.Instance(web_scrapy_logger_cfg).LoggerInstance
