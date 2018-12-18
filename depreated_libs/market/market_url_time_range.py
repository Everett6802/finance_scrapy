# -*- coding: utf8 -*-

import os
import sys
import re
from datetime import datetime, timedelta
import libs.common as CMN
g_logger = CMN.LOG.get_logger()


@CMN.CLS.Singleton
class MarketURLTimeRange(object):

    def __init__(self):
        # import pdb; pdb.set_trace()
        self.METHOD_START_DATE_LIST = None
        # self.METHOD_START_DATE_LIST_LEN = 0
        self.last_url_data_date = None


    def initialize(self):
        self.METHOD_START_DATE_LIST = [
            CMN.CLS.FinanceDate("2001-01-01"),
            CMN.CLS.FinanceDate("2004-04-07"),
            CMN.CLS.FinanceDate("2001-01-01"),
            CMN.CLS.FinanceDate(CMN.FUNC.get_year_offset_datetime_cfg(datetime.today(), -3)),
            CMN.CLS.FinanceDate(CMN.FUNC.get_year_offset_datetime_cfg(datetime.today(), -3)),
            CMN.CLS.FinanceDate(CMN.FUNC.get_year_offset_datetime_cfg(datetime.today(), -3)),
            CMN.CLS.FinanceDate("2002-01-01"),
            CMN.CLS.FinanceDate("2004-07-01"),
            # CMN.CLS.FinanceDate("2012-05-02"),
            # CMN.CLS.FinanceDate("2012-05-02"),
            # CMN.CLS.FinanceDate("2015-04-30"),
            # transform_string2datetime("2010-01-04"),
            # transform_string2datetime("2004-12-17"),
            # transform_string2datetime("2004-12-17"),
            # transform_string2datetime("2004-12-17"),
        ]
        # self.METHOD_START_DATE_LIST_LEN = len(self.METHOD_START_DATE_LIST)


    # def __get_year_offset_datetime_cfg(self, datetime_cfg, year_offset):
    #     return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


    def get_time_range_start(self, scrapy_class_index):
        CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_index)
        return self.METHOD_START_DATE_LIST[scrapy_class_index]


    def get_time_range_end(self, scrapy_class_index):
        CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_index)
        if self.last_url_data_date is None:
            self.last_url_data_date = CMN.CLS.FinanceDate.get_last_finance_date()
        return self.last_url_data_date


    def get_time_range(self, scrapy_class_index):
        return (self.get_time_range_start(scrapy_class_index), self.get_time_range_end(scrapy_class_index))
