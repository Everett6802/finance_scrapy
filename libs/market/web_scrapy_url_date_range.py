# -*- coding: utf8 -*-

import os
import sys
import re
from datetime import datetime, timedelta
import libs.common as CMN
g_logger = CMN.WSL.get_web_scrapy_logger()


@CMN.CLS.Singleton
class WebScrapyURLDateRange(object):

    def __init__(self):
        # import pdb; pdb.set_trace()
        self.DEF_DATA_SOURCE_START_DATE_CFG = None
        self.DEF_DATA_SOURCE_START_DATE_CFG_LEN = 0
        self.last_url_data_date = None


    def initialize(self):
        self.DEF_DATA_SOURCE_START_DATE_CFG = [
            CMN.CLS.FinanceDate("2001-01-01"),
            CMN.CLS.FinanceDate("2004-04-07"),
            CMN.CLS.FinanceDate("2001-01-01"),
            CMN.CLS.FinanceDate(self.__get_year_offset_datetime_cfg(datetime.today(), -3)),
            CMN.CLS.FinanceDate(self.__get_year_offset_datetime_cfg(datetime.today(), -3)),
            CMN.CLS.FinanceDate(self.__get_year_offset_datetime_cfg(datetime.today(), -3)),
            CMN.CLS.FinanceDate("2002-01-01"),
            CMN.CLS.FinanceDate("2004-07-01"),
            CMN.CLS.FinanceDate("2012-05-02"),
            CMN.CLS.FinanceDate("2012-05-02"),
            CMN.CLS.FinanceDate("2015-04-30"),
            # transform_string2datetime("2010-01-04"),
            # transform_string2datetime("2004-12-17"),
            # transform_string2datetime("2004-12-17"),
            # transform_string2datetime("2004-12-17"),
        ]
        self.DEF_DATA_SOURCE_START_DATE_CFG_LEN = len(self.DEF_DATA_SOURCE_START_DATE_CFG)


    def __get_year_offset_datetime_cfg(self, datetime_cfg, year_offset):
        return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


    def get_date_range_start(self, date_source_id):
        if date_source_id < 0 or date_source_id >= self.DEF_DATA_SOURCE_START_DATE_CFG_LEN:
            raise ValueError("The data source ID[%d] is OUT OF RANGE[0, %d)" % (date_source_id, self.DEF_DATA_SOURCE_START_DATE_CFG_LEN))
        return self.DEF_DATA_SOURCE_START_DATE_CFG[date_source_id]


    def get_date_range_end(self, date_source_id):
        if self.last_url_data_date is None:
            self.last_url_data_date = CMN.FUNC.get_last_url_data_date(CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_MINUTE)
        return self.last_url_data_date
