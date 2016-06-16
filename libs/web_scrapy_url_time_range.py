# -*- coding: utf8 -*-

import os
import sys
import re
# import requests
# import csv
# import shutil
# from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_company_profile_lookup as CompanyProfileLookup
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


# Find the oldest and latest data
class WebScrapyURLTimeRangeBase(object):

    def __init__(self):
        self.datetime_range_end = None


    def __get_time_range_end(self, today_data_exist_hour, today_data_exst_minute):
        datetime_now = datetime.today()
        datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
        datetime_yesterday = datetime_today + timedelta(days = -1)
        datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, today_data_exist_hour, today_data_exst_minute)
        self.datetime_range_end = datetime_today if datetime_now >= datetime_threshold else datetime_yesterday


    def get_time_range_start(self, date_source_id):
        raise NotImplementedError


    def get_time_range_end(self, date_source_id):
        raise NotImplementedError

####################################################################################################

@CMN_CLS.Singleton
class WebScrapyMarketURLTimeRange(WebScrapyURLTimeRangeBase):

    def __init__(self):
        super(WebScrapyMarketURLTimeRange, self).__init__()
        self.DEF_DATA_SOURCE_START_DATE_CFG = None
        self.DEF_DATA_SOURCE_START_DATE_CFG_LEN = 0


    def initialize(self):
        self.DEF_DATA_SOURCE_START_DATE_CFG = [
            transform_string2datetime("2001-01-01"),
            transform_string2datetime("2004-04-07"),
            transform_string2datetime("2001-01-01"),
            get_year_offset_datetime_cfg(datetime.today(), -3),
            get_year_offset_datetime_cfg(datetime.today(), -3),
            get_year_offset_datetime_cfg(datetime.today(), -3),
            transform_string2datetime("2002-01-01"),
            transform_string2datetime("2004-07-01"),
            transform_string2datetime("2012-05-02"),
            transform_string2datetime("2012-05-02"),
            transform_string2datetime("2015-04-30"),
            # transform_string2datetime("2010-01-04"),
            # transform_string2datetime("2004-12-17"),
            # transform_string2datetime("2004-12-17"),
            # transform_string2datetime("2004-12-17"),
        ]
        self.DEF_DATA_SOURCE_START_DATE_CFG_LEN = len(self.DEF_DATA_SOURCE_START_DATE_CFG)


    def get_time_range_start(self, date_source_id):
        if date_source_id < 0 or date_source_id >= self.DEF_DATA_SOURCE_START_DATE_CFG_LEN:
            raise ValueError("The data source ID[%d] is OUT OF RANGE[0, %d)" % (date_source_id, self.DEF_DATA_SOURCE_START_DATE_CFG_LEN))
        return self.DEF_DATA_SOURCE_START_DATE_CFG[date_source_id]


    def get_time_range_end(self, date_source_id):
        if self.datetime_range_end is None:
            self.__get_time_range_end(CMN.DEF_TODAY_MARKET_DATA_EXIST_HOUR, CMN.DEF_TODAY_MARKET_DATA_EXIST_MINUTE)
        return self.datetime_range_end


####################################################################################################

@CMN_CLS.Singleton
class WebScrapyStockURLTimeRange(WebScrapyURLTimeRangeBase):

    def __init__(self):
        super(WebScrapyStockURLTimeRange, self).__init__()
        self.company_profile_lookup = None


    def initialize(self):
        self.company_profile_lookup = WebScrapyCompanyProfileLookup.Instance()


    def get_time_range_start(self, date_source_id):
        return self.company_profile_lookup.lookup_company_listing_date(date_source_id)


    def get_time_range_end(self, date_source_id):
        if self.datetime_range_end is None:
            self.__get_time_range_end(CMN.DEF_TODAY_STOCK_DATA_EXIST_HOUR, CMN.DEF_TODAY_STOCK_DATA_EXIST_MINUTE)
        return self.datetime_range_end
