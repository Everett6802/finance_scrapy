# -*- coding: utf8 -*-

import os
import sys
import re
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_company_profile as CompanyProfile
g_logger = CMN.WSL.get_web_scrapy_logger()


@CMN.CLS.Singleton
class WebScrapyURLTimeRange(object):

    def __init__(self):
        self.company_profile_lookup = None
        self.company_listing_date_dict = None


    def initialize(self):
        # self.company_profile_lookup = WebScrapyCompanyProfileLookup.Instance()
        self.company_listing_date_dict = {}


    def get_time_range_start(self, date_source_id, company_code_number):
        # listing_date = self.company_listing_date_dict.get(date_source_id, None)
        # if listing_date is None:
        #     listing_date_str = self.company_profile_lookup.lookup_company_listing_date(date_source_id)
        #     listing_date = self.company_listing_date_dict[date_source_id] = CMN.CLS.FinanceDate(listing_date_str)
        return CMN.CLS.FinanceDate("2000-01-01")


    def get_time_range_end(self, date_source_id, company_code_number):
        # if self.last_url_data_date is None:
        #     self.__get_date_range_end(CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_MINUTE)
        return CMN.CLS.FinanceDate("2099-12-31")
