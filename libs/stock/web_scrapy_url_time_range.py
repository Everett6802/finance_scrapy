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


    def get_time_range_start(self, source_type_index, company_code_number):
        CMN.FUNC.check_source_type_index_in_range(source_type_index)
        # listing_date = self.company_listing_date_dict.get(source_type_index, None)
        # if listing_date is None:
        #     listing_date_str = self.company_profile_lookup.lookup_company_listing_date(source_type_index)
        #     listing_date = self.company_listing_date_dict[source_type_index] = CMN.CLS.FinanceDate(listing_date_str)
        return CMN.CLS.FinanceDate("2000-01-01")


    def get_time_range_end(self, source_type_index, company_code_number):
        CMN.FUNC.check_source_type_index_in_range(source_type_index)
        # if self.last_url_data_date is None:
        #     self.__get_date_range_end(CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_MINUTE)
        return CMN.CLS.FinanceDate("2099-12-31")
