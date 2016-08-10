# -*- coding: utf8 -*-

import os
import re
import json
import requests
import csv
import time
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
# from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
# import libs.base.web_scrapy_base as WebScrapyBase
# import libs.base.web_scrapy_timeslice_generator as TimeSliceGenerator
import web_scrapy_company_profile as CompanyProfile
import web_scrapy_company_group_set as CompanyGroupSet
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyStockBase(BASE.BASE.WebScrapyBase):

    company_profile = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyStockBase, self).__init__(cur_file_path, **kwargs)
        self.company_group_set = None
        if kwargs.get("company_group_set", None) is None:
            self.company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_group_set()
        else:
            self.company_group_set = kwargs["company_group_set"]


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def assemble_csv_filepath(cls, data_source_index, company_code_number, company_group_index=-1):
        if company_group_index == -1:
            company_group_index = cls.__get_company_profile().lookup_company_group_number(company_code_number)
        csv_filepath = "%s/%s%02d/%s%s.csv" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, company_group_index, company_code_number, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[self.data_source_index]) 
        return csv_filepath


    def scrap_web_to_csv(self):
        timeslice_generator = self._get_time_slice_generator()
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
                timeslice_iterable = timeslice_generator.generate_time_slice(self.timeslice_generate_method, **self.time_slice_kwargs)
                for timeslice in timeslice_iterable:
                    url = self.assemble_web_url(timeslice, company_code_number)
                    g_logger.debug("Get the data by date from URL: %s" % url)
                    try:
        # Grab the data from website and assemble the data to the entry of CSV
                        csv_data_list = self.parse_web_data(self._get_web_data(url))
                        if csv_data_list is None:
                            raise RuntimeError(url)
                        csv_filepath = self.assemble_csv_filepath(self.source_type_index, company_code_number, company_group_number)
                        g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
                        WebScrapyBase._write_to_csv(csv_filepath, csv_data_list)
                    except Exception as e:
                        g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))


    def assemble_web_url(self, timeslice, company_code_number):
        raise NotImplementedError
