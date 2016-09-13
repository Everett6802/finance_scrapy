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
import web_scrapy_company_group_set as CompanyGroupSet
import web_scrapy_company_profile as CompanyProfile
import web_scrapy_url_time_range as URLTimeRange
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyStockBase(BASE.BASE.WebScrapyBase):

    company_profile = None
    url_time_range = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyStockBase, self).__init__(cur_file_path, **kwargs)
        self.company_group_set = None
        if kwargs.get("company_group_set", None) is None:
            self.company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_group_set()
        else:
            self.company_group_set = kwargs["company_group_set"]
# Determine the time range
        if self.xcfg["time_start"] is None:
            self.xcfg["time_start"] = self.__get_url_time_range().get_date_range_start(self.source_type_index)
        if self.xcfg["time_end"] is None:
            self.xcfg["time_end"] = self.__get_url_time_range().get_date_range_end(self.source_type_index)
# Create the dict for the time slice generator
        if self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY or self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_WEEK:
            self.time_slice_kwargs["time_start"] = self.xcfg["time_start"]
            self.time_slice_kwargs["time_end"] = self.xcfg["time_end"]
        elif self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
            self.time_slice_kwargs["time_start"] = CMN.CLS.FinanceMonth(self.xcfg["time_start"].year, self.xcfg["time_start"].month)
            self.time_slice_kwargs["time_end"] = CMN.CLS.FinanceMonth(self.xcfg["time_end"].year, self.xcfg["time_end"].month)
        else:
            raise ValueError("Unsupported time unit: %d" % self.url_time_unit)
        self.time_slice_kwargs["company_code_number"] = None


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def __get_url_time_range(cls):
        # import pdb; pdb.set_trace()
        if cls.url_time_range is None:
            cls.url_time_range = URLTimeRange.WebScrapyURLTimeRange.Instance()
        return cls.url_time_range


    @classmethod
    def assemble_csv_filepath(cls, data_source_index, company_code_number, company_group_index=-1):
        if company_group_index == -1:
            company_group_index = cls.__get_company_profile().lookup_company_group_number(company_code_number)
        csv_filepath = "%s/%s%02d/%s%s.csv" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, company_group_index, company_code_number, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[self.data_source_index]) 
        return csv_filepath


    def scrap_web_to_csv(self):
        import pdb; pdb.set_trace()
        timeslice_generator = self._get_time_slice_generator()
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
                self.time_slice_kwargs["company_code_number"] = company_code_number
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
