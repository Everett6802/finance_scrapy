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
    # url_time_range = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyStockBase, self).__init__(cur_file_path, **kwargs)
        self.company_group_set = None
        if kwargs.get("company_group_set", None) is None:
            self.company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_group_set()
        else:
            self.company_group_set = kwargs["company_group_set"]
        self.time_slice_kwargs["company_code_number"] = None


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def _get_url_time_range(cls):
        # import pdb; pdb.set_trace()
        if cls.url_time_range is None:
            cls.url_time_range = URLTimeRange.WebScrapyURLTimeRange.Instance()
        return cls.url_time_range


    @classmethod
    def assemble_csv_company_folderpath(cls, company_code_number, company_group_number=-1):
        if company_group_number == -1:
            company_group_number = cls.__get_company_profile().lookup_company_group_number(company_code_number)
        csv_company_folderpath = "%s/%s%02d/%s" % (CMN.DEF.DEF_CSV_ROOT_FOLDERPATH, CMN.DEF.DEF_CSV_STOCK_FOLDERNAME, company_group_number, company_code_number) 
        return csv_company_folderpath


    def assemble_csv_filepath(self, source_type_index, company_code_number, company_group_number=-1):
        if company_group_number == -1:
            company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
        csv_filepath = "%s/%s%02d/%s/%s.csv" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_STOCK_FOLDERNAME, company_group_number, company_code_number, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[source_type_index]) 
        return csv_filepath


    def _adjust_time_duration_from_lookup_table(self, company_code_number):
# Find the actual time range for each source
        (self._adjust_time_duration_start_and_end_time_func_ptr(self.xcfg["time_duration_type"]))(self.source_type_index, company_code_number)
        # if self.xcfg["time_duration_start"] is None:
        #     self.xcfg["time_duration_start"] = self.__get_url_time_range().get_date_range_start(self.source_type_index)
        # if self.xcfg["time_duration_end"] is None:
        #     self.xcfg["time_duration_end"] = self.__get_url_time_range().get_date_range_end(self.source_type_index)
# Create the dict for the time slice generator
        if self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY or self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_WEEK:
            self.time_slice_kwargs["time_duration_start"] = self.xcfg["time_duration_start"]
            self.time_slice_kwargs["time_duration_end"] = self.xcfg["time_duration_end"]
        elif self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
            self.time_slice_kwargs["time_duration_start"] = CMN.CLS.FinanceMonth(self.xcfg["time_duration_start"].year, self.xcfg["time_duration_start"].month)
            self.time_slice_kwargs["time_duration_end"] = CMN.CLS.FinanceMonth(self.xcfg["time_duration_end"].year, self.xcfg["time_duration_end"].month)
        else:
            raise ValueError("Unsupported time unit: %d" % self.url_time_unit)
        # g_logger.debug("The time range after adjustment: %s-%s" % (self.time_slice_kwargs["time_duration_start"], self.time_slice_kwargs["time_duration_end"]))


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
# Create a folder for a specific company
                csv_group_folderpath = self.assemble_csv_company_folderpath(company_code_number, company_group_number)
                CMN.FUNC.create_folder_if_not_exist(csv_group_folderpath)
                # import pdb; pdb.set_trace()
                self.time_slice_kwargs["company_code_number"] = company_code_number
# Find the file path for writing data into csv
                csv_filepath = self.assemble_csv_filepath(self.source_type_index, company_code_number, company_group_number)
# Determine the actual time range
                self._adjust_time_duration_from_lookup_table(company_code_number)
                scrapy_msg = "[%s:%s] %s %s:%s => %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], company_code_number, CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], self.xcfg["time_duration_start"], self.xcfg["time_duration_end"], csv_filepath)
                g_logger.debug(scrapy_msg)
# Check if only dry-run
                if self.xcfg["dry_run_only"]:
                    print scrapy_msg
                    continue
# Create the time slice iterator due to correct time range
                # import pdb; pdb.set_trace()
                timeslice_iterable = self._get_time_slice_generator().generate_time_slice(self.timeslice_generate_method, **self.time_slice_kwargs)
                csv_data_list_each_year = []
                cur_year = None
# Generate the time slice list                
                for timeslice in timeslice_iterable:
# Write the data into csv year by year
                    if timeslice.year != cur_year:
                        if len(csv_data_list_each_year) > 0:
                            # import pdb; pdb.set_trace()
                            self._write_to_csv(csv_filepath, csv_data_list_each_year, self.source_url_parsing_cfg["url_multi_data_one_page"])
                            csv_data_list_each_year = []
                        cur_year = timeslice.year
                    url = self.assemble_web_url(timeslice, company_code_number)
                    g_logger.debug("Get the data by date from URL: %s" % url)
                    try:
# Grab the data from website and assemble the data to the entry of CSV
                        csv_data_list = self._parse_web_data(self._get_web_data(url))
                        if csv_data_list is None:
                            raise RuntimeError(url)
                        csv_data_list_each_year.append(csv_data_list)
                    except Exception as e:
                        g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))
# Write the data of last year into csv
                if len(csv_data_list_each_year) > 0:
                    self._write_to_csv(csv_filepath, csv_data_list_each_year, self.source_url_parsing_cfg["url_multi_data_one_page"])


    def assemble_web_url(self, timeslice, company_code_number):
        raise NotImplementedError
