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
import web_scrapy_url_date_range as URLDateRange
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyMarketBase(BASE.BASE.WebScrapyBase):

    url_date_range = None
    # TIME_SLICE_GENERATOR = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyMarketBase, self).__init__(cur_file_path, **kwargs)
        # import pdb; pdb.set_trace()


    @classmethod
    def __get_url_date_range(cls):
        # import pdb; pdb.set_trace()
        if cls.url_date_range is None:
            cls.url_date_range = URLDateRange.WebScrapyURLDateRange.Instance()
        return cls.url_date_range


    @classmethod
    def _get_time_last_start_and_end_time(cls, *args):
        last_finance_date = CMN.FUNC.get_last_url_data_date(CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_MINUTE) 
        return (last_finance_date, last_finance_date)


    @classmethod
    def _get_time_range_start_and_end_time(cls, *args):
# args[0]: source_type_index
        return (cls.__get_url_date_range().get_date_range_start(args[0]), cls.__get_url_date_range().get_date_range_end(args[0]))


    @classmethod
    def assemble_csv_filepath(cls, data_source_index):
        csv_filepath = "%s/%s/%s.csv" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index]) 
        return csv_filepath


    def _adjust_time_duration_from_lookup_table(self):
        (self.xcfg["time_duration_start"], self.xcfg["time_duration_end"]) = (self.get_time_duration_start_and_end_time_func_ptr(self.xcfg["time_duration_type"]))(self.source_type_index)
        # if self.xcfg["time_duration_start"] is None:
        #     self.xcfg["time_duration_start"] = self.__get_url_date_range().get_date_range_start(self.source_type_index)
        # if self.xcfg["time_duration_end"] is None:
        #     self.xcfg["time_duration_end"] = self.__get_url_date_range().get_date_range_end(self.source_type_index)
# Transform to correct time unit
        if self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY:
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
# Find the file path for writing data into csv
        csv_filepath = WebScrapyMarketBase.assemble_csv_filepath(self.source_type_index)
# Determine the actual time range
        self._adjust_time_duration_from_lookup_table()
        scrapy_msg = "[%s] %s %s-%s => %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[source_type_time_duration.time_duration_type], source_type_time_duration.time_duration_start,source_type_time_duration.time_duration_end, csv_filepath)
        g_logger.debug(scrap_msg)
# Check if only dry-run
        if self.xcfg["dry_run_only"]:
            print scrapy_msg
            return
# Create the time slice iterator due to correct time range
        timeslice_iterable = self._get_time_slice_generator().generate_time_slice(self.timeslice_generate_method, **self.time_slice_kwargs)
        csv_data_list_each_year = []
        cur_year = None
# Generate the time slice list
        for timeslice in timeslice_iterable:
# Write the data into csv year by year
            if timeslice.year != cur_year:
                if len(csv_data_list_each_year) > 0:
                    self._write_to_csv(csv_filepath, csv_data_list_each_year, self.source_url_parsing_cfg["url_multi_data_one_page"])
                    csv_data_list_each_year = []
                cur_year = timeslice.year
            url = self.assemble_web_url(timeslice)
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


    def assemble_web_url(self, timeslice):
        raise NotImplementedError
