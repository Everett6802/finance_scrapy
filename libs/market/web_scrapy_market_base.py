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
# Determine the time range
        if self.xcfg["time_start"] is None:
            self.xcfg["time_start"] = self.__get_url_date_range().get_date_range_start(self.source_type_index)
        if self.xcfg["time_end"] is None:
            self.xcfg["time_end"] = self.__get_url_date_range().get_date_range_end(self.source_type_index)
        if self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY:
            self.time_slice_kwargs["time_start"] = self.xcfg["time_start"]
            self.time_slice_kwargs["time_end"] = self.xcfg["time_end"]
        elif self.url_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
            self.time_slice_kwargs["time_start"] = CMN.CLS.FinanceMonth(self.xcfg["time_start"].year, self.xcfg["time_start"].month)
            self.time_slice_kwargs["time_end"] = CMN.CLS.FinanceMonth(self.xcfg["time_end"].year, self.xcfg["time_end"].month)
        else:
            raise ValueError("Unsupported time unit: %d" % self.url_time_unit)


    @classmethod
    def __get_url_date_range(cls):
        # import pdb; pdb.set_trace()
        if cls.url_date_range is None:
            cls.url_date_range = URLDateRange.WebScrapyURLDateRange.Instance()
        return cls.url_date_range


    # @classmethod
    # def __get_time_slice_generator(cls):
    #     if cls.TIME_SLICE_GENERATOR is None:
    #         cls.TIME_SLICE_GENERATOR = BASE.TSG.WebScrapyTimeSliceGenerator.Instance()
    #     return cls.TIME_SLICE_GENERATOR


    @classmethod
    def assemble_csv_filepath(cls, data_source_index):
        csv_filepath = "%s/%s/%s.csv" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index]) 
        return csv_filepath


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
# Find the file path for writing data into csv
        csv_filepath = WebScrapyMarketBase.assemble_csv_filepath(self.source_type_index)
# Generate the time slice list
        timeslice_iterable = self._get_time_slice_generator().generate_time_slice(self.timeslice_generate_method, **self.time_slice_kwargs)
        csv_data_list_each_year = []
        cur_year = None
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
                csv_data_list = self.parse_web_data(self._get_web_data(url))
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
