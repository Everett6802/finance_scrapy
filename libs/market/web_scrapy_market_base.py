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
# import libs.base.web_scrapy_base as WebScrapyBase
# import libs.base.web_scrapy_timeslice_generator as TimeSliceGenerator
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyMarketBase(BASE.BASE.WebScrapyBase):

    URL_DATE_RANGE = None
    TIME_SLICE_GENERATOR = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyMarketBase, self).__init__(cur_file_path, **kwargs)
        # import pdb; pdb.set_trace()
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
        if cls.URL_DATE_RANGE is None:
            cls.URL_DATE_RANGE = URLDateRange.WebScrapyURLDateRange.Instance()
        return cls.URL_DATE_RANGE


    @classmethod
    def __get_time_slice_generator(cls):
        if cls.TIME_SLICE_GENERATOR is None:
            cls.TIME_SLICE_GENERATOR = BASE.TSG.WebScrapyTimeSliceGenerator.Instance()
        return cls.TIME_SLICE_GENERATOR


    @staticmethod
    def assemble_csv_filepath(cls, data_source_index):
        csv_filepath = "%s/%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index]) 
        return csv_filepath


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
        timeslice_iterable = self.__get_time_slice_generator().generate_time_slice(self.timeslice_generate_method, **self.time_slice_kwargs)
        for timeslice in timeslice_iterable:
            url = self.assemble_web_url(timeslice)
            g_logger.debug("Get the data by date from URL: %s" % url)
            try:
# Grab the data from website and assemble the data to the entry of CSV
                csv_data_list = self.parse_web_data(self._get_web_data(url))
                if csv_data_list is None:
                    raise RuntimeError(url)
                csv_filepath = self.assemble_csv_filepath(timeslice)
                g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
                WebScrapyBase._write_to_csv(csv_filepath, csv_data_list)
            except Exception as e:
                g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))

        return CMN.RET_SUCCESS


    def assemble_web_url(self, timeslice):
        raise NotImplementedError
