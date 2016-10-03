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
import web_scrapy_timeslice_generator as TimeSliceGenerator
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyBase(object):

    PARSE_URL_DATA_FUNC_PTR = None
    GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR = None
    timeslice_generator = None
    url_time_range = None
    def __init__(self, cur_file_path, **kwargs):
        self.xcfg = {
            "time_duration_type": CMN.DEF.DATA_TIME_DURATION_TODAY,
            "time_duration_start": None,
            "time_duration_end": None,
            "dry_run_only": False,
        }
        # import pdb; pdb.set_trace()
        self.xcfg.update(kwargs)
# Find which module is instansiate
        cur_module_name = re.sub(CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX, "", CMN.FUNC.get_cur_module_name(cur_file_path))
# Find correspnding index of the module
        self.source_type_index = CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index(cur_module_name)
# Find corresponding config of the module
        self.source_url_parsing_cfg = CMN.DEF.DEF_SOURCE_URL_PARSING[self.source_type_index]

        self.timeslice_generate_method = self.source_url_parsing_cfg["url_timeslice"]
        self.url_format = self.source_url_parsing_cfg["url_format"]
        self.url_parsing_method = self.source_url_parsing_cfg["url_parsing_method"]
        # csv_time_unit = CMN.DEF.DEF_CSV_TIME_UNIT[self.source_type_index]
        self.url_time_unit = CMN.DEF.TIMESLICE_TO_TIME_UNIT_MAPPING[self.timeslice_generate_method]
        # self.scrap_web_to_csv_func_ptr = self.__scrap_multiple_web_data_to_single_csv_file if url_time_unit == csv_time_unit self.__scrap_single_web_data_to_single_csv_file
        # self.timeslice_iterable = timeslice_generator_obj.generate_time_slice(self.timeslice_generate_method, **kwargs)
# To be fixed...
        self.time_slice_kwargs = {"time_duration_start": None, "time_duration_end": None}
        self.description = None

        # self.timeslice_list_generated = False
#         self.timeslice_buffer = None
#         self.timeslice_buffer_len = -1
# # It's required to create the time slice list to setup the following variables
#         self.timeslice_start = None
#         self.timeslice_end = None
#         self.timeslice_list_description = None
#         self.timeslice_cnt = None


    @classmethod
    def _get_time_slice_generator(cls):
        if cls.timeslice_generator is None:
            cls.timeslice_generator = TimeSliceGenerator.WebScrapyTimeSliceGenerator.Instance()
        return cls.timeslice_generator


    @classmethod
    def get_parse_url_data_func_ptr(cls):
        if cls.PARSE_URL_DATA_FUNC_PTR is None:
            cls.PARSE_URL_DATA_FUNC_PTR = [cls.__select_web_data_by_bs4, cls.__select_web_data_by_json]
        return cls.PARSE_URL_DATA_FUNC_PTR


    @classmethod
    def __select_web_data_by_bs4(cls, url_data, parse_url_data_type_cfg):
        g_logger.debug("Parse URL data by BS4, Encoding:%s, Selector: %s" % (parse_url_data_type_cfg["url_encoding"], parse_url_data_type_cfg["url_css_selector"]))
        url_data.encoding = parse_url_data_type_cfg["url_encoding"]
        soup = BeautifulSoup(url_data.text)
        return soup.select(parse_url_data_type_cfg["url_css_selector"])


    @classmethod
    def __select_web_data_by_json(cls, url_data, parse_url_data_type_cfg):
        g_logger.debug("Parse URL data by JSON, Selector: %s" % parse_url_data_type_cfg["url_css_selector"])
        json_url_data = json.loads(url_data.text)
        return json_url_data[parse_url_data_type_cfg["url_css_selector"]]


    @classmethod
    def _write_to_csv(cls, csv_filepath, csv_data_list, multi_data_one_page):
        # import pdb; pdb.set_trace()
        if multi_data_one_page:
            g_logger.debug("Multi-data in one page, need to transform data")
            csv_data_list_tmp = csv_data_list
            csv_data_list = []
            for csv_data_tmp in csv_data_list_tmp:
                csv_data_list.extend(csv_data_tmp)

        g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
        with open(csv_filepath, 'a+') as fp:
            fp_writer = csv.writer(fp, delimiter=',')
# Write the web data into CSV
            fp_writer.writerows(csv_data_list)


    @classmethod
    def _get_url_time_range(cls):
        raise NotImplementedError


    # def __generate_timeslice_list(self):
    #     if not self.timeslice_list_generated:
    #         timeslice_list = list(self.timeslice_iterable)
    #         self.timeslice_start = timeslice_list[0]
    #         self.timeslice_end = timeslice_list[-1]
    #         self.timeslice_cnt = len(timeslice_list)
    #         self.timeslice_list_generated = True
    #         if self.timeslice_start == self.timeslice_end:
    #             msg = "%s: %04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], config['start'].year, config['start'].month, config['start'].day)
    #         else:
    #             msg = "%s: %04d-%02d-%02d:%04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], config['start'].year, config['start'].month, config['start'].day, config['end'].year, config['end'].month, config['end'].day)
    #         g_logger.info(msg)


    def get_description(self):
        if self.description is None:
            self.description = "%s" % CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index]
            # if show_detail:
            #     if not self.timeslice_list_generated:
            #         self.__generate_timeslice_list()
            #     if self.timeslice_start == self.timeslice_end:
            #         self.description = "%s[%s]" % (
            #             CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], 
            #             self.timeslice_start.to_string()
            #         )
            #     else:
            #         self.description = "%s[%s-%s]" % (
            #             CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], 
            #             self.timeslice_start.to_string(), 
            #             self.timeslice_end.to_string()
            #         )
            # else:
            #     self.description = "%s" % CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index]
        return self.description


    def _get_web_data(self, url):
        try:
            # g_logger.debug("Try to Scrap data [%s]" % url)
            res = requests.get(url, timeout=CMN.DEF.DEF_SCRAPY_WAIT_TIMEOUT)
        except requests.exceptions.Timeout as e:
            # g_logger.debug("Try to Scrap data [%s]... Timeout" % url)
            fail_to_scrap = False
            for index in range(CMN.DEF.DEF_SCRAPY_RETRY_TIMES):
                time.sleep(randint(3,9))
                try:
                    # g_logger.debug("Retry to scrap web data [%s]......%d" % (url, index))
                    res = requests.get(url, timeout=CMN.DEF.DEF_SCRAPY_WAIT_TIMEOUT)
                except requests.exceptions.Timeout as ex:
                    # g_logger.debug("Retry to scrap web data [%s]......%d, FAIL!!!" % (url, index))
                    fail_to_scrap = True
                if not fail_to_scrap:
                    break
            if fail_to_scrap:
                g_logger.error("Fail to scrap web data [%s] even retry for %d times !!!!!!" % (url, self.SCRAPY_RETRY_TIMES))
                raise e
        # parse_url_data_type = self.parse_url_data_type_obj.get_type()
        # return (self.PARSE_URL_DATA_FUNC_PTR[parse_url_data_type])(res)
        # import pdb; pdb.set_trace()
        return (self.get_parse_url_data_func_ptr()[self.url_parsing_method])(res, self.source_url_parsing_cfg)


    def _adjust_time_duration_start_and_end_time_func_ptr(self, time_duration_type):
        if self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR is None:
            self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR = [self._adjust_time_today_start_and_end_time, self._adjust_time_last_start_and_end_time, self._adjust_time_range_start_and_end_time]
        return self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR[time_duration_type]


    def _adjust_time_today_start_and_end_time(self, *args):
        self.xcfg["time_duration_start"] = self.xcfg["time_duration_end"] = CMN.CLS.FinanceDate.get_today_finance_date()


    def _adjust_time_last_start_and_end_time(self, *args):
        today_data_exist_hour = CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_HOUR if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR
        today_data_exist_minute = CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_MINUTE if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR
        self.xcfg["time_duration_start"] = self.xcfg["time_duration_end"] = CMN.FUNC.get_last_url_data_date(today_data_exist_hour, today_data_exist_minute) 


    def _adjust_time_range_start_and_end_time(self, *args):
# in Market mode
# args[0]: source_type_index
# in Stock mode
# args[0]: source_type_index
# args[1]: company_code_number
        time_duration_start_from_lookup_table = self._get_url_time_range().get_time_range_start(*args)
        time_duration_end_from_lookup_table = self._get_url_time_range().get_time_range_end(*args)
        if self.xcfg["time_duration_start"] is None:
            self.xcfg["time_duration_start"] = time_duration_start_from_lookup_table
        else:
            if self.xcfg["time_duration_start"] < time_duration_start_from_lookup_table:
                self.xcfg["time_duration_start"] = time_duration_start_from_lookup_table    
        if self.xcfg["time_duration_end"] is None:
            self.xcfg["time_duration_end"] = time_duration_end_from_lookup_table
        else:
            if self.xcfg["time_duration_end"] > time_duration_end_from_lookup_table:
                self.xcfg["time_duration_end"] = time_duration_end_from_lookup_table    


    def _parse_web_data(self, web_data):
        raise NotImplementedError
