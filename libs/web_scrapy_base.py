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
import common as CMN
# from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
from libs import web_scrapy_timeslice_generator as TimeSliceGenerator
from libs import web_scrapy_company_profile as CompanyProfile
from libs import web_scrapy_company_group_set as CompanyGroupSet
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebScrapyBase(object):

    PARSE_URL_DATA_FUNC_PTR = None
    timeslice_generator = None
    def __init__(self, cur_file_path, **kwargs):
        self.xargs = {
            "time_start": None,
            "time_end": None,
        }
        self.xargs.update(kwargs)
# Find which module is instansiate
        cur_module_name = re.sub(CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX, "", CMN.FUNC.get_cur_module_name(cur_file_path))
# Find correspnding index of the module
        self.data_source_index = CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index(cur_module_name)
# Find corresponding config of the module
        self.source_url_parsing_cfg = CMN.DEF.DEF_SOURCE_URL_PARSING[self.data_source_index]

        self.timeslice_generate_method = self.source_url_parsing_cfg["url_timeslice"]
        # csv_time_unit = CMN.DEF.DEF_CSV_TIME_UNIT[self.data_source_index]
        url_time_unit = CMN.TIMESLICE_TO_TIME_UNIT_MAPPING[self.timeslice_generate_method]
        # self.scrap_web_to_csv_func_ptr = self.__scrap_multiple_web_data_to_single_csv_file if url_time_unit == csv_time_unit self.__scrap_single_web_data_to_single_csv_file
        # self.timeslice_iterable = timeslice_generator_obj.generate_time_slice(self.timeslice_generate_method, **kwargs)
# To be fixed...
        self.time_slice_kwargs = {}
        self.time_slice_kwargs['time_start'] = self.xargs.get("time_start", None)
        self.time_slice_kwargs['time_end'] = self.xargs.get("time_end", None)
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
    def __get_parse_url_data_func_ptr(cls):
        if cls.PARSE_URL_DATA_FUNC_PTR is None:
            cls.PARSE_URL_DATA_FUNC_PTR = [cls.__select_web_data_by_bs4, cls.__select_web_data_by_json]
        return cls.PARSE_URL_DATA_FUNC_PTR


    @classmethod
    def __select_web_data_by_bs4(cls, url_data, url_css_selector):
        url_data.encoding = getattr(self.parse_url_data_type_obj, "encoding")
        soup = BeautifulSoup(url_data.text)
        return soup.select(url_css_selector)


    @classmethod
    def __select_web_data_by_json(cls, url_data, url_css_selector):
        json_url_data = json.loads(url_data.text)
        return json_url_data[url_css_selector]


    @staticmethod
    def _write_to_csv(csv_filepath, csv_data_list):
        with open(csv_filepath, 'w') as fp:
            fp_writer = csv.writer(fp, delimiter=',')
# Write the web data into CSV
            fp_writer.writerows(csv_data_list)


    # def __generate_timeslice_list(self):
    #     if not self.timeslice_list_generated:
    #         timeslice_list = list(self.timeslice_iterable)
    #         self.timeslice_start = timeslice_list[0]
    #         self.timeslice_end = timeslice_list[-1]
    #         self.timeslice_cnt = len(timeslice_list)
    #         self.timeslice_list_generated = True
    #         if self.timeslice_start == self.timeslice_end:
    #             msg = "%s: %04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], config['start'].year, config['start'].month, config['start'].day)
    #         else:
    #             msg = "%s: %04d-%02d-%02d:%04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], config['start'].year, config['start'].month, config['start'].day, config['end'].year, config['end'].month, config['end'].day)
    #         g_logger.info(msg)


    def get_description(self):
        if self.description is None:
            self.description = "%s" % CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index]
            # if show_detail:
            #     if not self.timeslice_list_generated:
            #         self.__generate_timeslice_list()
            #     if self.timeslice_start == self.timeslice_end:
            #         self.description = "%s[%s]" % (
            #             CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], 
            #             self.timeslice_start.to_string()
            #         )
            #     else:
            #         self.description = "%s[%s-%s]" % (
            #             CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], 
            #             self.timeslice_start.to_string(), 
            #             self.timeslice_end.to_string()
            #         )
            # else:
            #     self.description = "%s" % CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index]
        return self.description


    # def get_timeslice_list_description(self):
    #     if self.timeslice_list_description is None:
    #         if not self.timeslice_list_generated:
    #             self.__generate_timeslice_list()
    #         self.timeslice_list_description = "Range[%s %s]; Totally %d data" % (self.timeslice_start.to_string, self.timeslice_end.to_string(), self.timeslice_cnt)
    #     return self.timeslice_list_description


    def get_data_source_index(self):
        return self.data_source_index


    def __get_web_data(self, url):
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
                # import pdb; pdb.set_trace()
                g_logger.error("Fail to scrap web data [%s] even retry for %d times !!!!!!" % (url, self.SCRAPY_RETRY_TIMES))
                raise e
        # parse_url_data_type = self.parse_url_data_type_obj.get_type()
        # return (self.PARSE_URL_DATA_FUNC_PTR[parse_url_data_type])(res)
        url_parsing_method = self.source_url_parsing_cfg[url_parsing_method]
        url_css_selector = self.source_url_parsing_cfg[url_css_selector]
        g_logger.debug("Parse URL data by method: %d, select by selector: %s" % (url_parsing_method, url_css_selector))
        return (self.__get_parse_url_data_func_ptr()[url_parsing_method])(res, url_css_selector)


    # def __check_datetime_input(self, datetime_range_start, datetime_range_end):
    #     datetime_tmp = datetime.today()
    #     datetime_today = datetime(datetime_tmp.year, datetime_tmp.month, datetime_tmp.day)
    #     datetime_start = None
    #     datetime_end = None
    #     if datetime_range_start is None:
    #         if datetime_range_end is not None:
    #             raise ValueError("datetime_range_start is None but datetime_range_end is NOT None")
    #         else:
    #             datetime_start = datetime_end = datetime_today
    #             g_logger.debug("Only grab the data today[%s]" % CMN.to_date_only_str(datetime_today))
    #     else:
    #         datetime_start = datetime_range_start
    #         if datetime_range_end is not None:
    #             datetime_end = datetime_range_end
    #         else:
    #             datetime_end = datetime_today
    #         g_logger.debug("Grab the data from date[%s] to date[%s]" % (CMN.to_date_only_str(datetime_start), CMN.to_date_only_str(datetime_end)))

    #     return (datetime_start, datetime_end)


    # def __reset_time_slice_buffer(self):
    #     self.timeslice_buffer = []
    #     self.timeslice_buffer_len = 0


    # def __add_to_time_slice_buffer(self, timeslice):
    #     assert (elf.timeslice_buffer is not None), "self.timeslice_buffer is None"
    #     timeslice_buffer = None
    #     if self.timeslice_buffer_len != 0 && not self.is_same_time_unit(timeslice):
    #         timeslice_buffer = self.timeslice_buffer
    #         self.__reset_time_slice_buffer()
    #     self.timeslice_buffer.append(timeslice)
    #     self.timeslice_buffer_len += 1
    #     return timeslice_buffer


    def parse_web_data(self, web_data):
        raise NotImplementedError


    def is_same_time_unit(self, timeslice):
        raise NotImplementedError


    # def scrap_web_to_csv(self):
    #     raise NotImplementedError


#############################################################################################


class WebScrapyMarketBase(WebScrapyBase):

    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyMarketBase, self).__init__(cur_file_path, **kwargs)


    @staticmethod
    def assemble_csv_filepath(cls, data_source_index):
        csv_filepath = "%s/%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index]) 
        return csv_filepath


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
        timeslice_iterable = self._get_time_slice_generator().generate_time_slice(self.timeslice_generate_method, **self.time_slice_kwargs)
        for timeslice in timeslice_iterable:
            url = self.assemble_web_url(timeslice)
            g_logger.debug("Get the data by date from URL: %s" % url)
            try:
# Grab the data from website and assemble the data to the entry of CSV
                csv_data_list = self.parse_web_data(self.__get_web_data(url))
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


#############################################################################################


class WebScrapyStockBase(WebScrapyBase):

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
        csv_filepath = "%s/%s%02d/%s%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, company_group_index, company_code_number, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[self.data_source_index]) 
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
                        csv_data_list = self.parse_web_data(self.__get_web_data(url))
                        if csv_data_list is None:
                            raise RuntimeError(url)
                        csv_filepath = self.assemble_csv_filepath(timeslice, company_code_number, company_group_number)
                        g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
                        WebScrapyBase._write_to_csv(csv_filepath, csv_data_list)
                    except Exception as e:
                        g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))

        return CMN.RET_SUCCESS


    def assemble_web_url(self, timeslice, company_code_number):
        raise NotImplementedError
