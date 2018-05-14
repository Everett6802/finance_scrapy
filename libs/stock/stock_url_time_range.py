# -*- coding: utf8 -*-

import os
import sys
import re
import time
import collections
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
# import web_scrapy_company_profile as CompanyProfile
# import web_scrapy_company_group_set as CompanyGroupSet
# import web_scrapy_timeslice_generator as TimeSliceGenerator
g_logger = CMN.LOG.get_logger()
g_profile_lookup = BASE.CP.CompanyProfile.Instance()


@CMN.CLS.Singleton
class StockURLTimeRange(object):

    def __init__(self):
        # import pdb; pdb.set_trace()
        self.TIME_RANGE_ROOT_FOLDERPATH = CMN.FUNC.get_config_filepath(CMN.DEF.TIME_RANGE_FOLDERNAME)
        self.TIME_RANGE_COMPANY_GROUP_FOLDERPATH_FORMAT = ("%s/%s" % (self.TIME_RANGE_ROOT_FOLDERPATH, CMN.DEF.CSV_STOCK_FOLDERNAME)) + "%02d"
# Caution: 
# Since CompanyProfile/URLTimeRange are the Singleton pattern.
# In the current implementation, One Singleton object can't be instanitate 
# in another Singleton object's instantiation. The singleton_thread_lock variable
# is deadlock in this situation
        # g_profile_lookup = BASE.CP.CompanyProfile.Instance()
        self.COMPANY_GROUP_SIZE = g_profile_lookup.CompanyGroupSize
        # self.stock_offset_scrapy_class_index_list = [scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_START for scrapy_class_index in self.scrapy_class_index_list]
        first_url_data_date = CMN.CLS.FinanceDate(CMN.FUNC.get_year_offset_datetime_cfg(datetime.today(), -1))
        self.METHOD_START_SCAN_TIME_CFG = [
            first_url_data_date,
            # # CMN.CLS.FinanceQuarter(CMN.DEF.STATEMENT_START_QUARTER_STR),
            # # CMN.CLS.FinanceQuarter(CMN.DEF.STATEMENT_START_QUARTER_STR),
            # # CMN.CLS.FinanceQuarter(CMN.DEF.STATEMENT_START_QUARTER_STR),
            # # CMN.CLS.FinanceQuarter(CMN.DEF.STATEMENT_START_QUARTER_STR),
            # CMN.CLS.FinanceDate(CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR),
            # CMN.CLS.FinanceDate(CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR),
            # CMN.CLS.FinanceDate(CMN.DEF.TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_START_DATE_STR),
            # CMN.CLS.FinanceDate(CMN.DEF.TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_START_DATE_STR),
            first_url_data_date,
            first_url_data_date,
            first_url_data_date,
            first_url_data_date,
        ]
        last_url_data_date = CMN.CLS.FinanceDate.get_last_finance_date()
        # last_url_data_quarter = CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(last_url_data_date)
        self.METHOD_END_TIME_CFG = [
            last_url_data_date,
            # last_url_data_quarter,
            # last_url_data_quarter,
            # last_url_data_quarter,
            # last_url_data_quarter,
            last_url_data_date,
            last_url_data_date,
            last_url_data_date,
            last_url_data_date,
        ]
        self.whole_company_group_set = None
        self.timeslice_generator = None
        self.scrapy_class_index_list = None
        self.web_scrapy_class_dict = None
        self.company_data_source_start_time_dict = {}
        self.auto_update = True


    def __get_timeslice_generator(self):
        if self.timeslice_generator is None:
            self.timeslice_generator = BASE.TSG.TimeSliceGenerator.Instance()
        return self.timeslice_generator


    def __get_company_group_time_range_folderpath(self, company_group):
        return self.TIME_RANGE_COMPANY_GROUP_FOLDERPATH_FORMAT % company_group


    def __get_company_number_time_range_filepath(self, company_number, company_group=None):
        if company_group is None:
            company_group = g_profile_lookup.lookup_company_group_number(company_number)
        return self.__get_company_group_time_range_folderpath(int(company_group)) + "/%s" % company_number


    def __create_time_range_folder_if_not_exist(self):
        CMN.FUNC.create_folder_if_not_exist(self.TIME_RANGE_ROOT_FOLDERPATH)
        for company_group in range(self.COMPANY_GROUP_SIZE):
            folderpath = self.__get_company_group_time_range_folderpath(company_group)
            CMN.FUNC.create_folder_if_not_exist(folderpath)


    def __remove_old_time_range_folder(self):
        if not CMN.FUNC.check_file_exist(self.TIME_RANGE_ROOT_FOLDERPATH):
            return
        for company_group in range(self.COMPANY_GROUP_SIZE):
            folderpath = self.__get_company_group_time_range_folderpath(company_group)
            # g_logger.debug("Remove old folder: %s" % folderpath)
            CMN.FUNC.remove_folder_if_exist(folderpath)


    def _create_finance_folder_if_not_exist(self, finance_root_folderpath=None):
        self._create_finance_root_folder_if_not_exist(finance_root_folderpath)
        folderpath_format = self.__get_finance_folderpath_format(finance_root_folderpath)
        for index in range(self.__get_company_profile().CompanyGroupSize):
            folderpath = folderpath_format % index
            # g_logger.debug("Try to create new folder: %s" % folderpath)
            CMN.FUNC.create_folder_if_not_exist(folderpath)


    def _remove_old_finance_folder(self, finance_root_folderpath=None):
# Remove the old data if necessary
        folderpath_format = self.__get_finance_folderpath_format(finance_root_folderpath)
        for index in range(self.__get_company_profile().CompanyGroupSize):
            folderpath = folderpath_format % index
            # g_logger.debug("Remove old folder: %s" % folderpath)
            shutil.rmtree(folderpath, ignore_errors=True)


    def __read_company_time_range_start_from_config(self, company_number, company_group=None):
        company_number_time_range_filepath = self.__get_company_number_time_range_filepath(company_number, company_group)
        if not CMN.FUNC.check_file_exist(company_number_time_range_filepath):
            return None
        company_time_range_start_ordereddict = collections.OrderedDict()
        line_list = CMN.FUNC.read_file_lines_ex(company_number_time_range_filepath, "r")
        for line in line_list:
            [scrapy_class_index_str, company_time_range_start_str] = line.split(CMN.DEF.SPACE_DATA_SPLIT)
            scrapy_class_index = int(scrapy_class_index_str)
            if company_time_range_start_ordereddict.has_key(scrapy_class_index):
                raise ValueError("Duplicate source type index: %d" % scrapy_class_index)
            company_time_range_start_ordereddict[scrapy_class_index] = CMN.CLS.FinanceTimeBase.from_time_string(company_time_range_start_str)
        return company_time_range_start_ordereddict


    def __write_company_time_range_start_to_config(self, company_time_range_start_ordereddict, company_number, company_group=None):
        company_number_time_range_filepath = self.__get_company_number_time_range_filepath(company_number, company_group)
        line_list = []
        for scrapy_class_index, company_time_range_start in company_time_range_start_ordereddict.items():
            line = "%d%s%s" % (scrapy_class_index, CMN.DEF.SPACE_DATA_SPLIT, company_time_range_start.to_string())
            line_list.append(line)
        CMN.FUNC.write_file_lines_ex(line_list, company_number_time_range_filepath, "w")


    def __get_web_scrapy_class(self, scrapy_class_index):
        # import pdb;pdb.set_trace()
        if self.web_scrapy_class_dict is None:
            self.web_scrapy_class_dict = {}
        if not CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_index):
            raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Incorrect source type index: %d" % scrapy_class_index)
        if not self.web_scrapy_class_dict.has_key(scrapy_class_index):
            web_scrapy_class = CMN.FUNC.get_web_scrapy_class(scrapy_class_index)
            # web_scrapy_class.init_class_common_variables()
            self.web_scrapy_class_dict[scrapy_class_index] = web_scrapy_class
        return self.web_scrapy_class_dict[scrapy_class_index]


    def __scan_company_time_range_start(self, company_number, company_group=None):
        # import pdb; pdb.set_trace()
        company_time_range_start_ordereddict = self.__read_company_time_range_start_from_config(company_number, company_group)
        if company_time_range_start_ordereddict is None:
            company_time_range_start_ordereddict = collections.OrderedDict()
        for scrapy_class_index in self.scrapy_class_index_list:
# The start time already exist, no need to search
            if company_time_range_start_ordereddict.has_key(scrapy_class_index):
                continue
            # import pdb;pdb.set_trace()
# Get the web scrapy class
            web_scrapy_class = self.__get_web_scrapy_class(scrapy_class_index)
            if web_scrapy_class.CAN_FIND_TIME_RANGE_START:
# Find the start time from company profile..., .etc
                first_web_data_time_str = web_scrapy_class.find_time_range_start(company_number)
                if first_web_data_time_str is not None:
                    company_time_range_start_ordereddict[scrapy_class_index] = CMN.CLS.FinanceTimeBase.from_time_string(first_web_data_time_str)
                    continue
# Scan to find the start time from the web
# Define the time range for scanning the start time
            stock_scrapy_class_index_offset = scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_START
            time_slice_generator_cfg = {
                "company_code_number": company_number, 
                "time_duration_start": self.METHOD_START_SCAN_TIME_CFG[stock_scrapy_class_index_offset], 
                "time_duration_end": self.METHOD_END_TIME_CFG[scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_START],
            }
            # import pdb; pdb.set_trace()
# Generate the time slice
            time_slice_generator_cfg["time_duration_start"], time_slice_generator_cfg["time_duration_end"] = web_scrapy_class.get_time_unit_for_timeslice_iterable(**time_slice_generator_cfg)
            timeslice_iterable = self.__get_timeslice_generator().generate_time_slice(web_scrapy_class.TIMESLICE_GENERATE_METHOD, **time_slice_generator_cfg)
# Scan the start time
            data_exist = False
            time_duration_start = None
            for timeslice in timeslice_iterable:
                url = web_scrapy_class.assemble_web_url(timeslice, company_number)
                g_logger.debug("Check the data exist from URL: %s" % url)
                if web_scrapy_class.NEED_FIRST_WEB_DATA_TIME:
                    web_data = web_scrapy_class.try_get_web_data(url, True)
                    if web_data is not None:
                        first_web_data_time_str = web_scrapy_class.get_first_web_data_time(web_data)
                        company_time_range_start_ordereddict[scrapy_class_index] = CMN.CLS.FinanceTimeBase.from_time_string(first_web_data_time_str)
                        data_exist = True
                else:
                    data_exist = web_scrapy_class.check_web_data_exist(url)
                    if data_exist:
                        company_time_range_start_ordereddict[scrapy_class_index] = timeslice
                if data_exist:
                    break
            if not data_exist:
                if not web_scrapy_class.check_company_first_data_exist(company_number):
                    g_logger.warn("The %s:%s first data does NOT EXIST !!!" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_index], company_number))
                    continue
                raise CMN.EXCEPTION.WebScrapyNotFoundException("Fail to check if the data[%s:%d] exist" % (company_number, scrapy_class_index))
        self.__write_company_time_range_start_to_config(company_time_range_start_ordereddict, company_number, company_group)
        return company_time_range_start_ordereddict


    def __scan_time_range_start(self, company_word_list_string = None):
# Update the time range of time slice
        # import pdb; pdb.set_trace()
        # company_group_set = BASE.CGS.CompanyGroupSet()
        # company_group_set.add_company("1256")
        # company_group_set.add_done()
        # for company_group, company_number_list in company_group_set.items():
        company_group_set = BASE.CGS.CompanyGroupSet.create_instance_from_string(company_word_list_string) if company_word_list_string is not None else self.whole_company_group_set
        for company_group, company_number_list in company_group_set.items():
            for company_number in company_number_list:
                self.__scan_company_time_range_start(company_number, company_group)


    def initialize(self):
        # import pdb; pdb.set_trace()
        self.whole_company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        self.scrapy_class_index_list = CMN.FUNC.get_scrapy_class_index_range_list()
        self.company_listing_date_dict = {}


    def renew_time_range(self, company_word_list_string = None, cleanup_old=True):
        # import pdb; pdb.set_trace()
        if cleanup_old:
            self.__remove_old_time_range_folder()
        self.__create_time_range_folder_if_not_exist()
        self.__scan_time_range_start(company_word_list_string)


    def __update_company_time_range(self, company_number, company_group):
        need_scan = False
        company_time_range_start_ordereddict = self.__read_company_time_range_start_from_config(company_number, company_group)
        if company_time_range_start_ordereddict is None:
            if self.auto_update:
                need_scan = True
            else:
                raise CMN.EXCEPTION.WebScrapyNotFoundException("Fail to find the time range in company[%s]" % company_number)
        else:
            for scrapy_class_index in self.scrapy_class_index_list:
                if not company_time_range_start_ordereddict.has_key(scrapy_class_index):
                    need_scan = True
                    break
        if need_scan:
            company_time_range_start_ordereddict = self.__scan_company_time_range_start(company_number, company_group)
        self.company_data_source_start_time_dict[company_number] = company_time_range_start_ordereddict


    def update_time_range(self):
        if not CMN.FUNC.check_file_exist(self.TIME_RANGE_ROOT_FOLDERPATH):
            raise CMN.EXCEPTION.WebScrapyNotFoundException("The company time range folder[%s] is NOT found" % self.TIME_RANGE_ROOT_FOLDERPATH)
        for company_group, company_number_list in self.whole_company_group_set.items():
            for company_number in company_number_list:
                self.__update_company_time_range(company_number, company_group)


    def get_time_range_start_all_dict(self, company_number, company_group=None):
        # import pdb; pdb.set_trace()
        if not self.company_data_source_start_time_dict.has_key(company_number):
            self.__update_company_time_range(company_number, company_group)
        return  self.company_data_source_start_time_dict[company_number]


    def get_time_range_start(self, scrapy_class_index, company_number, company_group=None):
        # import pdb;pdb.set_trace()
        CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_index)
        return self.get_time_range_start_all_dict(company_number, company_group)[scrapy_class_index]


    def get_time_range_end(self, scrapy_class_index):
        CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_index)
        return self.METHOD_END_TIME_CFG[scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_START]


    def get_time_range(self, scrapy_class_index, company_number=None, company_group=None):
        # import pdb;pdb.set_trace()
# If the company_number is None, stop scanning the start time, just exploit the default start time
        if company_number is None:
            return (self.METHOD_START_SCAN_TIME_CFG[scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_START], self.METHOD_END_TIME_CFG[scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_START])
        return (self.get_time_range_start(scrapy_class_index, company_number, company_group), self.get_time_range_end(scrapy_class_index))


    @property
    def AutoUpdate(self):
        return self.auto_update


    @AutoUpdate.setter
    def AutoUpdate(self, new_auto_update):
        self.auto_update = new_auto_update
