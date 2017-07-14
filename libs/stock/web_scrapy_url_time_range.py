# -*- coding: utf8 -*-

import os
import sys
import re
import time
import collections
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_company_profile as CompanyProfile
import web_scrapy_company_group_set as CompanyGroupSet
# import web_scrapy_timeslice_generator as TimeSliceGenerator
g_logger = CMN.WSL.get_web_scrapy_logger()
g_profile_lookup = CompanyProfile.WebScrapyCompanyProfile.Instance()


@CMN.CLS.Singleton
class WebScrapyURLTimeRange(object):

    DEF_STATEMENT_START_YEAR = 2013
    DEF_STATEMENT_START_QUARTER = 1
    DEF_STATEMENT_START_QUARTER_STR = "%dq%d" % (DEF_STATEMENT_START_YEAR, DEF_STATEMENT_START_QUARTER)

    def __init__(self):
        # import pdb; pdb.set_trace()
        self.TIME_RANGE_ROOT_FOLDERPATH = CMN.FUNC.get_config_filepath(CMN.DEF.DEF_TIME_RANGE_FOLDERNAME)
        self.TIME_RANGE_COMPANY_GROUP_FOLDERPATH_FORMAT = ("%s/%s" % (self.TIME_RANGE_ROOT_FOLDERPATH, CMN.DEF.DEF_CSV_STOCK_FOLDERNAME)) + "%02d"
# Caution: 
# Since WebScrapyCompanyProfile/WebScrapyURLTimeRange are the Singleton pattern.
# In the current implementation, One Singleton object can't be instanitate 
# in another Singleton object's instantiation. The singleton_thread_lock variable
# is deadlock in this situation
        # g_profile_lookup = CompanyProfile.WebScrapyCompanyProfile.Instance()
        self.COMPANY_GROUP_SIZE = g_profile_lookup.CompanyGroupSize
        # self.stock_offset_source_type_index_list = [source_type_index - CMN.DEF.DEF_DATA_SOURCE_STOCK_START for source_type_index in self.source_type_index_list]
        self.DEF_DATA_SOURCE_START_SCAN_TIME_CFG = [
            CMN.CLS.FinanceDate(CMN.FUNC.get_year_offset_datetime_cfg(datetime.today(), -1)),
            CMN.CLS.FinanceQuarter(self.DEF_STATEMENT_START_QUARTER_STR),
            CMN.CLS.FinanceQuarter(self.DEF_STATEMENT_START_QUARTER_STR),
            CMN.CLS.FinanceQuarter(self.DEF_STATEMENT_START_QUARTER_STR),
            CMN.CLS.FinanceQuarter(self.DEF_STATEMENT_START_QUARTER_STR),
        ]
        last_url_data_date = CMN.CLS.FinanceDate.get_last_finance_date()
        last_url_data_quarter = CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(last_url_data_date)
        self.DEF_DATA_SOURCE_END_TIME_CFG = [
            last_url_data_date,
            last_url_data_quarter,
            last_url_data_quarter,
            last_url_data_quarter,
            last_url_data_quarter,
        ]
        self.whole_company_group_set = None
        self.timeslice_generator = None
        self.source_type_index_list = None
        self.web_scrapy_class_list = None
        self.company_data_source_start_time_dict = {}
        self.auto_update = True


    def __get_timeslice_generator(self):
        if self.timeslice_generator is None:
            self.timeslice_generator = BASE.TSG.WebScrapyTimeSliceGenerator.Instance()
        return self.timeslice_generator


    def __get_company_group_time_range_folderpath(self, company_group):
        return self.TIME_RANGE_COMPANY_GROUP_FOLDERPATH_FORMAT % company_group


    def __get_company_number_time_range_filepath(self, company_number, company_group=None):
        if company_group is None:
            company_group_unicode = g_profile_lookup.lookup_company_group_number(company_number)
            company_group = int(company_group_unicode)
        return self.__get_company_group_time_range_folderpath(company_group) + "/%s" % company_number


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
            [source_type_index_str, company_time_range_start_str] = line.split(CMN.DEF.DEF_SPACE_DATA_SPLIT)
            source_type_index = int(source_type_index_str)
            if company_time_range_start_ordereddict.has_key(source_type_index):
                raise ValueError("Duplicate source type index: %d" % source_type_index)
            company_time_range_start_ordereddict[source_type_index] = CMN.CLS.FinanceTimeBase.from_time_string(company_time_range_start_str)
        return company_time_range_start_ordereddict


    def __write_company_time_range_start_to_config(self, company_time_range_start_ordereddict, company_number, company_group=None):
        company_number_time_range_filepath = self.__get_company_number_time_range_filepath(company_number, company_group)
        line_list = []
        for source_type_index, company_time_range_start in company_time_range_start_ordereddict.items():
            line = "%d%s%s" % (source_type_index, CMN.DEF.DEF_SPACE_DATA_SPLIT, company_time_range_start.to_string())
            line_list.append(line)
        CMN.FUNC.write_file_lines_ex(line_list, company_number_time_range_filepath, "w")


    def __scan_company_time_range_start(self, company_number, company_group=None):
        # import pdb; pdb.set_trace()
        company_time_range_start_ordereddict = self.__read_company_time_range_start_from_config(company_number, company_group)
        if company_time_range_start_ordereddict is None:
            company_time_range_start_ordereddict = collections.OrderedDict()
        for source_type_index in self.source_type_index_list:
# The start time already exist, no need to search
            if company_time_range_start_ordereddict.has_key(source_type_index):
                continue
            # import pdb;pdb.set_trace()
            stock_source_type_index_offset = source_type_index - CMN.DEF.DEF_DATA_SOURCE_STOCK_START
# Get the web scrapy class
            web_scrapy_class = self.web_scrapy_class_list[stock_source_type_index_offset]
# Define the time range for scanning the start time
            time_slice_generator_cfg = {
                "company_code_number": company_number, 
                "time_duration_start": self.DEF_DATA_SOURCE_START_SCAN_TIME_CFG[stock_source_type_index_offset], 
                "time_duration_end": self.DEF_DATA_SOURCE_END_TIME_CFG[source_type_index - CMN.DEF.DEF_DATA_SOURCE_STOCK_START],
            }
# Generate the time slice
            timeslice_iterable = self.__get_timeslice_generator().generate_time_slice(web_scrapy_class.TIMESLICE_GENERATE_METHOD, **time_slice_generator_cfg)
# Scan the start time
            data_exist = False
            time_duration_start = None
            for timeslice in timeslice_iterable:
                url = web_scrapy_class.assemble_web_url(timeslice, company_number)
                g_logger.debug("Check the data exist from URL: %s" % url)
                try:
# Grab the data from website and assemble the data to the entry of CSV
                    data_exist = web_scrapy_class.check_web_data_exist(url)
                except CMN.EXCEPTION.WebScrapyServerBusyException as e:
                    RETRY_TIMES = 5
                    SLEEP_TIME_BEFORE_RETRY = 15
                    for retry_times in range(1, RETRY_TIMES + 1):
                        if data_exist:
                            break
                        g_logger.warn("Server is busy, let's retry...... %d", retry_times)
                        time.sleep(SLEEP_TIME_BEFORE_RETRY * retry_times)
                        try:
# Grab the data from website and assemble the data to the entry of CSV
                            data_exist = web_scrapy_class.check_web_data_exist(url)
                        except CMN.EXCEPTION.WebScrapyServerBusyException as e:
                            pass
                    if not data_exist:
                        raise CMN.EXCEPTION.WebScrapyServerBusyException("Fail to scrap URL[%s] after retry for %d times" % (url, RETRY_TIMES))
                except Exception as e:
                    # import pdb;pdb.set_trace()
                    if isinstance(e.message, str):
                        g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, e.message))
                    else:
                        g_logger.warn(u"Fail to scrap URL[%s], due to: %s" % (url, e.message))
                if data_exist:
                    company_time_range_start_ordereddict[source_type_index] = timeslice
                    break
            if not data_exist:
                raise CMN.EXCEPTION.WebScrapyNotFoundException("Fail to check if the data[%s:%d] exist" % (company_number, source_type_index))
        self.__write_company_time_range_start_to_config(company_time_range_start_ordereddict, company_number, company_group)
        return company_time_range_start_ordereddict


    def __scan_time_range_start(self):
# Update the time range of time slice
        for company_group, company_number_list in self.whole_company_group_set.items():
            for company_number in company_number_list:
                self.__scan_company_time_range_start(company_number, company_group)


    def initialize(self):
        # import pdb; pdb.set_trace()
        self.whole_company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_number_in_group_dict()
        self.source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
        self.web_scrapy_class_list = []
        for source_type_index in self.source_type_index_list:
            web_scrapy_class = CMN.FUNC.get_web_scrapy_class(source_type_index, False)
            web_scrapy_class.init_class_common_variables()
            self.web_scrapy_class_list.append(web_scrapy_class)
        self.company_listing_date_dict = {}


    def renew_time_range(self, cleanup_old=True):
        if cleanup_old:
            self.__remove_old_time_range_folder()
        self.__create_time_range_folder_if_not_exist()
        self.__scan_time_range_start()


    def __update_company_time_range(self, company_number, company_group):
        company_time_range_start_ordereddict = self.__read_company_time_range_start_from_config(company_number, company_group)
        if company_time_range_start_ordereddict is None:
            if self.auto_update:
                company_time_range_start_ordereddict = self.__scan_company_time_range_start(company_number, company_group)
            else:
                raise CMN.EXCEPTION.WebScrapyNotFoundException("Fail to find the time range in company[%s]" % company_number)
        self.company_data_source_start_time_dict[company_number] = company_time_range_start_ordereddict


    def update_time_range(self):
        if not CMN.FUNC.check_file_exist(self.TIME_RANGE_ROOT_FOLDERPATH):
            raise CMN.EXCEPTION.WebScrapyNotFoundException("The company time range folder[%s] is NOT found" % self.TIME_RANGE_ROOT_FOLDERPATH)
        for company_group, company_number_list in self.whole_company_group_set.items():
            for company_number in company_number_list:
                self.__update_company_time_range(company_number, company_group)


    def get_time_range_start_all_dict(self, company_number, company_group=None):
        if not self.company_data_source_start_time_dict.has_key(company_number):
            self.__update_company_time_range(company_number, company_group)
        return  self.company_data_source_start_time_dict[company_number]


    def get_time_range_start(self, source_type_index, company_number, company_group=None):
        # import pdb;pdb.set_trace()
        CMN.FUNC.check_source_type_index_in_range(source_type_index)
        return self.get_time_range_start_all_dict(company_number, company_group)[source_type_index]


    def get_time_range_end(self, source_type_index):
        CMN.FUNC.check_source_type_index_in_range(source_type_index)
        return self.DEF_DATA_SOURCE_END_TIME_CFG[source_type_index - CMN.DEF.DEF_DATA_SOURCE_STOCK_START]


    def get_time_range(self, source_type_index, company_number, company_group=None):
        return (self.get_time_range_start(source_type_index, company_number, company_group), self.get_time_range_end(source_type_index))


    @property
    def AutoUpdate(self):
        return self.auto_update


    @AutoUpdate.setter
    def AutoUpdate(self, new_auto_update):
        self.auto_update = new_auto_update
