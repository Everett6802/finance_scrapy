# -*- coding: utf8 -*-

import os
import re
import json
import requests
import csv
import time
import copy
import collections
from abc import ABCMeta, abstractmethod
# from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
# import CMN.CLS.CSVTimeRangeUpdate as CSVTimeRangeUpdate
import stock_url_time_range as URLTimeRange
g_logger = CMN.LOG.get_logger()


class ScrapyStockBase(BASE.BASE.ScrapyBase):

    __metaclass__ = ABCMeta
    company_profile = None

    @classmethod
    def _get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = BASE.CP.CompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def check_company_first_data_exist(cls, company_code_number):
        first_data_time = cls._get_company_profile().lookup_company_first_data_date(company_code_number, cls.URL_TIME_UNIT)
        return True if first_data_time is not None else False


    def __init__(self, **kwargs):
        super(ScrapyStockBase, self).__init__(**kwargs)
        # import pdb; pdb.set_trace()
        # self.is_whole_company_group_set = False
# CAUTION: The company group set should be determined outside the class, including
# the market type, and the companies belong to the specific market type
        self.company_group_set = kwargs.get("company_group_set", None)
        if self.company_group_set is None:
            raise ValueError("The company_group_set should NOT be None")
        # if kwargs.get("company_group_set", None) is None:
        #     # self.is_whole_company_group_set = True
        #     self.company_group_set = CompanyGroupSet.CompanyGroupSet.get_whole_company_group_set(self.COMPANY_MARKET_TYPE)
        # else:
        #     self.company_group_set = kwargs["company_group_set"]
        # self.time_slice_kwargs["company_code_number"] = None
        self.new_csv_time_duration_dict = None
        # self.scrapy_company_progress_count = 0
        self.company_profile = None
        self.cur_company_code_number = None # Caution: This value is updated every time when _scrape_web_data() is called
        self.auto_scan_start_time = False


    def _get_url_time_range(self):
        if self.url_time_range is None:
            self.url_time_range = URLTimeRange.StockURLTimeRange.Instance()
        return self.url_time_range


    def assemble_csv_company_folderpath(self, company_code_number, company_group_number=-1):
        if company_group_number == -1:
            company_group_number = self._get_company_profile().lookup_company_group_number(company_code_number)
        csv_company_folderpath = "%s/%s%02d/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.CSV_STOCK_FOLDERNAME, int(company_group_number), company_code_number) 
        return csv_company_folderpath


    def assemble_csv_filepath(self, scrapy_class_index, company_code_number, company_group_number=-1):
        if company_group_number == -1:
            company_group_number = self._get_company_profile().lookup_company_group_number(company_code_number)
        # csv_filepath = "%s/%s%02d/%s/%s.csv" % (self.xcfg["finance_root_folderpath"], CMN.DEF.CSV_STOCK_FOLDERNAME, int(company_group_number), company_code_number, CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[scrapy_class_index]) 
        # return csv_filepath
        return CMN.FUNC.assemble_stock_csv_filepath(self.xcfg["finance_root_folderpath"], scrapy_class_index, company_code_number, company_group_number)


    def _adjust_time_range_from_csv(self, *args):
        # import pdb; pdb.set_trace()
#         def check_old_csv_time_duration_exist(scrapy_class_index, company_code_number, csv_time_duration_table):
#             if csv_time_duration_table is None:
#                 return False 
#             company_csv_time_duration_table = csv_time_duration_table.get(company_code_number, None)
#             if company_csv_time_duration_table is None:
#                 return False
#             if company_csv_time_duration_table.get(scrapy_class_index, None) is None:
#                 return False
#             return True
#         assert len(args) == 2, "The argument length should be 2, NOT %d" % len(args)
#         time_duration_after_lookup_time = args[0]
#         company_code_number = args[1]
# # Determine the CSV/Web time duration
#         web2csv_time_duration_update_tuple = None
#         if not check_old_csv_time_duration_exist(self.SCRAPY_CLASS_INDEX, company_code_number, self.xcfg["csv_time_duration_table"]):
#             web2csv_time_duration_update = self._get_init_web2csv_time_duration_update_cfg(
#                 time_duration_after_lookup_time.time_duration_start, 
#                 time_duration_after_lookup_time.time_duration_end
#             )
#             web2csv_time_duration_update_tuple = (web2csv_time_duration_update,)
#         else:
#             # web2csv_time_duration_update_tuple = self._get_extended_web2csv_time_duration_update_cfg(
#             #     self.xcfg["csv_time_duration_table"][company_code_number][self.SCRAPY_CLASS_INDEX], 
#             #     time_duration_after_lookup_time.time_duration_start, 
#             #     time_duration_after_lookup_time.time_duration_end
#             # )
#             self.new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
#                 time_duration_after_lookup_time.time_duration_start, 
#                 time_duration_after_lookup_time.time_duration_end,
#                 self.xcfg["csv_time_duration_table"][company_code_number][self.SCRAPY_CLASS_INDEX]
#             )
#             # if web2csv_time_duration_update_before is not None or web2csv_time_duration_update_after is not None:
#             #     web2csv_time_duration_update_tuple = (web2csv_time_duration_update_before, web2csv_time_duration_update_after)

        def get_old_csv_time_duration_if_exist(scrapy_class_index, company_code_number, csv_time_duration_table):
            if csv_time_duration_table is not None:
                company_csv_time_duration_table = csv_time_duration_table.get(company_code_number, None)
                if company_csv_time_duration_table is not None:
                    if company_csv_time_duration_table.get(scrapy_class_index, None) is not None:
                        return company_csv_time_duration_table[scrapy_class_index]
            return None
        assert len(args) == 2, "The argument length should be 2, NOT %d" % len(args)
        time_duration_after_lookup_time = args[0]
        company_code_number = args[1]

        csv_old_time_duration_tuple = get_old_csv_time_duration_if_exist(self.SCRAPY_CLASS_INDEX, company_code_number, self.xcfg["csv_time_duration_table"])

        self.new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_duration_after_lookup_time.time_duration_start, 
            time_duration_after_lookup_time.time_duration_end,
            csv_old_time_duration_tuple
        )

        if web2csv_time_duration_update_tuple is not None:
            self.new_csv_time_duration_dict[company_code_number] = self.new_csv_extension_time_duration
        return web2csv_time_duration_update_tuple


    def _parse_csv_file_status_to_string_list(self):
        # import pdb; pdb.set_trace()
        record_type_dict = self.csv_file_no_scrapy_record.record_type_dict
        # record_type_description_dict = self.csv_file_no_scrapy_record.record_type_description_dict
# Type: "TimeRangeNotOverlap"    
        record_type = self.CSVFileNoScrapyTypeList[self.CSVFileNoScrapyTimeRangeNotOverlapRecordIndex]
        if len(record_type_dict[record_type]) != 0:
# args[0]: source type index
# args[1]: company code number
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                record_string = "%s:%s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[0]], args[1])
                self.csv_file_no_scrapy_record_string_dict[record_type].append(record_string)
# Type: "CSVFileAlreadyExist"
        record_type = self.CSVFileNoScrapyTypeList[self.CSVFileNoScrapyCSVFileAlreadyExistRecordIndex]
        if len(record_type_dict[record_type]) != 0:
# args[0]: source type index
# args[1]: company code number
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                record_string = "%s:%s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[0]], args[1])
                self.csv_file_no_scrapy_record_string_dict[record_type].append(record_string)
# Type: "WebDataNotFound"
        record_type = self.CSVFileNoScrapyTypeList[self.CSVFileNoScrapyWebDataNotFoundRecordIndex]
        if len(record_type_dict[record_type]) != 0:
# args[0]: time slice
# args[1]: source type index
# args[2]: company code number
# args[3]: empty time start
# args[4]: empty time end
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                record_string = None
                if self.SCRAPY_CLASS_INDEX in CMN.DEF.TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_SCRAPY_CLASS_INDEX_LIST:
                    record_string = "%s:[%s %s]" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[1]], args[3].to_string(), args[4].to_string())
                else:
                    record_string = "%s:%d:[%s %s]" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[1]], args[2], args[3].to_string(), args[4].to_string())
                self.csv_file_no_scrapy_record_string_dict[record_type].append(record_string)


    def _calculate_progress_amount(self, **kwargs):
        self.progress_amount = self.company_group_set.CompanyAmount


    def _need_filter_scrape_company(self, company_code_number):
        pass


    def scrape_web_to_csv(self):
        # import pdb; pdb.set_trace()
        self._calculate_progress_amount()
        self.new_csv_time_duration_dict = {}
        scrapy_sleep_time_generator = None
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
#  Filter the company which should NOT be scraped in certain a scrapy class
                if self._need_filter_scrape_company(company_code_number):
                    g_logger.debug("[%s:%s] is filtered......" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[self.SCRAPY_CLASS_INDEX], company_code_number))
                    continue
                # import pdb; pdb.set_trace()
# We assume the market type of all the companies in the Company Group Set are correct before trasverse
                # if self.COMPANY_MARKET_TYPE != CMN.DEF.MARKET_TYPE_NONE and (not self.is_whole_company_group_set):
                #     if not CompanyGroupSet.CompanyGroupSet.check_company_market_type(company_code_number, self.COMPANY_MARKET_TYPE):
                #         continue
# Limit the searching time range from the web site
                company_code_number_for_auto_scan = company_code_number if self.auto_scan_start_time else None
                time_duration_after_lookup_time = self._adjust_time_range_from_web(self.SCRAPY_CLASS_INDEX, company_code_number_for_auto_scan)
                if time_duration_after_lookup_time is None:
                    self.csv_file_no_scrapy_record.add_time_range_not_overlap_record(self.SCRAPY_CLASS_INDEX, company_code_number)
                    g_logger.debug("[%s:%s] %s => The searching time range is NOT in the time range of web data !!!" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[self.SCRAPY_CLASS_INDEX], company_code_number, CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]]))
                    continue
# Limit the searching time range from the local CSV data
                web2csv_time_duration_update_tuple = self._adjust_time_range_from_csv(time_duration_after_lookup_time, company_code_number)
                if web2csv_time_duration_update_tuple is None:
                    self.csv_file_no_scrapy_record.add_csv_file_already_exist_record(self.SCRAPY_CLASS_INDEX, company_code_number)
                    g_logger.debug("[%s:%s] %s %s:%s => The CSV data already cover this time range !!!" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[self.SCRAPY_CLASS_INDEX], company_code_number, CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], self.xcfg["csv_time_duration_table"][company_code_number][self.SCRAPY_CLASS_INDEX].time_duration_start, self.xcfg["csv_time_duration_table"][company_code_number][self.SCRAPY_CLASS_INDEX].time_duration_end))
                    continue
# Create a folder for a specific company
                csv_company_folderpath = self.assemble_csv_company_folderpath(company_code_number, company_group_number)
                CMN.FUNC.create_folder_if_not_exist(csv_company_folderpath)
# Find the file path for writing data into csv
                csv_filepath = self.assemble_csv_filepath(self.SCRAPY_CLASS_INDEX, company_code_number, company_group_number)
                scrapy_msg = "[%s:%s] %s %s:%s => %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[self.SCRAPY_CLASS_INDEX], company_code_number, CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], self.new_csv_extension_time_duration.time_duration_start, self.new_csv_extension_time_duration.time_duration_end, csv_filepath)
                g_logger.debug(scrapy_msg)
# Check if only dry-run
                if self.xcfg["dry_run_only"]:
                    print scrapy_msg
                    continue
# Scrape the web data from each time duration
                for web2csv_time_duration_update in web2csv_time_duration_update_tuple: 
# Adjust the config if necessary
                    self._adjust_config_before_scrapy(web2csv_time_duration_update)
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
                    web2csv_time_duration_update.backup_old_csv_if_necessary(csv_filepath)
# Create the time slice iterator due to correct time range
                    # import pdb; pdb.set_trace()
                    csv_data_list_each_year = []
                    cur_year = None
                    timeslice_generator_cfg = {"company_code_number": company_code_number, "time_duration_start": web2csv_time_duration_update.NewWebStart, "time_duration_end": web2csv_time_duration_update.NewWebEnd,}
                    timeslice_iterable = self._get_timeslice_iterable(**timeslice_generator_cfg)
                    for timeslice in timeslice_iterable:
# Write the data into csv year by year
                        if timeslice.year != cur_year:
                            if len(csv_data_list_each_year) > 0:
                                # import pdb; pdb.set_trace()
                                self._write_to_csv(csv_filepath, csv_data_list_each_year)
                                csv_data_list_each_year = []
                            cur_year = timeslice.year
                        try:
                            web_data = self._scrape_web_data(timeslice, company_code_number)
                            if len(web_data) == 0:
# Keep track of the time range in which the web data is empty
                                self.csv_file_no_scrapy_record.add_web_data_not_found_record(timeslice, self.SCRAPY_CLASS_INDEX, company_code_number)
                            else:
                                csv_data_list = self._parse_web_data(web_data)
                                if len(csv_data_list) == 0:
                                    raise CMN.EXCEPTION.WebScrapyNotFoundException("No entry in the web data from URL: %s" % url)
                                csv_data_list_each_year.append(csv_data_list)
                        except Exception as err:
                            if self.xcfg["disable_flush_scrapy_while_exception"]:
                                if len(csv_data_list_each_year) > 0:
                                    g_logger.debug("Flush the web data into CSV since exception occurs......")
                                    self._write_to_csv(csv_filepath, csv_data_list_each_year)                        
                            raise err
                        else:
# Slow down the scrapy process
                            if scrapy_sleep_time_generator is None:
                                scrapy_sleep_time_generator = CMN.FUNC.get_scrapy_sleep_time(self.SCRAPY_NEED_LONG_SLEEP)
                            scrapy_sleep_time = scrapy_sleep_time_generator.next()
                            if scrapy_sleep_time != 0:
                                if scrapy_sleep_time > 30:
                                    g_logger.info("Sleep %d seconds before next scrapy")
                                time.sleep(scrapy_sleep_time)
# Flush the last data into the list if required
                    self.csv_file_no_scrapy_record.add_web_data_not_found_record(None, self.SCRAPY_CLASS_INDEX, company_code_number)
# Write the data of last year into csv
                    if len(csv_data_list_each_year) > 0:
                        # import pdb; pdb.set_trace()
                        self._write_to_csv(csv_filepath, csv_data_list_each_year)
# Append the old CSV data after the new web data if necessary
                    web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath)
# Increase the progress count
                self.progress_count += 1
# Parse csv file status
        self._parse_csv_file_status_to_string_list()


    def get_new_csv_time_duration_dict(self):
# No matter the csv time range would be updated, the new time duration is required to re-write into the config file
        assert self.new_csv_time_duration_dict is not None, "self.new_csv_time_duration_dict should NOT be None"
        return self.new_csv_time_duration_dict


    # @property
    # def CompanyProgressCount(self):
    #     return self.scrapy_company_progress_count


    @property
    def AutoScanStartTime(self):
        return self.auto_scan_start_time


    @AutoScanStartTime.setter
    def AutoScanStartTime(self, new_auto_scan_start_time):
        self.auto_scan_start_time = new_auto_scan_start_time


    @abstractmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
# CAUTION: This function MUST be called by the LEAF derived class
        raise NotImplementedError


    @abstractmethod
    def _scrape_web_data(self, timeslice, company_code_number):
        raise NotImplementedError
