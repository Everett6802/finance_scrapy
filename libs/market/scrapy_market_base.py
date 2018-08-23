# -*- coding: utf8 -*-

import os
import re
import json
import requests
import csv
import time
import collections
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import market_url_time_range as URLTimeRange
g_logger = CMN.LOG.get_logger()


class ScrapyMarketBase(BASE.BASE.ScrapyBase):

    # url_date_range = None
    # TIME_SLICE_GENERATOR = None
    def __init__(self, **kwargs):
        super(ScrapyMarketBase, self).__init__(**kwargs)
        # import pdb; pdb.set_trace()
        # self.web2csv_time_duration_update = None
        self.new_csv_time_duration = None


    def _get_url_time_range(self):
        # import pdb; pdb.set_trace()
        if self.url_time_range is None:
            self.url_time_range = URLTimeRange.MarketURLTimeRange.Instance()
        return self.url_time_range


    def assemble_csv_filepath(self, scrapy_class_index):
        # csv_filepath = "%s/%s/%s.csv" % (self.xcfg["finance_root_folderpath"], CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[scrapy_class_index]) 
        # return csv_filepath
        return CMN.FUNC.assemble_market_csv_filepath(self.xcfg["finance_root_folderpath"], scrapy_class_index)


    def _adjust_time_range_from_csv(self, *args):
        # import pdb; pdb.set_trace()
#         def check_old_csv_time_duration_exist(scrapy_class_index, csv_time_duration_table):
#             if csv_time_duration_table is None:
#                 return False 
#             if csv_time_duration_table.get(scrapy_class_index, None) is None:
#                 return False 
#             return True
#         assert len(args) == 1, "The argument length should be 1, NOT %d" % len(args)
#         time_duration_after_lookup_time = args[0]
# # Determine the CSV/Web time duration
#         web2csv_time_duration_update_tuple = None
#         if not check_old_csv_time_duration_exist(self.SCRAPY_CLASS_INDEX, self.xcfg["csv_time_duration_table"]):
# # The file of the csv time duration does NOT exist. 
#             web2csv_time_duration_update = self._get_init_web2csv_time_duration_update_cfg(
#                 time_duration_after_lookup_time.time_duration_start, 
#                 time_duration_after_lookup_time.time_duration_end
#             )
#             web2csv_time_duration_update_tuple = (web2csv_time_duration_update,)
#         else:
#             # web2csv_time_duration_update_tuple = self._get_extended_web2csv_time_duration_update_cfg(
#             #     self.xcfg["csv_time_duration_table"][self.SCRAPY_CLASS_INDEX], 
#             #     time_duration_after_lookup_time.time_duration_start, 
#             #     time_duration_after_lookup_time.time_duration_end
#             # )
#             self.new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
#                 time_duration_after_lookup_time.time_duration_start, 
#                 time_duration_after_lookup_time.time_duration_end,
#                 self.xcfg["csv_time_duration_table"][self.SCRAPY_CLASS_INDEX]
#             )
#             # if web2csv_time_duration_update_before is not None or web2csv_time_duration_update_after is not None:
#             #     web2csv_time_duration_update_tuple = (web2csv_time_duration_update_before, web2csv_time_duration_update_after)

        def get_old_csv_time_duration_if_exist(scrapy_class_index, csv_time_duration_table):
            if csv_time_duration_table is not None:
                if csv_time_duration_table.get(scrapy_class_index, None) is not None:
                    return csv_time_duration_table[scrapy_class_index]
            return None
        assert len(args) == 1, "The argument length should be 1, NOT %d" % len(args)
        time_duration_after_lookup_time = args[0]

        csv_old_time_duration_tuple = get_old_csv_time_duration_if_exist(self.SCRAPY_CLASS_INDEX, self.xcfg["csv_time_duration_table"])

        self.new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_duration_after_lookup_time.time_duration_start, 
            time_duration_after_lookup_time.time_duration_end,
            csv_old_time_duration_tuple
        )

        if web2csv_time_duration_update_tuple is not None:
            self.new_csv_time_duration = self.new_csv_extension_time_duration
        return web2csv_time_duration_update_tuple


    def _parse_csv_file_status_to_string_list(self):
        # import pdb; pdb.set_trace()
        record_type_dict = self.csv_file_no_scrapy_record.record_type_dict
        # record_type_description_dict = self.csv_file_no_scrapy_record.record_type_description_dict
# Type: "TimeRangeNotOverlap"    
        record_type = BASE.BASE.ScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST[BASE.BASE.ScrapyBase.CSVFileNoScrapyRecord.TIME_RANGE_NOT_OVERLAP_RECORD_INDEX]
        if len(record_type_dict[record_type]) != 0:
# args[0]: source type index
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                self.csv_file_no_scrapy_record_string_dict[record_type].append(CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[0]])
# Type: "CSVFileAlreadyExist"
        record_type = BASE.BASE.ScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST[BASE.BASE.ScrapyBase.CSVFileNoScrapyRecord.CSV_FILE_ALREADY_EXIST_RECORD_INDEX]
        if len(record_type_dict[record_type]) != 0:
# args[0]: source type index
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                self.csv_file_no_scrapy_record_string_dict[record_type].append(CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[0]])
# Type: "WebDataNotFound"
        record_type = BASE.BASE.ScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST[BASE.BASE.ScrapyBase.CSVFileNoScrapyRecord.WEB_DATA_NOT_FOUND_RECORD_INDEX]
        if len(record_type_dict[record_type]) != 0:
# args[0]: time slice
# args[1]: source type index
# args[2]: empty time start
# args[3]: empty time end
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                record_string = "%s:[%s %s]" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[args[1]], args[2].to_string(), args[3].to_string())
                self.csv_file_no_scrapy_record_string_dict[record_type].append(record_string)


    def _calculate_progress_amount(self, **kwargs):
        self.progress_amount = 1


    def scrape_web_to_csv(self):
        # import pdb; pdb.set_trace()
# Limit the searching time range from the web site
        time_duration_after_lookup_time = self._adjust_time_range_from_web(self.SCRAPY_CLASS_INDEX)
        if time_duration_after_lookup_time is None:
            self.csv_file_no_scrapy_record.add_time_range_not_overlap_record(self.SCRAPY_CLASS_INDEX)
            g_logger.debug("[%s] %s => The searching time range is NOT in the time range of web data !!!" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]]))
            return
# Limit the searching time range from the local CSV data
        web2csv_time_duration_update_tuple = self._adjust_time_range_from_csv(time_duration_after_lookup_time)
        if web2csv_time_duration_update_tuple is None:
            self.csv_file_no_scrapy_record.add_csv_file_already_exist_record(self.SCRAPY_CLASS_INDEX)
            g_logger.debug("[%s] %s %s:%s => The CSV data already cover this time range !!!" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], self.xcfg["csv_time_duration_table"][self.SCRAPY_CLASS_INDEX].time_duration_start, self.xcfg["csv_time_duration_table"][self.SCRAPY_CLASS_INDEX].time_duration_end))
            return
# Find the file path for writing data into csv
        csv_filepath = self.assemble_csv_filepath(self.SCRAPY_CLASS_INDEX)
        scrapy_msg = "[%s] %s %s:%s => %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], self.new_csv_extension_time_duration.time_duration_start, self.new_csv_extension_time_duration.time_duration_end, csv_filepath)
        g_logger.debug(scrapy_msg)
# Check if only dry-run
        if self.xcfg["dry_run_only"]:
            print scrapy_msg
            return
        # import pdb; pdb.set_trace()
# Scrape the web data from each time duration
        scrapy_sleep_time_generator = None
        for web2csv_time_duration_update in web2csv_time_duration_update_tuple:
# Adjust the config if necessary
            self._adjust_config_before_scrapy(web2csv_time_duration_update)
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
            web2csv_time_duration_update.backup_old_csv_if_necessary(csv_filepath)
# Create the time slice iterator due to correct time range
            csv_data_list_each_year = []
            cur_year = None
            time_slice_generator_cfg = {"time_duration_start": web2csv_time_duration_update.NewWebStart, "time_duration_end": web2csv_time_duration_update.NewWebEnd,}
            timeslice_iterable = self._get_timeslice_iterable(**time_slice_generator_cfg)
            for timeslice in timeslice_iterable:
                csv_data_list = None
# Write the data into csv year by year
                if timeslice.year != cur_year:
                    if len(csv_data_list_each_year) > 0:
                        self._write_to_csv(csv_filepath, csv_data_list_each_year)
                        csv_data_list_each_year = []
                    cur_year = timeslice.year
                try:
                    web_data = self._scrape_web_data(timeslice)
                    if len(web_data) == 0:
# Keep track of the time range in which the web data is empty
                        self.csv_file_no_scrapy_record.add_web_data_not_found_record(timeslice, self.SCRAPY_CLASS_INDEX)
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
                    # import pdb; pdb.set_trace()
                    if scrapy_sleep_time_generator is None:
                        scrapy_sleep_time_generator = CMN.FUNC.get_scrapy_sleep_time(self.SCRAPY_NEED_LONG_SLEEP)
                    scrapy_sleep_time = scrapy_sleep_time_generator.next()
                    if scrapy_sleep_time != 0:
                        if scrapy_sleep_time > 30:
                            g_logger.info("Sleep %d seconds before next scrapy")
                        time.sleep(scrapy_sleep_time)
# Flush the last data into the list if required
            self.csv_file_no_scrapy_record.add_web_data_not_found_record(None, self.SCRAPY_CLASS_INDEX)
# Write the data of last year into csv
            if len(csv_data_list_each_year) > 0:
                self._write_to_csv(csv_filepath, csv_data_list_each_year)
# Append the old CSV data after the new web data if necessary
            web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath)
# Increase the progress count
        self.progress_count = 1
# parse csv file status
        self._parse_csv_file_status_to_string_list()


    def get_new_csv_time_duration(self):
# No matter the csv time range would be updated, the new time duration is required to re-write into the config file
        assert self.new_csv_time_duration is not None, "self.new_csv_time_duration should NOT be None"
        return self.new_csv_time_duration


    @classmethod
    def _get_date_not_whole_month_list(self, date_duration_start, date_duration_end):
        data_not_whole_month_list = []
        if CMN.CLS.FinanceDate.is_same_month(date_duration_start, date_duration_end):
            if date_duration_start.day > 1 or date_duration_end.day < CMN.FUNC.get_month_last_day(date_duration_end.year, date_duration_end.month):
                data_not_whole_month_list.append(CMN.CLS.FinanceMonth(date_duration_end.year, date_duration_end.month))
        else:
            if date_duration_start.day > 1:
                data_not_whole_month_list.append(CMN.CLS.FinanceMonth(date_duration_start.year, date_duration_start.month))
            if date_duration_end.day < CMN.FUNC.get_month_last_day(date_duration_end.year, date_duration_end.month):
                data_not_whole_month_list.append(CMN.CLS.FinanceMonth(date_duration_end.year, date_duration_end.month))
        return data_not_whole_month_list


    @classmethod
    def assemble_web_url(cls, timeslice, *args):
# CAUTION: This function MUST be called by the LEAF derived class
        raise NotImplementedError


    def _scrape_web_data(self, timeslice):
        raise NotImplementedError
