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
import web_scrapy_url_time_range as URLTimeRange
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyMarketBase(BASE.BASE.WebScrapyBase):

    # url_date_range = None
    # TIME_SLICE_GENERATOR = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyMarketBase, self).__init__(cur_file_path, **kwargs)
        # import pdb; pdb.set_trace()
        # self.web2csv_time_duration_update = None
        self.new_csv_time_duration = None


    def _get_url_time_range(self):
        # import pdb; pdb.set_trace()
        if self.url_time_range is None:
            self.url_time_range = URLTimeRange.WebScrapyURLTimeRange.Instance()
        return self.url_time_range


    def assemble_csv_filepath(self, source_type_index):
        # csv_filepath = "%s/%s/%s.csv" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_MARKET_FOLDERNAME, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[source_type_index]) 
        # return csv_filepath
        return CMN.FUNC.assemble_market_csv_filepath(self.xcfg["finance_root_folderpath"], source_type_index)


    def _adjust_time_range_from_csv(self, *args):
        # import pdb; pdb.set_trace()
        def check_old_csv_time_duration_exist(source_type_index, csv_time_duration_table):
            if csv_time_duration_table is None:
                return False 
            if csv_time_duration_table.get(source_type_index, None) is None:
                return False 
            return True
        assert len(args) == 1, "The argument length should be 1, NOT %d" % len(args)
        time_duration_after_lookup_time = args[0]
# Determine the CSV/Web time duration
        web2csv_time_duration_update = None
        if check_old_csv_time_duration_exist(self.SOURCE_TYPE_INDEX, self.xcfg["csv_time_duration_table"]):
            web2csv_time_duration_update = self._get_overlapped_web2csv_time_duration_update_cfg(
                self.xcfg["csv_time_duration_table"][self.SOURCE_TYPE_INDEX], 
                time_duration_after_lookup_time.time_duration_start, 
                time_duration_after_lookup_time.time_duration_end
            )
        else:
            web2csv_time_duration_update = self._get_non_overlapped_web2csv_time_duration_update_cfg(
                time_duration_after_lookup_time.time_duration_start, 
                time_duration_after_lookup_time.time_duration_end
            )
        if web2csv_time_duration_update.NeedUpdate:
            self.new_csv_time_duration = CMN.CLS.TimeDurationTuple(web2csv_time_duration_update.NewCSVStart, web2csv_time_duration_update.NewCSVEnd)
        return web2csv_time_duration_update


    def _parse_csv_file_status_to_string_list(self):
        record_type_dict = self.csv_file_no_scrapy_record.record_type_dict
        # record_type_description_dict = self.csv_file_no_scrapy_record.record_type_description_dict
# Type: "TimeRangeNotOverlap"    
        record_type = WebScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST[WebScrapyBase.CSVFileNoScrapyRecord.TIME_RANGE_NOT_OVERLAP_RECORD_INDEX]
        if len(record_type_dict[record_type]) != 0:
# args[0]: source type index
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                self.csv_file_no_scrapy_record_string_dict[record_type].append(CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[args[0]])
# Type: "CSVFileAlreadyExist"
        record_type = WebScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST[WebScrapyBase.CSVFileNoScrapyRecord.CSV_FILE_ALREADY_EXIST_RECORD_INDEX]
        if len(record_type_dict[record_type]) != 0:
# args[0]: source type index
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                self.csv_file_no_scrapy_record_string_dict[record_type].append(CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[args[0]])
# Type: "WebDataNotFound"
        record_type = WebScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST[WebScrapyBase.CSVFileNoScrapyRecord.WEB_DATA_NOT_FOUND_RECORD_INDEX]
        if len(record_type_dict[record_type]) != 0:
# args[0]: time slice
# args[1]: source type index
# args[2]: empty time start
# args[3]: empty time end
            self.csv_file_no_scrapy_record_string_dict[record_type] = []
            for args in record_type_dict[record_type]:
                record_string = "%s:%s-%s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[args[1]], args[2].to_string(), args[3].to_string())
                self.csv_file_no_scrapy_record_string_dict[record_type].append(record_string)


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
# Limit the searching time range from the web site
        time_duration_after_lookup_time = self._adjust_time_range_from_web(self.SOURCE_TYPE_INDEX)
        if time_duration_after_lookup_time is None:
            self.csv_file_no_scrapy_record.add_time_range_not_overlap_record(self.SOURCE_TYPE_INDEX)
            g_logger.debug("[%s:%s] %s => The searching time range is NOT in the time range of web data !!!" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX], company_code_number, CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]]))
            return
# Limit the searching time range from the local CSV data
        web2csv_time_duration_update = self._adjust_time_range_from_csv(time_duration_after_lookup_time)
        if not web2csv_time_duration_update.NeedUpdate:
            self.csv_file_no_scrapy_record.add_csv_file_already_exist_record(self.SOURCE_TYPE_INDEX)
            g_logger.debug("[%s] %s %s:%s => The CSV data already cover this time range !!!" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX], CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewCSVStart, web2csv_time_duration_update.NewCSVEnd))
            return
        scrapy_msg = "[%s] %s %s:%s => %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX], CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
        g_logger.debug(scrapy_msg)
# Check if only dry-run
        if self.xcfg["dry_run_only"]:
            print scrapy_msg
            return
# Find the file path for writing data into csv
        csv_filepath = self.assemble_csv_filepath(self.SOURCE_TYPE_INDEX)
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
                    self._write_to_csv(csv_filepath, csv_data_list_each_year, self.SOURCE_URL_PARSING_CFG["url_multi_data_one_page"])
                    csv_data_list_each_year = []
                cur_year = timeslice.year
            url = self.prepare_for_scrapy(timeslice)
            # import pdb;pdb.set_trace()
            web_data = self.try_get_web_data(url)
            if len(web_data) == 0:
# Keep track of the time range in which the web data is empty
                self.csv_file_no_scrapy_record.add_web_data_not_found_record(timeslice, self.SOURCE_TYPE_INDEX)
            else:
                csv_data_list = self._parse_web_data(web_data)
                if len(csv_data_list) == 0:
                    raise CMN.EXCEPTION.WebScrapyNotFoundException("No entry in the web data from URL: %s" % url)
                csv_data_list_each_year.append(csv_data_list)
# Flush the last data into the list if required
            self.csv_file_no_scrapy_record.add_web_data_not_found_record(None, self.SOURCE_TYPE_INDEX)
# Write the data of last year into csv
        if len(csv_data_list_each_year) > 0:
            self._write_to_csv(csv_filepath, csv_data_list_each_year, self.SOURCE_URL_PARSING_CFG["url_multi_data_one_page"])
# Append the old CSV data after the new web data if necessary
        web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath)
# parse csv file status
        self._parse_csv_file_status_to_string_list()


    def get_new_csv_time_duration(self):
# No matter the csv time range would be updated, the new time duration is required to re-write into the config file
        assert self.new_csv_time_duration is not None, "self.new_csv_time_duration should NOT be None"
        return self.new_csv_time_duration


    @classmethod
    def assemble_web_url(cls, timeslice, *args):
# CAUTION: This function MUST be called by the LEAF derived class
        raise NotImplementedError


    def prepare_for_scrapy(self, timeslice):
        raise NotImplementedError
