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


    def _check_old_csv_time_duration_exist(self, *args):
        if self.xcfg["csv_time_duration_table"] is None:
            return False 
        if self.xcfg["csv_time_duration_table"].get(self.source_type_index, None) is None:
            return False 
        return True


    def _adjust_csv_time_duration(self):
        # import pdb; pdb.set_trace()
# Limit the time range from the web site
        time_duration_after_lookup_time = (self._adjust_time_duration_start_and_end_time_func_ptr(self.xcfg["time_duration_type"]))(self.source_type_index)
# Determine the CSV/Web time duration
        web2csv_time_duration_update = None
        if self._check_old_csv_time_duration_exist():
            web2csv_time_duration_update = self._get_overlapped_web2csv_time_duration_update_cfg(
                self.xcfg["csv_time_duration_table"][self.source_type_index], 
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


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
# Find the file path for writing data into csv
        csv_filepath = self.assemble_csv_filepath(self.source_type_index)
# Determine the actual time range
        web2csv_time_duration_update = self._adjust_csv_time_duration()
        if not web2csv_time_duration_update.NeedUpdate:
            g_logger.debug("[%s] %s %s:%s => The CSV data already cover this time range !!!" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewCSVStart, web2csv_time_duration_update.NewCSVEnd))
            return
        scrapy_msg = "[%s] %s %s:%s => %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
        g_logger.debug(scrapy_msg)
# Check if only dry-run
        if self.xcfg["dry_run_only"]:
            print scrapy_msg
            return
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
        if web2csv_time_duration_update.AppendDirection == BASE.BASE.WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
            g_logger.debug("Need add the new data in front of the old CSV data, rename the file: %s" % (csv_filepath + ".old"))
            CMN.FUNC.rename_file_if_exist(csv_filepath, csv_filepath + ".old")
# Update the time range of time slice
        time_slice_generator_cfg = {"time_duration_start": web2csv_time_duration_update.NewWebStart, "time_duration_end": web2csv_time_duration_update.NewWebEnd,}
# Generate the time slice
        timeslice_iterable = self._get_timeslice_iterable(**time_slice_generator_cfg)
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
# Append the old CSV data after the new web data if necessary
        if web2csv_time_duration_update.AppendDirection == BASE.BASE.WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
            g_logger.debug("Append the old CSV data to the file: %s" % csv_filepath)
            CMN.FUNC.append_data_into_file(csv_filepath + ".old", csv_filepath)
            CMN.FUNC.remove_file_if_exist(csv_filepath + ".old") 


    def get_new_csv_time_duration(self):
# No matter the csv time range would be updated, the new time duration is required to re-write into the config file
        assert self.new_csv_time_duration is not None, "self.new_csv_time_duration should NOT be None"
        return self.new_csv_time_duration


    def assemble_web_url(self, timeslice):
        raise NotImplementedError
