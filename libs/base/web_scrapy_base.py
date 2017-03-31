# -*- coding: utf8 -*-

import os
import re
import json
import requests
import csv
import time
import collections
import sys
from abc import ABCMeta, abstractmethod
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_timeslice_generator as TimeSliceGenerator
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyBase(object):

    __metaclass__ = ABCMeta
    class Web2CSVTimeRangeUpdate(object):
        WEB2CSV_APPEND_NONE = 0 # No new web data to append
        WEB2CSV_APPEND_FRONT = 1 #  new web data will be appended in front of the old csv data
        WEB2CSV_APPEND_BACK = 2 #  new web data will be appended in back of the old csv data
        def __init__(self):
            self.append_direction = self.WEB2CSV_APPEND_NONE
            self.old_csv_start = None
            self.old_csv_end = None
            self.new_web_start = None
            self.new_web_end = None
            self.new_csv_start = None
            self.new_csv_end = None
            self.description = None

        def __str__(self):
            if self.description is None:
                self.description = ""
                if self.old_csv_start is not None:
                    self.description += "OCS: %s; " % self.old_csv_start
                if self.old_csv_end is not None:
                    self.description += "OCE: %s; " % self.old_csv_end
                if self.new_web_start is not None:
                    self.description += "NWS: %s; " % self.new_web_start
                if self.new_web_end is not None:
                    self.description += "NWE: %s; " % self.new_web_end
                if self.new_csv_start is not None:
                    self.description += "NCS: %s; " % self.new_csv_start
                if self.new_csv_end is not None:
                    self.description += "NCE: %s; " % self.new_csv_end
            return self.description


        def __repr__(self):
            return self.__str__()


        @property
        def NeedUpdate(self):
            return (True if (self.append_direction != self.WEB2CSV_APPEND_NONE) else False)

        @property
        def AppendDirection(self):
            return self.append_direction
        @AppendDirection.setter
        def AppendDirection(self, append_direction):
            self.append_direction = append_direction

        @property
        def OldCSVStart(self):
            return self.old_csv_start
        @OldCSVStart.setter
        def OldCSVStart(self, old_csv_start):
            self.old_csv_start = old_csv_start

        @property
        def OldCSVEnd(self):
            return self.old_csv_end
        @OldCSVEnd.setter
        def OldCSVEnd(self, old_csv_end):
            self.old_csv_end = old_csv_end

        @property
        def NewWebStart(self):
            return self.new_web_start
        @NewWebStart.setter
        def NewWebStart(self, new_web_start):
            self.new_web_start = new_web_start

        @property
        def NewWebEnd(self):
            return self.new_web_end
        @NewWebEnd.setter
        def NewWebEnd(self, new_web_end):
            self.new_web_end = new_web_end

        @property
        def NewCSVStart(self):
            return self.new_csv_start
        @NewCSVStart.setter
        def NewCSVStart(self, new_csv_start):
            self.new_csv_start = new_csv_start

        @property
        def NewCSVEnd(self):
            return self.new_csv_end
        @NewCSVEnd.setter
        def NewCSVEnd(self, new_csv_end):
            self.new_csv_end = new_csv_end

# Can't be declared in class due to thread-safe
    # PARSE_URL_DATA_FUNC_PTR = None
    # GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR = None
    @classmethod
    def init_class_variables(cls):
        pass


    def __init__(self, cur_file_path, **kwargs):
        self.xcfg = {
            "time_duration_type": CMN.DEF.DATA_TIME_DURATION_TODAY,
            "time_duration_start": None,
            "time_duration_end": None,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.DEF_CSV_ROOT_FOLDERPATH,
            "csv_time_duration_table": None,
            # "multi_thread": False,
        }
        # import pdb; pdb.set_trace()
        self.xcfg.update(kwargs)
# Find which module is instansiate
        cur_module_name = re.sub(CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX, "", CMN.FUNC.get_cur_module_name(cur_file_path))
# Find correspnding index of the module
        self.source_type_index = CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index(cur_module_name)
# Find corresponding config of the module
        self.source_url_parsing_cfg = CMN.DEF.DEF_SOURCE_URL_PARSING[self.source_type_index]

        self.PARSE_URL_DATA_FUNC_PTR = None
        self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR = None

        self.timeslice_generate_method = self.source_url_parsing_cfg["url_timeslice"]
        self.url_format = self.source_url_parsing_cfg["url_format"]
        self.url_parsing_method = self.source_url_parsing_cfg["url_parsing_method"]
        # csv_time_unit = CMN.DEF.DEF_CSV_TIME_UNIT[self.source_type_index]
        self.url_time_unit = CMN.DEF.TIMESLICE_TO_TIME_UNIT_MAPPING[self.timeslice_generate_method]
        # self.scrap_web_to_csv_func_ptr = self.__scrap_multiple_web_data_to_single_csv_file if url_time_unit == csv_time_unit self.__scrap_single_web_data_to_single_csv_file
        # self.timeslice_iterable = timeslice_generator_obj.generate_time_slice(self.timeslice_generate_method, **kwargs)
        # self.time_slice_kwargs = {"time_duration_start": None, "time_duration_end": None}
        self.description = None
        self.timeslice_generator = None
        self.url_time_range = None


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


    @property
    def SourceTypeIndex(self):
        return self.source_type_index


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


    def _get_time_slice_generator(self):
        if self.timeslice_generator is None:
            self.timeslice_generator = TimeSliceGenerator.WebScrapyTimeSliceGenerator.Instance()
        return self.timeslice_generator


    def __get_parse_url_data_func_ptr(self):
        if self.PARSE_URL_DATA_FUNC_PTR is None:
            self.PARSE_URL_DATA_FUNC_PTR = [
                self.__select_web_data_by_bs4, 
                self.__select_web_data_by_json
            ]
        return self.PARSE_URL_DATA_FUNC_PTR


    def _get_web_data(self, url):
        req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
        # parse_url_data_type = self.parse_url_data_type_obj.get_type()
        # return (self.PARSE_URL_DATA_FUNC_PTR[parse_url_data_type])(res)
        # import pdb; pdb.set_trace()
        return (self.__get_parse_url_data_func_ptr()[self.url_parsing_method])(req, self.source_url_parsing_cfg)


    def _get_overlapped_web2csv_time_duration_update_cfg(self, csv_old_time_duration_tuple, time_duration_start, time_duration_end):
        # import pdb; pdb.set_trace()
        is_overlap = False
        web2csv_time_duration_update = WebScrapyBase.Web2CSVTimeRangeUpdate()
# Adjust the time duration, ignore the data which already exist in the finance data folder
# I assume that the time duration between the csv data and new data should be consecutive
        # if csv_old_time_duration_tuple.source_type_index != self.source_type_index:
        #     raise ValueError("The source type index is NOT identical: %d %d", (csv_old_time_duration_tuple.source_type_index, self.source_type_index))
        is_overlap = CMN.FUNC.is_time_range_overlap(time_duration_start, time_duration_end, csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end)
        if is_overlap:
            is_time_duration_start_in_range = CMN.FUNC.is_time_in_range(csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start) 
            is_time_duration_end_in_range = CMN.FUNC.is_time_in_range(csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_end) 
            if is_time_duration_start_in_range and is_time_duration_end_in_range:
# All csv data already exists, no need to update the new data
                g_logger.debug("The time duration[%s:%s] of the CSV data[%s] already exist ......" % (time_duration_start, time_duration_end, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index]))
                return web2csv_time_duration_update
            elif is_time_duration_start_in_range:
# I just assume the new time range can be only extended from the start of end side of the original time range
                if time_duration_end < csv_old_time_duration_tuple.time_duration_end:
                    raise ValueError("The system does NOT support this type[0] of the range update; CSV data[%s:%s], new data[%s:%s]" % (csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
                time_duration_start = csv_old_time_duration_tuple.time_duration_end + 1
                web2csv_time_duration_update.AppendDirection = WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_BACK
            elif is_time_duration_end_in_range:
# I just assume the new time range can be only extended from the start of end side of the original time range
                if time_duration_start > csv_old_time_duration_tuple.time_duration_start:
                    raise ValueError("The system does NOT support this type[1] of the range update; CSV data[%s:%s], new data[%s:%s]" % (csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
                time_duration_end = csv_old_time_duration_tuple.time_duration_start - 1
                web2csv_time_duration_update.AppendDirection = WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT
            else:
# If the time range of new data contain all the time range of csv data, the system is not desiged to update two time range interval
                raise ValueError("The system does NOT support this type[2] of the range update; CSV data[%s:%s], new data[%s:%s]" % (csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
            g_logger.debug("Time range overlap !!! Adjust the time duration from the CSV data[%s %s:%s]: %s:%s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
        else:
# I assume that the two interval must be consecutive 
            if time_duration_start > csv_old_time_duration_tuple.time_duration_end: 
                time_duration_start = csv_old_time_duration_tuple.time_duration_end + 1
                web2csv_time_duration_update.AppendDirection = WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_BACK
            elif time_duration_end < csv_old_time_duration_tuple.time_duration_start:
                time_duration_end = csv_old_time_duration_tuple.time_duration_start -1
                web2csv_time_duration_update.AppendDirection = WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT
            else:
                raise ValueError("The system does NOT support this type[4] of the range update; CSV data[%s:%s], new data[%s:%s]" % (csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
            g_logger.debug("Time range does Not overlap !!! Adjust the time duration from the CSV data[%s %s:%s]: %s:%s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
# Set the time range config
            # if web2csv_time_duration_update.NeedUpdate:
        assert web2csv_time_duration_update.NeedUpdate, "Error! No data to be updated!!"
# The CSV time range is going to be modified due to new web data
        web2csv_time_duration_update.OldCSVStart = csv_old_time_duration_tuple.time_duration_start
        web2csv_time_duration_update.OldCSVEnd = csv_old_time_duration_tuple.time_duration_end
        web2csv_time_duration_update.NewWebStart = time_duration_start
        web2csv_time_duration_update.NewWebEnd = time_duration_end
        if web2csv_time_duration_update.AppendDirection == WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
# Check if the new CSV time range is continous
            if web2csv_time_duration_update.OldCSVStart - 1 != web2csv_time_duration_update.NewWebEnd:
                raise ValueError("Incorrect time range for update front; OldCSV[%s:%s], NewWeb[%s:%s]" % (web2csv_time_duration_update.OldCSVStart, web2csv_time_duration_update.OldCSVEnd, web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd))
            web2csv_time_duration_update.NewCSVStart = web2csv_time_duration_update.NewWebStart
            web2csv_time_duration_update.NewCSVEnd = web2csv_time_duration_update.OldCSVEnd
        elif web2csv_time_duration_update.AppendDirection == WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_BACK:
# Check if the new CSV time range is continous
            if web2csv_time_duration_update.NewWebStart - 1 != web2csv_time_duration_update.OldCSVEnd:
                raise ValueError("Incorrect time range for update back; OldCSV[%s:%s], NewWeb[%s:%s]" % (web2csv_time_duration_update.OldCSVStart, web2csv_time_duration_update.OldCSVEnd, web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd))
            web2csv_time_duration_update.NewCSVStart = web2csv_time_duration_update.OldCSVStart
            web2csv_time_duration_update.NewCSVEnd = web2csv_time_duration_update.NewWebEnd 
        return web2csv_time_duration_update


    def _get_non_overlapped_web2csv_time_duration_update_cfg(self, time_duration_start, time_duration_end):
        # import pdb; pdb.set_trace()
# If it's time first time to write the data from web to CSV ......
        web2csv_time_duration_update = WebScrapyBase.Web2CSVTimeRangeUpdate()
        web2csv_time_duration_update.NewCSVStart = web2csv_time_duration_update.NewWebStart = time_duration_start
        web2csv_time_duration_update.NewCSVEnd = web2csv_time_duration_update.NewWebEnd = time_duration_end
        web2csv_time_duration_update.AppendDirection = WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_BACK
        return web2csv_time_duration_update


    def _adjust_time_duration_start_and_end_time_func_ptr(self, time_duration_type):
        if self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR is None:
            self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR = [
                self._adjust_time_today_start_and_end_time, 
                self._adjust_time_last_start_and_end_time, 
                self._adjust_time_range_start_and_end_time
            ]
        return self.GET_TIME_DURATION_START_AND_END_TIME_FUNC_PTR[time_duration_type]


    def _adjust_time_today_start_and_end_time(self, *args):
        time_duration_start = time_duration_end = CMN.CLS.FinanceDate.get_today_finance_date()
        return CMN.CLS.TimeDurationTuple(time_duration_start, time_duration_end)


    def _adjust_time_last_start_and_end_time(self, *args):
        today_data_exist_hour = CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_HOUR if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR
        today_data_exist_minute = CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_MINUTE if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR
        time_duration_start = time_duration_end = CMN.FUNC.get_last_url_data_date(today_data_exist_hour, today_data_exist_minute) 
        return CMN.CLS.TimeDurationTuple(time_duration_start, time_duration_end)


    def _adjust_time_range_start_and_end_time(self, *args):
# in Market mode
# args[0]: source_type_index
# in Stock mode
# args[0]: source_type_index
# args[1]: company_code_number
        time_duration_start_from_lookup_table = self._get_url_time_range().get_time_range_start(*args)
        time_duration_end_from_lookup_table = self._get_url_time_range().get_time_range_end(*args)
        time_duration_start = time_duration_end = None
        if self.xcfg["time_duration_start"] is None:
            time_duration_start = time_duration_start_from_lookup_table
        else:
            if self.xcfg["time_duration_start"] < time_duration_start_from_lookup_table:
                time_duration_start = time_duration_start_from_lookup_table
            else:
                time_duration_start = self.xcfg["time_duration_start"]
        if self.xcfg["time_duration_end"] is None:
            time_duration_end = time_duration_end_from_lookup_table
        else:
            if self.xcfg["time_duration_end"] > time_duration_end_from_lookup_table:
                time_duration_end = time_duration_end_from_lookup_table
            else:
                time_duration_end = self.xcfg["time_duration_end"]
        return CMN.CLS.TimeDurationTuple(time_duration_start, time_duration_end)


    def _get_timeslice_iterable(self, **kwargs):
        # import pdb;pdb.set_trace()
        assert kwargs["time_duration_start"].get_time_unit_type() == kwargs["time_duration_end"].get_time_unit_type(), "The time unit of start and end time is NOT identical; Start: %s, End: %s" % (type(kwargs["time_duration_start"]), type(kwargs["time_duration_end"]))
        if self.url_time_unit != kwargs["time_duration_start"].get_time_unit_type():
            (new_finance_time_start, new_finance_time_end) = self._modify_time_for_timeslice_generator(kwargs["time_duration_start"], kwargs["time_duration_end"])
            kwargs["time_duration_start"] = new_finance_time_start
            kwargs["time_duration_end"] = new_finance_time_end
# Generate the time slice
        timeslice_iterable = self._get_time_slice_generator().generate_time_slice(self.timeslice_generate_method, **kwargs)
        return timeslice_iterable


    def _modify_time_for_timeslice_generator(self, finance_time_start, finance_time_end):
        # """IMPORTANT: This function should NOT be implemented and called unless the time unit is NOT date !"""
        raise RuntimeError("This %s function should NOT be called !!!" % sys._getframe(0).f_code.co_name)


    @abstractmethod
    def _check_old_csv_time_duration_exist(self, *args):
        raise NotImplementedError


    @abstractmethod
    def _adjust_csv_time_duration(self):
        raise NotImplementedError


    @abstractmethod
    def _parse_web_data(self, web_data):
        raise NotImplementedError


    @abstractmethod
    def scrap_web_to_csv(self):
        raise NotImplementedError
