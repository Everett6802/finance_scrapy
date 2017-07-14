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


        def backup_old_csv_if_necessary(self, csv_filepath):
            if self.append_direction == self.WEB2CSV_APPEND_FRONT: #BASE.BASE.WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
                g_logger.debug("Need add the new data in front of the old CSV data, rename the file: %s" % (csv_filepath + ".old"))
                CMN.FUNC.rename_file_if_exist(csv_filepath, csv_filepath + ".old") 


        def append_old_csv_if_necessary(self, csv_filepath):
            if self.append_direction == self.WEB2CSV_APPEND_FRONT: #BASE.BASE.WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
                g_logger.debug("Append the old CSV data to the file: %s" % csv_filepath)
                CMN.FUNC.append_data_into_file(csv_filepath + ".old", csv_filepath)
                CMN.FUNC.remove_file_if_exist(csv_filepath + ".old") 


    class CSVFileNoScrapyRecord(object):

        # STATUS_RECORD_TIME_RANGE_NOT_OVERLAP = 0
        # STATUS_RECORD_CSV_FILE_ALREADY_EXIST = 1
        # STATUS_RECORD_WEB_DATA_NOT_FOUND = 2
        # RECORD_TYPE_INDEX_LIST = [
        #     STATUS_RECORD_TIME_RANGE_NOT_OVERLAP,
        #     STATUS_RECORD_CSV_FILE_ALREADY_EXIST,
        #     STATUS_RECORD_WEB_DATA_NOT_FOUND
        # ]
        RECORD_TYPE_INDEX = 0
        RECORD_TYPE_DESCRIPTION_INDEX = 1
        RECORD_TYPE_ENTRY_LIST = [
            ["TimeRangeNotOverlap", "The search time range does NOT overlap the one in the URL time range lookup table",],
            ["CSVFileAlreadyExist", "The CSV files of the time range has already existed in the local folder",],
            ["WebDataNotFound", "The web data of the URL is NOT found",],
        ]
        RECORD_TYPE_SIZE = len(RECORD_TYPE_ENTRY_LIST)
        TIME_RANGE_NOT_OVERLAP_RECORD_INDEX = 0
        CSV_FILE_ALREADY_EXIST_RECORD_INDEX = 1
        WEB_DATA_NOT_FOUND_RECORD_INDEX = 2

        RECORD_TYPE_LIST = [entry[RECORD_TYPE_INDEX] for entry in RECORD_TYPE_ENTRY_LIST]
        RECORD_TYPE_DESCRIPTION_LIST = [entry[RECORD_TYPE_DESCRIPTION_INDEX] for entry in RECORD_TYPE_ENTRY_LIST]

        @classmethod
        def create_register_status_instance(cls):
            # import pdb; pdb.set_trace()
            csv_file_no_scrapy_record = cls()
            for index in range(cls.RECORD_TYPE_SIZE):
                csv_file_no_scrapy_record.__register_record_type(
                    cls.RECORD_TYPE_LIST[index], 
                    cls.RECORD_TYPE_DESCRIPTION_LIST[index]
                )
            return csv_file_no_scrapy_record


        def __init__(self):
            self.record_type_dict = {}
            self.record_type_description_dict = {}
            self.web_data_not_found_time_start = None
            self.web_data_not_found_time_end = None


        def __register_record_type(self, record_type_name, record_type_description):
            # import pdb; pdb.set_trace()
            if self.record_type_dict.has_key(record_type_name):
                g_logger.debug("The type[%s] has already exist" % record_type_name)
                return
            self.record_type_dict[record_type_name] = []
            self.record_type_description_dict[record_type_name] = record_type_description


        def __add_record(self, record_type_name, *args):
            if not self.record_type_dict.has_key(record_type_name):
                raise ValueError("Unknown Check Status Type: %s" % record_type_name)
            self.record_type_dict[record_type_name].append(args)


        def add_time_range_not_overlap_record(self, *args):
# Market
# args[0]: source type index
# Stock
# args[0]: source type index
# args[1]: company code number
            self.__add_record("TimeRangeNotOverlap", *args)

        def add_csv_file_already_exist_record(self, *args):
# Market
# args[0]: source type index
# Stock
# args[0]: source type index
# args[1]: company code number
            self.__add_record("CSVFileAlreadyExist", *args)

        def add_web_data_not_found_record(self, *args):
# Market
# args[0]: time slice. None for a must to flush data into list
# args[1]: source type index
# Stock
# args[0]: time slice. None for a must to flush data into list
# args[1]: source type index
# args[2]: company code number
            need_flush = False
            if args[0] is None:
                if self.web_data_not_found_time_start is not None:
                    need_flush = True
            else:
                if self.web_data_not_found_time_start is None:
                    self.web_data_not_found_time_start = self.web_data_not_found_time_end = args[0]
                else:
                    if self.web_data_not_found_time_end.check_continous_time_duration(args[0]):
                        self.web_data_not_found_time_end = args[0]
                    else:
                        need_flush = True
# Keep track of the time range in which the web data is empty
            if need_flush:
# Market
# args_new[0]: time slice. None for a must to flush data into list
# args_new[1]: source type index
# args_new[2]: empty time start
# args_new[3]: empty time end
# Stock
# args_new[0]: time slice. None for a must to flush data into list
# args_new[1]: source type index
# args_new[2]: company code number
# args_new[2]: empty time start
# args_new[3]: empty time end
                args_new = copy.deepcopy(args)
                args_new.append(self.web_data_not_found_time_start)
                args_new.append(self.web_data_not_found_time_end)
                self.web_data_not_found_time_start = self.web_data_not_found_time_end = None
                self.__add_record("WebDataNotFound", *args_new)


        # def get_status_list(self, record_type_name):
        #     if not self.record_type_dict.has_key(record_type_name):
        #         raise ValueError("Unknown Check Status Type: %s" % record_type_name)
        #     return self.record_type_dict[record_type_name]


    PARSE_URL_DATA_FUNC_PTR = None
# Find corresponding config of the module
    SOURCE_TYPE_INDEX = None
    CONSTANT_CFG = None
    URL_TIME_UNIT = None
    URL_FORMAT = None
    URL_PARSING_METHOD = None
    TIMESLICE_GENERATE_METHOD = None
    TIMESLICE_TIME_UNIT = None

    @classmethod
    def __select_web_data_by_bs4(cls, url_data, parse_url_data_type_cfg):
        g_logger.debug("Parse URL data by BS4, Encoding:%s, Selector: %s" % (parse_url_data_type_cfg["url_encoding"], parse_url_data_type_cfg["url_data_selector"]))
        url_data.encoding = parse_url_data_type_cfg["url_encoding"]
        soup = BeautifulSoup(url_data.text)
        return soup.select(parse_url_data_type_cfg["url_data_selector"])


    @classmethod
    def __select_web_data_by_json(cls, url_data, parse_url_data_type_cfg):
        g_logger.debug("Parse URL data by JSON, Selector: %s" % parse_url_data_type_cfg["url_data_selector"])
        json_url_data = json.loads(url_data.text)
        return json_url_data[parse_url_data_type_cfg["url_data_selector"]]


    @classmethod
    def __select_web_data_by_customization(cls, url_data, parse_url_data_type_cfg):
        g_logger.debug("Parse URL data by Customization......")
        if not hasattr(cls, "_customized_select_web_data"):
            raise AttributeError("_customized_select_web_data() is NOT implemented")
        return cls._customized_select_web_data(url_data, parse_url_data_type_cfg)


    @classmethod
    def _write_to_csv(cls, csv_filepath, csv_data_list):
        # import pdb; pdb.set_trace()
        if cls.TIMESLICE_TIME_UNIT != cls.URL_TIME_UNIT:
            g_logger.debug("Multi-data in one page, need to transform data structure")
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
    def get_class_name(cls):
        return cls.__name__


    @classmethod
    def get_parent_class(cls):
        assert len(cls.__bases__) == 1, "Only support single inheritance"
        return cls.__bases__[0]


    @classmethod
    def init_class_common_variables(cls, designated_source_type_index=None):
# CAUTION: This function MUST be called by the LEAF derived class
        if cls.PARSE_URL_DATA_FUNC_PTR is None:
            cls.PARSE_URL_DATA_FUNC_PTR = [
                cls.__select_web_data_by_bs4, 
                cls.__select_web_data_by_json,
                cls.__select_web_data_by_customization,
            ]
# Find correspnding index of the class
# Caution: Can NOT be called here !!! 
# Since this function is probably called by the class which is NOT a leaf derived class
        if cls.SOURCE_TYPE_INDEX is None:
            if designated_source_type_index is not None:
                assert designated_source_type_index >= 0 and designated_source_type_index < CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN, "source type index is Out-of-Range [0, %d)" % CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN
                cls.SOURCE_TYPE_INDEX = designated_source_type_index
            else:
                cls.SOURCE_TYPE_INDEX = CMN.DEF.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index(cls.__name__)
# Find corresponding config of the module
        if cls.CONSTANT_CFG is None:
            cls.CONSTANT_CFG = CMN.DEF.DEF_SOURCE_CONSTANT_CFG[cls.SOURCE_TYPE_INDEX]
        if cls.URL_FORMAT is None:
            cls.URL_FORMAT = cls.CONSTANT_CFG["url_format"]
        if cls.URL_TIME_UNIT is None:
            cls.URL_TIME_UNIT = cls.CONSTANT_CFG["url_time_unit"]
        if cls.URL_PARSING_METHOD is None:
            cls.URL_PARSING_METHOD = cls.CONSTANT_CFG["url_parsing_method"]
        if cls.TIMESLICE_GENERATE_METHOD is None:
            cls.TIMESLICE_GENERATE_METHOD = cls.CONSTANT_CFG["timeslice_generate_method"]
        if cls.TIMESLICE_TIME_UNIT is None:
            cls.TIMESLICE_TIME_UNIT = CMN.DEF.TIMESLICE_GENERATE_TO_TIME_UNIT_MAPPING[cls.TIMESLICE_GENERATE_METHOD]


    @classmethod
    def init_class_customized_variables(cls):
# CAUTION: This function MUST be called by the LEAF derived class
        pass


    @classmethod
    def get_web_data(cls, url):
        # req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
        req = CMN.FUNC.request_from_url_and_check_return(url)
        web_data = (cls.PARSE_URL_DATA_FUNC_PTR[cls.URL_PARSING_METHOD])(req, cls.CONSTANT_CFG)
        return web_data


    @classmethod
    def post_check_web_data(cls, web_data):
        pass


    @classmethod
    def try_get_web_data(cls, url):
        g_logger.debug("Scrape web data from URL: %s" % url)
        web_data = None
        try:
# Grab the data from website and assemble the data to the entry of CSV
            web_data = cls.get_web_data(url)
            assert (web_data is not None), "web_data should NOT be None"
            cls.post_check_web_data(web_data)
        except CMN.EXCEPTION.WebScrapyNotFoundException as e:
            errmsg = None
            if isinstance(e.message, str):
                errmsg = "WebScrapyNotFoundException occurs while scraping URL[%s], due to: %s" % (url, e.message)
            else:
                errmsg = u"WebScrapyNotFoundException occurs while scraping URL[%s], due to: %s" % (url, e.message)
            CMN.FUNC.try_print(errmsg)
            g_logger.error(errmsg)
            raise e
        except CMN.EXCEPTION.WebScrapyServerBusyException as e:
# Server is busy, let's retry......
            RETRY_TIMES = 5
            SLEEP_TIME_BEFORE_RETRY = 15
            scrapy_success = False
            for retry_times in range(1, RETRY_TIMES + 1):
                if scrapy_success:
                    break
                g_logger.warn("Server is busy, let's retry...... %d", retry_times)
                time.sleep(SLEEP_TIME_BEFORE_RETRY * retry_times)
                try:
                    web_data = cls.get_web_data(url)
                    assert (web_data is not None), "web_data should NOT be None"
                    cls.post_check_web_data(web_data)
                except CMN.EXCEPTION.WebScrapyServerBusyException as e:
                    pass
                else:
                    scrapy_success = True
            if not scrapy_success:
                raise CMN.EXCEPTION.WebScrapyServerBusyException("Fail to scrap URL[%s] after retry for %d times" % (url, RETRY_TIMES))
        except Exception as e:
            # import pdb;pdb.set_trace()
            if isinstance(e.message, str):
                g_logger.warn("Exception occurs while scraping URL[%s], due to: %s" % (url, e.message))
            else:
                g_logger.warn(u"Exception occurs while scraping URL[%s], due to: %s" % (url, e.message))
        return web_data


    @classmethod
    def check_web_data_exist(cls, url):
        web_data = cls.get_web_data(url)
        return True if (web_data is not None and len(web_data) > 0) else False


    def __init__(self, **kwargs):
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
        self.description = None
        self.timeslice_generator = None
        self.url_time_range = None
        self.csv_file_no_scrapy_record = WebScrapyBase.CSVFileNoScrapyRecord.create_register_status_instance()
        self.csv_file_no_scrapy_record_string_dict = collections.OrderedDict()
        # self.emtpy_web_data_list = None


    @classmethod
    def _get_url_time_range(cls):
        raise NotImplementedError


    @property
    def SourceTypeIndex(self):
        return self.SOURCE_TYPE_INDEX


    def get_description(self):
        if self.description is None:
            self.description = "%s" % CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX]
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
                g_logger.debug("The time duration[%s:%s] of the CSV data[%s] already exist ......" % (time_duration_start, time_duration_end, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX]))
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
            g_logger.debug("Time range overlap !!! Adjust the time duration from the CSV data[%s %s:%s]: %s:%s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX], csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
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
            g_logger.debug("Time range does Not overlap !!! Adjust the time duration from the CSV data[%s %s:%s]: %s:%s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.SOURCE_TYPE_INDEX], csv_old_time_duration_tuple.time_duration_start, csv_old_time_duration_tuple.time_duration_end, time_duration_start, time_duration_end))
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


    def _adjust_time_range_from_web(self, *args):
# in Market mode
# args[0]: source_type_index
# in Stock mode
# args[0]: source_type_index
# args[1]: company_code_number
# Define the suitable time range
# define the function for transforming the time unit
        def transfrom_time_duration_start_time_unit_from_date(time_duration_start):
            assert isinstance(time_duration_start, CMN.CLS.FinanceDate), "The input start time duration time unit is %s, not FinanceDate" % type(time_duration_start)
# Trasform the start time unit
            if self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_DAY:
                return time_duration_start
            elif self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_MONTH:
                return CMN.CLS.FinanceMonth.get_finance_month_from_date(time_duration_start)
            elif self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_QUARTER:
                return CMN.CLS.FinanceQuarter.get_start_finance_quarter_from_date(time_duration_start)
            raise ValueError("Unsupported URL time unit in start time: %d" % self.TIMESLICE_TIME_UNIT)
        def transfrom_time_duration_end_time_unit_from_date(time_duration_end):
            assert isinstance(time_duration_end, CMN.CLS.FinanceDate), "The input end time duration time unit is %s, not FinanceDate" % type(time_duration_end)
# Trasform the end time unit
            if self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_DAY:
                return time_duration_end
            elif self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_MONTH:
                return  CMN.CLS.FinanceMonth.get_finance_month_from_date(time_duration_end)
            elif self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_QUARTER:
                return  CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(time_duration_end)
            raise ValueError("Unsupported URL time unit in end time: %d" % self.TIMESLICE_TIME_UNIT)
# Transform the time unit
        # import pdb; pdb.set_trace()
        time_duration_start = None
        time_duration_end = None
        if self.xcfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_TODAY:
            time_duration_start = time_duration_end = CMN.CLS.FinanceDate.get_today_finance_date()
            time_duration_start = transfrom_time_duration_start_time_unit_from_date(time_duration_start)
            time_duration_end = transfrom_time_duration_end_time_unit_from_date(time_duration_end)
        elif self.xcfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_LAST:
            time_duration_start = time_duration_end = CMN.CLS.FinanceDate.get_last_finance_date()
            time_duration_start = transfrom_time_duration_start_time_unit_from_date(time_duration_start)
            time_duration_end = transfrom_time_duration_end_time_unit_from_date(time_duration_end)
        elif self.xcfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_RANGE:
            (time_duration_start_from_lookup_table, time_duration_end_from_lookup_table) = self._get_url_time_range().get_time_range(*args)
            time_start_from_table = False
            if self.xcfg["time_duration_start"] is None:
                time_duration_start = time_duration_start_from_lookup_table
                time_start_from_table = True
            else:
# Trasform the start time unit
                time_duration_start = transfrom_time_duration_start_time_unit_from_date(self.xcfg["time_duration_start"])
                assert time_duration_start.get_time_unit_type() == time_duration_start_from_lookup_table.get_time_unit_type() , "The time duration start time unit is NOT identical, %d, %d" % (time_duration_start.get_time_unit_type(), time_duration_start_from_lookup_table.get_time_unit_type()) 
            time_end_from_table = False
            if self.xcfg["time_duration_end"] is None:
                time_duration_end = time_duration_end_from_lookup_table
                time_end_from_table = True
            else:
# Trasform the end time unit
                time_duration_end = transfrom_time_duration_end_time_unit_from_date(self.xcfg["time_duration_end"])
                assert time_duration_end.get_time_unit_type() == time_duration_end_from_lookup_table.get_time_unit_type() , "The time duration end time unit is NOT identical, %d, %d" % (time_duration_end.get_time_unit_type(), time_duration_end_from_lookup_table.get_time_unit_type())     
            need_check_overlap = not (time_start_from_table or time_end_from_table)
# Check the time duration is in the range of table
            if need_check_overlap:
                if not CMN.FUNC.is_time_range_overlap(time_duration_start_from_lookup_table, time_duration_end_from_lookup_table, time_duration_start, time_duration_end):
                    g_logger.debug("The time range[%s-%s] for searching is Out of Range of the table[%s-%s]" % (time_duration_start, time_duration_end, time_duration_start_from_lookup_table, time_duration_end_from_lookup_table))
                    return None
            if (not time_start_from_table) and (time_duration_start < time_duration_start_from_lookup_table):
                time_duration_start = time_duration_start_from_lookup_table
            if (not time_end_from_table) and (time_duration_end > time_duration_end_from_lookup_table):
                time_duration_end = time_duration_end_from_lookup_table
        else:
            raise ValueError("Unknown time duration type: %d" % self.xcfg["time_duration_type"])
        # import pdb; pdb.set_trace()
        assert time_duration_start.get_time_unit_type() == self.URL_TIME_UNIT, "The time unit shold be: %d, NOT: %d" % (self.TIMESLICE_TIME_UNIT, time_duration_start.get_time_unit_type())
        assert time_duration_start.get_time_unit_type() == time_duration_end.get_time_unit_type(), "The time unit is NOT identical, start: %d, end: %d" % (time_duration_start.get_time_unit_type(), time_duration_end.get_time_unit_type())
        return CMN.CLS.TimeDurationTuple(time_duration_start, time_duration_end)


    def _get_timeslice_iterable(self, **kwargs):
        # import pdb;pdb.set_trace()
        if self.TIMESLICE_TIME_UNIT != self.URL_TIME_UNIT:
            assert kwargs["time_duration_start"].get_time_unit_type() == CMN.DEF.DATA_TIME_UNIT_DAY, "The time unit shold be: %d, NOT: %d" % (CMN.DEF.DATA_TIME_UNIT_DAY, kwargs["time_duration_start"].get_time_unit_type())
            assert kwargs["time_duration_start"].get_time_unit_type() == kwargs["time_duration_end"].get_time_unit_type(), "The time unit of start and end time is NOT identical; Start: %s, End: %s" % (type(kwargs["time_duration_start"]), type(kwargs["time_duration_end"]))
            (new_finance_time_start, new_finance_time_end) = self.__change_time_unit_for_timeslice_generator(kwargs["time_duration_start"], kwargs["time_duration_end"])
            kwargs["time_duration_start"] = new_finance_time_start
            kwargs["time_duration_end"] = new_finance_time_end
# Generate the time slice
        if self.timeslice_generator is None:
            self.timeslice_generator = TimeSliceGenerator.WebScrapyTimeSliceGenerator.Instance()
        timeslice_iterable = self.timeslice_generator.generate_time_slice(self.TIMESLICE_GENERATE_METHOD, **kwargs)
        return timeslice_iterable


    def __change_time_unit_for_timeslice_generator(self, finance_time_start, finance_time_end):
        # """IMPORTANT: This function should NOT be implemented and called unless the time unit is NOT date !"""
        # raise RuntimeError("This %s function should NOT be called !!!" % sys._getframe(0).f_code.co_name)
        if self.URL_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_DAY:
            if self.TIMESLICE_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_MONTH:
                time_duration_start = CMN.CLS.FinanceMonth.get_finance_month_from_date(finance_time_start)
                time_duration_end = CMN.CLS.FinanceMonth.get_finance_month_from_date(finance_time_end)
                return (time_duration_start, time_duration_end)
            elif self.TIMESLICE_TIME_UNIT == CMN.DEF.DATA_TIME_UNIT_QUARTER:
                time_duration_start = CMN.CLS.FinanceQuarter.get_start_finance_quarter_from_date(finance_time_start)
                time_duration_end = CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(finance_time_end)
                return (time_duration_start, time_duration_end)
        raise ValueError("Unsupported timeslice time unit: %d" % self.TIMESLICE_TIME_UNIT)
 

    # @property
    # def EmptyWebDataFound(self):
    #     if self.emtpy_web_data_list is None:
    #         raise ValueError("self.emtpy_web_data_list should NOT be None")
    #     return True if len(self.emtpy_web_data_list) != 0 else False


    # @property
    # def EmptyWebDataList(self):
    #     if self.emtpy_web_data_list is None:
    #         raise ValueError("self.emtpy_web_data_list should NOT be None")
    #     return self.emtpy_web_data_list


    @property
    def CSVFileNoScrapyTypeSize(self):
        return WebScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_SIZE


    @property
    def CSVFileNoScrapyTypeList(self):
        return WebScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_LIST


    @property
    def CSVFileNoScrapyTypeDescriptionList(self):
        return WebScrapyBase.CSVFileNoScrapyRecord.RECORD_TYPE_DESCRIPTION_LIST


    @property
    def CSVFileNoScrapyTimeRangeNotOverlapRecordIndex(self):
        return WebScrapyBase.CSVFileNoScrapyRecord.TIME_RANGE_NOT_OVERLAP_RECORD_INDEX


    @property
    def CSVFileNoScrapyCSVFileAlreadyExistRecordIndex(self):
        return WebScrapyBase.CSVFileNoScrapyRecord.CSV_FILE_ALREADY_EXIST_RECORD_INDEX


    @property
    def CSVFileNoScrapyWebDataNotFoundRecordIndex(self):
        return WebScrapyBase.CSVFileNoScrapyRecord.WEB_DATA_NOT_FOUND_RECORD_INDEX


    @property
    def CSVFileNoScrapyDescriptionDict(self):
        assert self.csv_file_no_scrapy_record_string_dict != None, "self.csv_file_no_scrapy_record_string_dict is None"
        return self.csv_file_no_scrapy_record_string_dict


    @abstractmethod
    def _adjust_time_range_from_csv(self, *args):
        raise NotImplementedError


    @abstractmethod
    def _parse_web_data(self, web_data):
        raise NotImplementedError


    # @abstractmethod
    # def _parse_status_to_string_list(self):
    #     raise NotImplementedError


    @abstractmethod
    def scrap_web_to_csv(self):
        raise NotImplementedError
