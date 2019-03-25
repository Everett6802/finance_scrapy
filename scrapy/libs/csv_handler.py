# -*- coding: utf8 -*-

import re
import collections
import company_profile as CompanyProfile
import workday_canlendar as WorkdayCanlendar
import scrapy.common as CMN
from scrapy.common.common_variable import GlobalVar as GV
g_logger = CMN.LOG.get_logger()


class CSVHandler(object):

    workday_canlendar = None


    @classmethod
    def __get_workday_canlendar(cls):
        if cls.workday_canlendar is None:
            cls.workday_canlendar = WorkdayCanlendar.WorkdayCanlendar.Instance()
        return cls.workday_canlendar


    @classmethod
    def __check_time_continuity(cls, new_time_start, new_time_end, orig_time_start, orig_time_end):
        # import pdb; pdb.set_trace()
        can_check = False
        if new_time_end < orig_time_start:
            assert type(new_time_end) == type(orig_time_start), "The time type [new_time_end: %s, orig_time_start: %s] is NOT identical" % (new_time_end, orig_time_start)
            if cls.__get_workday_canlendar().is_consecutive_prev_workday(orig_time_start, new_time_end):
                return CMN.DEF.TIME_OVERLAP_BEFORE
        else:
            assert type(new_time_start) == type(orig_time_end), "The time type [new_time_start: %s, orig_time_end: %s] is NOT identical" % (new_time_start, orig_time_end) 
            if cls.__get_workday_canlendar().is_consecutive_next_workday(orig_time_end, new_time_start):
                return CMN.DEF.TIME_OVERLAP_AFTER
        return CMN.DEF.TIME_OVERLAP_NONE


    def __enter__(self):
        return self


    def __exit__(self, type, msg, traceback):
        self.__flush()
        return False


    def __init__(self, scrapy_method, csv_parent_folderpath=None, company_number=None, company_group_number=None):
        if type(scrapy_method) is int:
            self.scrapy_method_index = scrapy_method
            self.scrapy_method = CMN.DEF.SCRAPY_METHOD_NAME[scrapy_method]
        else:
            self.scrapy_method = scrapy_method
            self.scrapy_method_index = CMN.DEF.SCRAPY_METHOD_NAME.index(scrapy_method)

        self.csv_parent_folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH if (csv_parent_folderpath is None) else csv_parent_folderpath

        if CMN.FUNC.scrapy_method_need_company_number(self.scrapy_method_index):
            if company_number is None:
                raise ValueError("The scrapy method[%s] requires company number" % self.scrapy_method)
        else:
            if company_number is not None:
                raise ValueError("The scrapy method[%s] doesn't require company number" % self.scrapy_method)
        self.company_number = company_number
        self.company_group_number = None
        if self.company_number is not None:
            if company_group_number is None:
                company_profile = CompanyProfile.CompanyProfile.Instance()
                company_group_number = company_profile.lookup_company_group_number(self.company_number)
            self.company_group_number = company_group_number

        self.csv_filepath = CMN.FUNC.get_finance_data_csv_filepath(self.scrapy_method_index, self.csv_parent_folderpath, self.company_group_number, self.company_number)
        self.csv_buffer_list = None
        self.csv_buffer_list_len = 0
        # self.csv_time_range = None
        self.csv_time_duration_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(self.scrapy_method_index, self.csv_parent_folderpath, self.company_group_number)
        self.csv_time_duration_cfg_dict = None # {}} if the time range configuration file does NOT exist
        self.csv_time_duration_tuple = None # None if the time range of the scrapy method does NOT exist
        self.append_direction = CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_NONE
        self.flush_threshold = CMN.FUNC.scrapy_method_csv_flush_threshold(self.scrapy_method_index)


    def __read_time_range_cfg(self):
        assert self.csv_time_duration_cfg_dict is None, "csv_time_duration_cfg_dict has already been read"
        # import pdb; pdb.set_trace()
# Check old data time range
        self.csv_time_duration_cfg_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, self.csv_time_duration_folderpath)
        if self.csv_time_duration_cfg_dict is None:
            self.csv_time_duration_cfg_dict = {}
        # assert self.csv_time_duration_cfg_dict is not None, "csv_time_duration_cfg_dict should NOT be None"


    def __get_old_csv_time_range(self):
        if self.csv_time_duration_cfg_dict is None:
            self.__read_time_range_cfg()
            self.csv_time_duration_tuple = self.csv_time_duration_cfg_dict.get(self.scrapy_method_index, None)
            if self.csv_time_duration_tuple is None:
               g_logger.debug("No %s data......" % CMN.FUNC.assemble_scrapy_method_description(self.scrapy_method_index, self.company_number))
          

    def find_scrapy_time_range(self, time_start, time_end):
        self.__get_old_csv_time_range()
        csv_old_data_exist = True if (self.csv_time_duration_tuple is not None) else False
        data_time_unit = CMN.DEF.SCRAPY_DATA_TIME_UNIT[self.scrapy_method_index]
        # import pdb; pdb.set_trace()
# Caution: Need transfrom the time string from unicode to string
# Set the end time
        if time_end is None:
            time_end = CMN.FUNC.generate_today_time_str()
        if type(time_end) is str: 
            time_end = CMN.CLS.FinanceTimeBase.from_time_string(time_end, data_time_unit)
# Set the start time
        if time_start is None:
            if csv_old_data_exist:
                time_start = self.csv_time_duration_tuple.time_duration_end
            else:
                time_start = time_end - CMN.DEF.DEF_TIME_RANGE_LIST[data_time_unit]
        if type(time_start) is str: 
            time_start = CMN.CLS.FinanceTimeBase.from_time_string(time_start, data_time_unit)
# Calculate the time range
        new_extension_csv_time_duration_tuple, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_start, 
            time_end,
            self.csv_time_duration_tuple,
            check_time_continuity_funcptr=self.__check_time_continuity,
        )
        return web2csv_time_duration_update_tuple


    def write(self, data_list, time_start, time_end, append_direction):
        assert data_list is not None, "data_list should NOT be None"
        self.__get_old_csv_time_range()
# Check need to flush the old data
        if self.append_direction != CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_NONE:
            should_flush = False
            if self.append_direction != append_direction:
                should_flush = True
            elif self.csv_buffer_list_len >= self.flush_threshold:
                should_flush = True
            if should_flush:
                self.__flush()
        else:
            assert self.csv_buffer_list is None, "csv_buffer_list should be None"
            assert self.csv_buffer_list_len == 0, "csv_buffer_list_len should be 0"
# Adjust the time range if the time unit is different if necessary
        if not CMN.FUNC.scrapy_method_is_scrapy_and_data_time_unit_the_same(self.scrapy_method_index):
            (time_start, time_end) = self.__adjust_time_range_for_different_time_unit(time_start, time_end)
# time_start, time_end is the scrapy time range
        # import pdb; pdb.set_trace()
        data_time_unit = CMN.FUNC.scrapy_method_data_time_unit(self.scrapy_method_index)
        is_scrapy_and_data_time_unit_the_same = CMN.FUNC.scrapy_method_is_scrapy_and_data_time_unit_the_same(self.scrapy_method_index)
        if type(time_start) is str: 
            time_start = CMN.CLS.FinanceTimeBase.from_time_string(time_start, data_time_unit)
        data_list_time_start = CMN.CLS.FinanceTimeBase.from_time_string(str(data_list[0][0]), data_time_unit)
        if time_start > data_list_time_start:
            if not is_scrapy_and_data_time_unit_the_same and append_direction == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
                time_start = data_list_time_start
            else:
                raise ValueError("Incorrect time range: time_start[%s] > data_list_time_start[%s]" % (time_start, data_list_time_start))
        if type(time_end) is str: 
            time_end = CMN.CLS.FinanceTimeBase.from_time_string(time_end, data_time_unit)
        data_list_time_end = CMN.CLS.FinanceTimeBase.from_time_string(str(data_list[-1][0]), data_time_unit)
        if time_end < data_list_time_end:
            if not is_scrapy_and_data_time_unit_the_same and append_direction == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_AFTER:
                time_end = data_list_time_end
            else:
                raise ValueError("Incorrect time range: time_end[%s] < data_list_time_end[%s]" % (time_end, data_list_time_end))
# Initialize the buffer
        if self.csv_buffer_list is None:
            assert self.append_direction == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_NONE, "append direction should be NONE"
            self.csv_buffer_list = []
            self.append_direction = append_direction
        # import pdb; pdb.set_trace()
# Calculate the time range
        new_extension_csv_time_duration_tuple, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_start, 
            time_end,
            self.csv_time_duration_tuple,
            check_time_continuity_funcptr=self.__check_time_continuity,
        )

        assert web2csv_time_duration_update_tuple is not None, "web2csv_time_duration_update_tuple should not be NONE"
        web2csv_time_duration_update_tuple_len = len(web2csv_time_duration_update_tuple)
        assert web2csv_time_duration_update_tuple_len == 1, "web2csv_time_duration_update_tuple should not be NONE"
        web2csv_time_duration_update = web2csv_time_duration_update_tuple[0]
# Find the file path for writing data into csv
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
        web2csv_time_duration_update.backup_old_csv_if_necessary(self.csv_filepath)
        count = 0
        if web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
            for data_entry in data_list:
                time_duration = CMN.CLS.FinanceTimeBase.from_time_string(str(data_entry[0]), data_time_unit)
                if time_duration > web2csv_time_duration_update.NewWebEnd:
                    break
                self.csv_buffer_list.append(data_entry)
                count += 1
        elif web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_AFTER:
            for data_entry in data_list:
                time_duration = CMN.CLS.FinanceTimeBase.from_time_string(str(data_entry[0]), data_time_unit)
                if time_duration < web2csv_time_duration_update.NewWebStart:
                    continue
                self.csv_buffer_list.append(data_entry)
                count += 1
        else:
            raise ValueError("Unsupport AppendDirection: %d" % web2csv_time_duration_update.AppendDirection)
        self.csv_buffer_list_len += count
        web2csv_time_duration_update.append_old_csv_if_necessary(self.csv_filepath)
# Update the time duration
        # import pdb; pdb.set_trace()
        self.csv_time_duration_tuple = new_extension_csv_time_duration_tuple


    def __adjust_time_range_for_different_time_unit(self, time_start, time_end):
        data_time_unit = CMN.FUNC.scrapy_method_data_time_unit(self.scrapy_method_index)
        scrapy_time_unit = CMN.FUNC.scrapy_method_scrapy_time_unit(self.scrapy_method_index)
        if data_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY and scrapy_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
            assert time_start == time_end, "The extended time range start[%s] and end[%s] should be the same month" % (time_start, time_end)
            workday_canlendar = WorkdayCanlendar.WorkdayCanlendar.Instance()
            # extended_time_range_date_start = None
            if workday_canlendar.FirstMonthOfWorkday == time_start:
                time_start = workday_canlendar.FirstWorkday
            else:
                time_start = CMN.CLS.FinanceDate(time_start.year, time_start.month, 1)
            # extended_time_range_date_end = None
            if workday_canlendar.LastMonthOfWorkday == time_end:
                time_end = workday_canlendar.LastWorkday
            else:
                time_end = CMN.CLS.FinanceDate(time_end.year, time_end.month, time_end.get_last_date_of_month())
        else:
            raise ValueError("Unsupport time unit to extend, data: %d, scrapy: %d" % (data_time_unit, scrapy_time_unit))
        return (time_start, time_end)


    def __flush(self):
# Update the data 
        if self.csv_buffer_list is not None and self.csv_buffer_list_len > 0:
            # assert self.csv_time_duration_tuple is not None, "csv_time_duration_tuple should NOT be None"
            g_logger.debug("Write %d data to %s" % (self.csv_buffer_list_len, self.csv_filepath))
            CMN.FUNC.write_csv_data(self.csv_buffer_list, self.csv_filepath)
            self.csv_buffer_list = None
            self.csv_buffer_list_len = 0
            self.append_direction = CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_NONE
# Update the data time range
        if self.csv_time_duration_tuple is not None and self.csv_time_duration_cfg_dict[self.scrapy_method_index] != self.csv_time_duration_tuple:
            self.csv_time_duration_cfg_dict[self.scrapy_method_index] = self.csv_time_duration_tuple
            CMN.FUNC.write_csv_time_duration(self.csv_time_duration_cfg_dict, self.csv_time_duration_folderpath)


    @property
    def FlushThreshold(self):
        return self.flush_threshold
    @FlushThreshold.setter
    def FlushThreshold(self, flush_threshold):
        self.flush_threshold = flush_threshold
