# -*- coding: utf8 -*-

import os
import sys
import re
from collections import OrderedDict
# import requests
# import json
# from bs4 import BeautifulSoup
import scrapy.common as CMN
from scrapy.common.common_variable import GlobalVar as GV
g_logger = CMN.LOG.get_logger()


@CMN.CLS.Singleton
class KeydayCalendar(object):

    TFEI_ACCOUNTING_DAY = 0
    STW_ACCOUNTING_DAY = 1
    TRIPLE_WITCHING_DAY = 2
    FINANCIAL_STATEMENT_DAY = 3
    KEY_DAY_SIZE = 4

    KEY_DAY_CONFIG_FILENAME_LIST = [
        CMN.DEF.TFEI_ACCOUNTING_DAY_CONF_FILENAME,
        CMN.DEF.STW_ACCOUNTING_DAY_CONF_FILENAME,
        CMN.DEF.TRIPLE_WITCHING_DAY_CONF_FILENAME,
        CMN.DEF.FINANCIAL_STATEMENT_DAY_CONF_FILENAME,
    ]
    KEY_DAY_FUNC_PTR = None


    @classmethod
    @CMN.FUNC.deprecated("This function is deprecated")
    def get_week_account_day(cls, cur_date=None):
        if cur_date is None:
            cur_date = datetime.today()
        elif isinstance(cur_date, CMN.CLS.FinanceDate):
            cur_date = datetime.datetime(cur_date.year, cur_date.month, cur_date.day)
# Wednesday
        weekday = cur_date.weekday()
        offset = 0
        if weekday > 2:
            offset += (7 - weekday) + 2
        else:
            offset += (2 - weekday)
        return (cur_date + timedelta(days=offset))


    @classmethod
    @CMN.FUNC.deprecated("This function is deprecated")
    def is_week_account_day(cls, cur_date=None):
        if cur_date is None:
            cur_date = datetime.today()
        elif isinstance(cur_date, CMN.CLS.FinanceDate):
            cur_date = datetime.datetime(cur_date.year, cur_date.month, cur_date.day)
# Wednesday
        return True if cur_date.weekday() == 2 else False


    @classmethod
    @CMN.FUNC.deprecated("This function is deprecated")
    def get_month_account_day(cls, cur_date=None):
        if cur_date is None:
            cur_date = datetime.today()
        elif isinstance(cur_date, CMN.CLS.FinanceDate):
            cur_date = datetime.datetime(cur_date.year, cur_date.month, 1)
        elif isinstance(cur_date, CMN.CLS.FinanceMonth):
            cur_date = datetime.datetime(cur_date.year, cur_date.month, 1)
# Find the first Wednesday of the month
        while cur_date.weekday() != 2:
            cur_date += timedelta(days = 1)
# Return the third Wednesday of the month
        return cur_date + timedelta(days = 14)


    @classmethod
    @CMN.FUNC.deprecated("This function is deprecated")
    def is_month_account_day(cls, cur_date=None):
        if cur_date is None:
            cur_date = datetime.today()
        elif isinstance(cur_date, CMN.CLS.FinanceDate):
            cur_date = datetime.datetime(cur_date.year, cur_date.month, cur_date.day)
# Wednesday
        month_account_day = get_month_account_day(cur_date)
        return True if cur_date == month_account_day else False


    @classmethod
    def __read_data_from_file(cls, data_filepath, data_dict):
        assert data_dict is not None, "data_dict should NOT be None"
        if not os.path.exists(data_filepath):
            errmsg = "The file[%s] does NOT exist" % data_filepath
            g_logger.error(errmsg)
            raise CMN.EXCEPTION.WebScrapyNotFoundException(errmsg) 
        # import pdb; pdb.set_trace()
        try:
            date_range_str = None
            with open(data_filepath, 'r') as fp:
                for line in fp:
                    if line.startswith('#'):
                        continue
                    line_strip = line.strip('\n').strip('\r')
                    if len(line_strip) == 0:
                        continue
                    if date_range_str is None:
                        date_range_str = line
                        mobj = re.search("([\d]{4}) ([\d]{4})", date_range_str)
                        if mobj is None:
                            raise RuntimeError("Unknown format[%s] of date range in Keyday Canlendar config file", line)
                        # time_start = CMN.CLS.FinanceDate(mobj.group(1))
                        # time_end = CMN.CLS.FinanceDate(mobj.group(2))
                        g_logger.debug("The file[%s] time range of the key days: %s, %s" % (data_filepath, mobj.group(1), mobj.group(2)))
                    else:
# Obtain the "year" value
                        mobj = re.search("\[([\d]{4})\](.*)", line.strip("\n"))
                        if mobj is None:
                            raise RuntimeError("Incorrect data format in file: %s", line)
                        year = int(mobj.group(1))
                        date_list = [map(int, entry.split(":")) for entry in mobj.group(2).split(";")]
                        # import pdb; pdb.set_trace()
                        data_dict[year] = date_list
        except Exception as e:
            g_logger.error("Error occur while parsing file[%s], due to %s" % (data_filepath, str(e)))
            raise e


    @classmethod
    def __read_keydays(cls, keyday_type):
        assert keyday_type >= 0 and keyday_type < cls.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, cls.KEY_DAY_SIZE)
        keyday_dict = OrderedDict()
        config_filename = cls.KEY_DAY_CONFIG_FILENAME_LIST[keyday_type]
        data_filepath = CMN.FUNC.get_config_filepath(CMN.DEF.TFEI_ACCOUNTING_DAY_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)
        cls.__read_data_from_file(data_filepath, keyday_dict)
        return keyday_dict


    @classmethod
    def __transform2finance_date(cls, *date_args):
        assert len(date_args) != 0, "The date_args should be NOT 0"
        finance_date = CMN.CLS.FinanceDate(*date_args) if not isinstance(date_args[0], CMN.CLS.FinanceDate) else date_args[0]
        return finance_date


    @classmethod
    def __get_date_value(cls, *month_day_args):
        assert len(month_day_args) == 2, "The month_day_args[%d] should be 2" % len(month_day_args)
        return month_day_args[0] << 5 + month_day_args[1]


    @classmethod
    def __compare_greater_equal_date(cls, compare_value, *month_day_args):
        return cls.__get_date_value(*month_day_args) >= compare_value


    @classmethod
    def __compare_less_equal_date(cls, compare_value, *month_day_args):
        return cls.__get_date_value(*month_day_args) <= compare_value


    def __init__(self):
        pass


    def initialize(self):
        self.keydays_list = [None,] * self.KEY_DAY_SIZE
        self.first_keyday_list = [None,] * self.KEY_DAY_SIZE
        self.last_keyday_list = [None,] * self.KEY_DAY_SIZE


    def get_keydays(self, keyday_type):
        assert keyday_type >= 0 and keyday_type < self.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, self.KEY_DAY_SIZE)
        if self.keydays_list[keyday_type] is None:
            self.keydays_list[keyday_type] = self.__read_keydays(keyday_type)
        return self.keydays_list[keyday_type]


    def find_pos(self, keyday_type, *date_cur_args):
        # import pdb; pdb.set_trace()
        assert keyday_type >= 0 and keyday_type < self.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, self.KEY_DAY_SIZE)
        date_cur = self.__transform2finance_date(*date_cur_args)
        keydays = self.get_keydays(keyday_type)
# Year
        try:
            keydays_year_list = keydays.keys()
            keydays_year_list.index(date_cur.year)
        except ValueError as e:
            msg = "The file[%s] does NOT exist" % data_filepath
            g_logger.debug(msg)
            return None
# Date
        keydays_in_year_list = keydays[date_cur.year]
        for date_index, date_entry in enumerate(keydays_in_year_list):
            if date_entry[0] == date_cur.month and date_entry[1] == date_cur.day:
                return date_cur.year, date_index    
        return None


    def get_first_keyday(self, keyday_type):
        assert keyday_type >= 0 and keyday_type < self.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, self.KEY_DAY_SIZE)
        if self.first_keyday_list[keyday_type] is None:
            keydays = self.get_keydays(keyday_type)
            year = keydays.keys()[0]
            [month, day] = keydays[year][0]
            self.first_keyday_list[keyday_type] = CMN.CLS.FinanceDate(year, month, day)
        return self.first_keyday_list[keyday_type]


    def get_last_keyday(self, keyday_type):
        assert keyday_type >= 0 and keyday_type < self.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, self.KEY_DAY_SIZE)
        if self.last_keyday_list[keyday_type] is None:
            keydays = self.get_keydays(keyday_type)
            year = keydays.keys()[-1]
            [month, day] = keydays[year][-1]
            self.last_keyday_list[keyday_type] = CMN.CLS.FinanceDate(year, month, day)
        return self.last_keyday_list[keyday_type]


    def find_nearest_next_pos(self, keyday_type, *date_cur_args):
        assert keyday_type >= 0 and keyday_type < self.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, self.KEY_DAY_SIZE)
        date_cur = self.__transform2finance_date(*date_cur_args)
        keydays = self.get_keydays(keyday_type)
# Check the boundary
        if date_cur > self.get_last_keyday(keyday_type):
            return None
        elif date_cur <= self.get_first_keyday(keyday_type):
            return date_cur.year, 0
        keydays_in_year = keydays[date_cur.year]
        compare_value = self.__get_date_value(date_cur.month, date_cur.day)
        compare_index_list = [entry_index for entry_index, entry in enumerate(keydays_in_year) if self.__compare_less_equal_date(entry, compare_value)]
        if len(compare_index_list) == 0:
            new_year = date_cur.year + 1
            return new_year, 0
        return date_cur.year, compare_index_list[0]


    def get_nearest_next_keyday(self, keyday_type, *date_cur_args):
        pos = self.find_nearest_next_pos(keyday_type, *date_cur_args)
        if pos is None:
            return None
        [year, date_index] = pos
        keydays = self.get_keydays(keyday_type)
        keydays_in_year = keydays[year]
        [month, day] = keydays_in_year[date_index]
        return CMN.CLS.FinanceDate(year, month, day)


    def find_nearest_prev_pos(self, keyday_type, *date_cur_args):
        assert keyday_type >= 0 and keyday_type < self.KEY_DAY_SIZE, "keyday_type[%d] should be in range [0, %d)" % (keyday_type, self.KEY_DAY_SIZE)
        date_cur = self.__transform2finance_date(*date_cur_args)
        keydays = self.get_keydays(keyday_type)
# Check the boundary
        if date_cur < self.get_first_keyday(keyday_type):
            return None
        elif date_cur >= self.get_last_keyday(keyday_type):
            return date_cur.year, (len(keydays[date_cur.year]) - 1)
        keydays_in_year = keydays[date_cur.year]
        compare_value = self.__get_date_value(date_cur.month, date_cur.day)
        compare_index_list = [entry_index for entry_index, entry in enumerate(keydays_in_year) if self.__compare_greater_equal_date(entry, compare_value)]
        if len(compare_index_list) == 0:
            new_year = date_cur.year - 1
            return new_year, (len(keydays[new_year]) - 1)
        return date_cur.year, compare_index_list[-1]


    def get_nearest_prev_keyday(self, keyday_type, *date_cur_args):
        pos = self.find_nearest_prev_pos(keyday_type, *date_cur_args)
        if pos is None:
            return None
        [year, date_index] = pos
        keydays = self.get_keydays(keyday_type)
        keydays_in_year = keydays[year]
        [month, day] = keydays_in_year[date_index]
        return CMN.CLS.FinanceDate(year, month, day)

####################################################################################################

class KeydayIterator(object):

    def __init__(self, keyday_type, date_start=None, date_end=None, ExceptionWhenStartLaterThanEnd=False):
        # import pdb; pdb.set_trace()
        self.keyday_type = keyday_type
        self.keyday_canlendar_obj = KeydayCalendar.Instance()
        self.keydays = self.keyday_canlendar_obj.get_keydays(self.keyday_type)
        self.keydays_year_list = self.keydays.keys()
        # self.keydays_year_list = self.keyday_canlendar_obj.keydays_year_list
        self.time_to_stop = False
        self.cur_year = None
        self.cur_date_index = None
        self.end_year = None
        self.end_date_index = None

        if date_start is None:
            date_start = self.keyday_canlendar_obj.get_first_keyday(self.keyday_type)
        assert (isinstance(date_start, CMN.CLS.FinanceDate)), "The type of start time[%s] is NOT FinanceDate" % date_start
        if date_end is None:
            date_end = self.keyday_canlendar_obj.get_last_keyday(self.keyday_type)
        assert (isinstance(date_end, CMN.CLS.FinanceDate)), "The type of end time[%s] is NOT FinanceDate" % date_end
# Check the time range
        if date_start > date_end:
            msg = "The start date[%s] is later than the end date[%s]" % (date_start, date_end)
            if ExceptionWhenStartLaterThanEnd:
                raise ValueError(msg)
            else:
                g_logger.debug(msg)
            self.time_to_stop = True
        else:
# start pos
            start_pos = self.keyday_canlendar_obj.find_pos(self.keyday_type, date_start)
            if start_pos is None:
                raise ValueError("Fails to find the pos of the start date[%s] in %d" % (date_start, self.keyday_type))
            (start_year, start_date_index) = start_pos
            g_logger.debug("The start date[%s] pos: %d %d" % (date_start, start_year, start_date_index))
# end pos
            end_pos = self.keyday_canlendar_obj.find_pos(self.keyday_type, date_end)
            if end_pos is None:
                raise ValueError("Fails to find the pos of the end date[%s] in %d" % (date_end, self.keyday_type))
            (self.end_year, self.end_date_index) = end_pos
            g_logger.debug("The end date[%s] pos: %d %d" % (date_end, self.end_year, self.end_date_index))
            self.cur_year = start_year
            self.cur_date_index = start_date_index


    def __iter__(self):
        return self


    def next(self):
        # import pdb; pdb.set_trace()
        if self.time_to_stop:
            raise StopIteration
        if self.cur_year == self.end_year and self.cur_date_index == self.end_date_index:
            self.time_to_stop = True
        keydays_in_year = self.keydays[self.cur_year]
        [cur_month, cur_day] = keydays_in_year[self.cur_date_index]
        date_cur = CMN.CLS.FinanceDate(self.cur_year, cur_month, cur_day)
# Find the next element
        if self.cur_date_index == len(keydays_in_year) - 1:
# A new year
            self.cur_year += 1
            self.cur_date_index = 0
        else:
            self.cur_date_index += 1         
        return date_cur

####################################################################################################

class KeydayNearestIterator(KeydayIterator):

    def __init__(self, keyday_type, date_start=None, date_end=None, ExceptionWhenStartLaterThanEnd=False):
        keyday_canlendar_obj = KeydayCalendar.Instance()
        # import pdb;pdb.set_trace()
        date_nearest_start = keyday_canlendar_obj.get_nearest_next_keyday(keyday_type, date_start) if date_start is not None else None
        date_nearest_end = keyday_canlendar_obj.get_nearest_prev_keyday(keyday_type, date_end) if date_end is not None else None
        super(KeydayNearestIterator, self).__init__(keyday_type, date_nearest_start, date_nearest_end, ExceptionWhenStartLaterThanEnd)
