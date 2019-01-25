# -*- coding: utf8 -*-

import os
import sys
import re
import requests
import json
from bs4 import BeautifulSoup
import scrapy.common as CMN
from scrapy.common.common_variable import GlobalVar as GV
g_logger = CMN.LOG.get_logger()


@CMN.CLS.Singleton
class WorkdayCanlendar(object):

    # WORKDAY_CANLENDAR_CONSTANT_CFG = CMN.DEF.SCRAPY_METHOD_CONSTANT_CFG[CMN.DEF.SCRAPY_WORKDAY_CANLENDAR_INDEX]
    URL_FORMAT = "http://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={0}{1:02d}01"
    URL_ENCODING = CMN.DEF.URL_ENCODING_UTF8
    URL_DATA_SELECTOR = 'data'

    def __init__(self, date_start=None, date_end=None):
# Caution: The types of the following member variabiles are FinanceDate
        self.date_start = CMN.CLS.FinanceDate(CMN.DEF.WORKDAY_CANLENDAR_START_DATE_STR) if date_start is None else date_start 
        self.date_end = CMN.CLS.FinanceDate.get_last_finance_date()
        # self.date_end = CMN.FUNC.get_last_url_data_date(self.TODAY_WORKDAY_DATA_EXIST_HOUR, self.TODAY_WORKDAY_DATA_EXIST_MINUTE)
# The start/end time of scraping data from the web
        self.date_start_from_web = self.date_start
        self.date_end_from_web = self.date_end
# The first/last workday
        self.date_first = None
        self.date_last = None
# The first/last month of workday
        self.month_of_date_first = None
        self.month_of_date_last = None
###############################################################################################
        self.update_from_web_flag = False # Should be updated for just one time
        self.date_start_year = self.date_start.year
        self.workday_canlendar = None
        self.workday_year_list = None


    def initialize(self):
        # import pdb; pdb.set_trace()
        self.update_workday_canlendar()


    def is_workday(self, date_cur):
        return self.__is_workday(date_cur.year, date_cur.month, date_cur.day)


    def __is_workday(self, year, month, day):
        if self.workday_canlendar is None:
            raise RuntimeError("Workday Canlendar is NOT initialized")
        date_check = CMN.CLS.FinanceDate(year, month, day)
        if not self.is_in_range(date_check):
            raise RuntimeError("The check date[%s] is OUT OF RANGE[%s %s]" % (date_check, self.date_start, self.date_end))
        return day in self.workday_canlendar[year][month - 1]


    def update_workday_canlendar(self):
        # import pdb; pdb.set_trace()
# Update data from the file
        need_update_from_web = self.__update_workday_from_file()
# It's required to update the new data
        if need_update_from_web:
# Update data from the web
            self.__update_workday_from_web()
# Write the result into the config file
            self.__write_workday_canlendar_to_file()
# # Copy the config file to the finance_analyzer/finance_recorder_java project
#             self.__copy_workday_canlendar_config_file()


    def __update_workday_from_file(self):
        need_update_from_web = True
        conf_filepath = CMN.FUNC.get_config_filepath(CMN.DEF.WORKDAY_CANLENDAR_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)
        g_logger.debug("Try to Acquire the Workday Canlendar data from the file: %s......" % conf_filepath)
        if not os.path.exists(conf_filepath):
            g_logger.warn("The Workday Canlendar config file does NOT exist")
            return need_update_from_web
        try:
            date_range_str = None
            with open(conf_filepath, 'r') as fp:
                for line in fp:
                    if date_range_str is None:
                        date_range_str = line
                        mobj = re.search("([\d]{4}-[\d]{2}-[\d]{2}) ([\d]{4}-[\d]{2}-[\d]{2})", date_range_str)
                        if mobj is None:
                            raise RuntimeError("Unknown format[%s] of date range in Workday Canlendar config file", line)
                        date_start_from_file = CMN.CLS.FinanceDate(mobj.group(1))
                        date_end_from_file = CMN.CLS.FinanceDate(mobj.group(2))
                        g_logger.debug("Original Date Range in Workday Canlendar config file: %s, %s" % (mobj.group(1), mobj.group(2)))
                        if date_start_from_file > self.date_start:
                            raise RuntimeError("Incorrect start date; File: %s, Expected: %s", date_start_from_file.to_string(), self.date_start.to_string())
# Check the time range in the file and adjust the range of scraping data from the web
                        if date_end_from_file >= self.date_end:
                            g_logger.debug("The last data has already existed in the file. It's no need to scrape data from the web")
                            need_update_from_web = False
                        else:
                            self.date_start_from_web = date_end_from_file + 1
                            g_logger.debug("Adjust the time range of web scrapy to %s %s" % (self.date_start_from_web, self.date_end_from_web))
                    else:
# Obtain the "year" value
                        mobj = re.search("\[([\d]{4})\]", line)
                        if mobj is None:
                            raise RuntimeError("Incorrect year format in Workday Canlendar config file: %s", line)
                        year = int(mobj.group(1))
                        # self.workday_canlendar[year] = self.__init_canlendar_each_year_list()
                        tmp_data_list = line.split(']')
                        if len(tmp_data_list) != 2:
                            raise RuntimeError("Incorrect data format in Workday Canlendar config file: %s", line)
# Parse the workday in each month
                        year_workday_list = tmp_data_list[1].rstrip("\n").split(";")
                        for year_workday_list_per_month in year_workday_list:
                            tmp_data1_list = year_workday_list_per_month.split(':')
                            if len(tmp_data1_list) != 2:
                                raise RuntimeError("Incorrect per month data format in Workday Canlendar config file: %s", line)
                            month = int(tmp_data1_list[0])
                            workday_list_str = tmp_data1_list[1]
                            workday_list = [int(workday_str) for workday_str in workday_list_str.split(",")]
# Check if there are nay duplicate elements in the list
                            if len(workday_list) != len(set(workday_list)):
                                # import pdb; pdb.set_trace()
                                raise RuntimeError("Duplicate element in [%d, %d]: %s", year, month, workday_list)
                            self.__set_canlendar_each_month(year, month, workday_list)
        except Exception as e:
            g_logger.error("Error occur while parsing Workday Canlendar config, due to %s" % str(e))
            raise e

        self.workday_year_list = sorted(self.workday_canlendar.keys())
        return need_update_from_web


    def __update_workday_from_web(self):
        if not self.update_from_web_flag:
            self.update_from_web_flag = True
        else:
            raise RuntimeError("Update workday from web should NOT be called more than once")
        # import pdb; pdb.set_trace()
        g_logger.debug("Try to Acquire the Workday Canlendar [%s:%s] data from the web......" % (self.date_start_from_web, self.date_end_from_web))
        if self.date_start_from_web.year == self.date_end_from_web.year and self.date_start_from_web.month == self.date_end_from_web.month:
            self.__update_workday_from_web_by_month(self.date_start_from_web.year, self.date_start_from_web.month, self.date_start_from_web.day, self.date_end_from_web.day)
        else:
            self.__update_workday_from_web_by_month(self.date_start_from_web.year, self.date_start_from_web.month, start_day=self.date_start_from_web.day)
            year = self.date_start_from_web.year
            month = self.date_start_from_web.month
            year_end = self.date_end_from_web.year
            month_end = self.date_end_from_web.month
            def get_next_month(cur_year, cur_month):
                next_year = cur_year + 1 if cur_month == 12 else cur_year
                next_month = 1 if cur_month == 12 else cur_month + 1
                return (next_year, next_month)
            while True:
                (year, month) = get_next_month(year, month)
                if year == year_end and month == month_end:
                    break
                self.__update_workday_from_web_by_month(year, month)
            self.__update_workday_from_web_by_month(self.date_end_from_web.year, self.date_end_from_web.month, end_day=self.date_end_from_web.day)
        self.workday_year_list = sorted(self.workday_canlendar.keys())
        # for year in self.workday_year_list:
        #     workday_month_list = self.workday_canlendar[year]
        #     print "%d: %s" % (year, workday_month_list)


    def __update_workday_from_web_by_month(self, year, month, start_day=None, end_day=None):
# Check if scraping the whole month data
        # import pdb; pdb.set_trace()
        whole_month_data = False
#***************************************************
# # Check if the whole month data is required
#         if start_day is None and end_day is None:
#             whole_month_data = True
#         elif end_day is None and start_day == 1:
#             whole_month_data = True
#         elif start_day is None and end_day == CMN.FUNC.get_month_last_day(year, month):
#             whole_month_data = True
# # Assemble the URL
#         url = self.url_format.format(*(year, month))
#***************************************************
# Setup the start/end date if it's None
        if start_day is None: start_day = 1
        if end_day is None: end_day = CMN.FUNC.get_month_last_day(year, month)
        if start_day == 1 and end_day == CMN.FUNC.get_month_last_day(year, month):
            whole_month_data = True
        url = self.URL_FORMAT.format(*(year, month, start_day, end_day))
# Scrap the web data
        req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
# Select the section we are interested in
        req.encoding = self.URL_ENCODING
#         # print res.text
        web_data = json.loads(req.text)[self.URL_DATA_SELECTOR]
        workday_list = []
        for entry in web_data:
            date_list = str(entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            cur_day = int(date_list[2])
            if not whole_month_data and cur_day < start_day:
                continue
            if not whole_month_data and cur_day > end_day:
                break
            workday_list.append(cur_day)
#         soup = BeautifulSoup(req.text)
#         web_data = soup.select(self.select_flag)
#         workday_list = []
#         web_data_len = len(web_data)
# # Parse the web data and obtain the workday list
#         if web_data_len != 0:
#         # print "len: %d" % data_len
# #***************************************************
#             # for tr in web_data[2:]:
# #***************************************************
#             for tr in web_data[web_data_len - 1 : 0 : -1]:
#                 td = tr.select('td')
#                 date_list = td[0].text.split('/')
#                 if len(date_list) != 3:
#                     raise RuntimeError("The date format is NOT as expected: %s", date_list)
#                 cur_day = int(date_list[2])
#                 if not whole_month_data and cur_day < start_day:
# # Caution: It's NO need to consider the end date since the last data is always today
#                     continue
#                 workday_list.append(cur_day)
        # import pdb; pdb.set_trace()
        self.__set_canlendar_each_month(year, month, workday_list)
# # Find the workday list
#         allday_list = []
#         if whole_month_data:
#             allday_list = range(1, CMN.get_month_last_day(year, month) + 1)
#         else:        
#             list_start_date = 1 if start_day is None else start_day
#             list_end_date = CMN.get_month_last_day(year, month) if end_day is None else end_day
#             allday_list = range(list_start_date, list_end_date + 1)
# # Get the non-workday list and set to the data structure
#         non_workday_list = list(set(allday_list) - set(workday_list))
#         self.__set_canlendar_each_month(year, month, non_workday_list)


    def __set_canlendar_each_month(self, year, month, workday_list):
        if not workday_list:
            return
        if self.workday_canlendar is None:
            self.workday_canlendar = {}
        if year not in self.workday_canlendar:
            self.workday_canlendar[year] = self.__init_canlendar_each_year_list()
        self.workday_canlendar[year][month - 1].extend(workday_list)
        self.workday_canlendar[year][month - 1].sort()


    def __init_canlendar_each_year_list(self):
        workday_canlendar_year_list = []
        for index in range(12):
            workday_canlendar_year_list.append([])
        return workday_canlendar_year_list


    def __write_workday_canlendar_to_file(self):
        # import pdb; pdb.set_trace()
        conf_filepath = CMN.FUNC.get_config_filepath(CMN.DEF.WORKDAY_CANLENDAR_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)
        g_logger.debug("Write the Workday Canlendar data to the file: %s......" % conf_filepath)
        try:
            date_range_str = None
            with open(conf_filepath, 'w') as fp:
                g_logger.debug("Start to write workday into %s", CMN.DEF.WORKDAY_CANLENDAR_CONF_FILENAME)
                fp.write("%s %s\n" % (self.date_start, self.date_end))
                # year_list = sorted(self.workday_canlendar)
                for year in self.workday_year_list:
                    workday_month_list = self.workday_canlendar[year]
                    # print "%d: %s" % (year, workday_month_list)
                    # sys.stderr.write("%s" % ("[%04d]" % year + ";".join(["%d:%s" % (month + 1, ",".join([str(workday) for workday in workday_month_list[month]])) for month in range(12)]) + "\n"))
                    workday_each_year_str = "[%04d]" % year + ";".join(["%d:%s" % (month + 1, ",".join([str(workday) for workday in workday_month_list[month]])) for month in range(12) if len(workday_month_list[month]) != 0]) + "\n"
                    # for month in range(12):
                    #     workday_each_year_str += "%d:%s;" % (month + 1, ",".join([str(workday) for workday in workday_month_list[month]]))
                    # workday_each_year_str += "\n"
                    g_logger.debug("Write data: %s" % workday_each_year_str)
                    fp.write(workday_each_year_str)
                    # print workday_each_year_str
        except Exception as e:
            g_logger.error("Error occur while writing Workday Canlendar into config file, due to %s" % str(e))
            raise e


    def get_first_workday(self):
        if self.date_first is None:
            if self.workday_canlendar is None:
                raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
            year = self.workday_year_list[0]
            for month in range(12):
                if len(self.workday_canlendar[year][month]) != 0:
                    self.date_first = CMN.CLS.FinanceDate(year, month + 1, self.workday_canlendar[year][month][0]) 
                    break
            if self.date_first is None:
                raise RuntimeError("Fail to find the frst workday in the canlendar") 
        return self.date_first


    def get_first_month_of_workday(self):
        if self.month_of_date_first is None:
            self.month_of_date_first = CMN.CLS.FinanceMonth.get_finance_month_from_date(self.get_first_workday())
        return self.month_of_date_first


    def get_last_workday(self):
        if self.date_last is None:
            if self.workday_canlendar is None:
                raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
            # import pdb; pdb.set_trace()
            # workday_year_list = sorted(self.workday_canlendar.keys())
            year = self.workday_year_list[-1]
            for month in range(12):
                if len(self.workday_canlendar[year][month]) == 0:
                    self.date_last = CMN.CLS.FinanceDate(year, month, self.workday_canlendar[year][month - 1][-1]) 
                    break
            if self.date_last is None:
                month = 12
                self.date_last = CMN.CLS.FinanceDate(year, month, self.workday_canlendar[year][month - 1][-1]) 
        return self.date_last


    def get_last_month_of_workday(self):
        if self.month_of_date_last is None:
            self.month_of_date_last = CMN.CLS.FinanceMonth.get_finance_month_from_date(self.get_last_workday())
        return self.month_of_date_last


    def is_in_range(self, date_cur):
        date_first = self.get_first_workday()
        date_last = self.get_last_workday()
        result = (self.get_first_workday() <= date_cur <= self.get_last_workday())
        if not result:
            g_logger.debug("Date[%s] is NOT in range[%s, %s]" % (date_cur, self.get_first_workday(), self.get_last_workday()))
        return result


    def find_index(self, date_cur):
        # import pdb; pdb.set_trace()
        if self.workday_canlendar is None:
            raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
        year_index = None
        month_index = None
        day_index = None
        try:
            year_index = self.workday_year_list.index(date_cur.year)
        except ValueError as e:
            return None
        year = self.workday_year_list[year_index]
        if len(self.workday_canlendar[year][date_cur.month - 1]) == 0:
            return None
        month_index = date_cur.month - 1
        try:
            day_index = self.workday_canlendar[year][month_index].index(date_cur.day)
        except ValueError as e:
            return None

        # g_logger.debug("The date[%s] index: %d %d %d" % (CMN.to_date_only_str(date_cur), year_index, month_index, day_index))
        return (year_index, month_index, day_index)


    def find_nearest_next_index(self, date_cur):
        if self.workday_canlendar is None:
            raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
        year_index = None
        month_index = None
        day_index = None
        is_workday = False
        if not self.is_in_range(date_cur):
            if date_cur > self.get_last_workday():
                return None
            else:
                date_cur = self.get_first_workday()
                is_workday = True
        else:
            is_workday = self.is_workday(date_cur)

        if is_workday:
            (year_index, month_index, day_index) = self.find_index(date_cur)
        else:
            if not self.is_in_range(date_cur):
                return None
            try:
                year_index = self.workday_year_list.index(date_cur.year)
            except ValueError as e:
                return None
            # year = self.workday_year_list[year_index]
            month_index = date_cur.month - 1
            day_list = self.workday_canlendar[date_cur.year][month_index]
            for index, day in enumerate(day_list):
                if day > date_cur.day:
                    day_index = index
                    break
            if day_index is None:
                if month_index == 11:
# A new year
                    year_index += 1
                    month_index = 0
                    day_index = 0
                else:
# A new month
                    month_index += 1
                    day_index = 0
        # g_logger.debug("The date[%s] index: %d %d %d" % (date_cur, year_index, month_index, day_index))
        return (year_index, month_index, day_index)


    def get_nearest_next_workday(self, date_cur):
        index_tuple = self.find_nearest_next_index(date_cur)
        if index_tuple is None:
            raise RuntimeError("Fail to find the nearest next workday from the date: %s" % date_cur)
        (year_index, month_index, day_index) = index_tuple
        year = self.workday_year_list[year_index]
        date_nearest_next = CMN.CLS.FinanceDate(year, month_index + 1, self.workday_canlendar[year][month_index][day_index])
        if not self.is_workday(date_cur):
            g_logger.debug("Find the nearest next workday[%s]: %s" % (date_cur, date_nearest_next))
        return date_nearest_next


    def find_nearest_prev_index(self, date_cur):
        if self.workday_canlendar is None:
            raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
        year_index = None
        month_index = None
        day_index = None
        is_workday = False
        if not self.is_in_range(date_cur):
            if date_cur < self.get_first_workday():
                return None
            else:
                date_cur = self.get_last_workday()
                is_workday = True
        else:
            is_workday = self.is_workday(date_cur)

        if is_workday:
            (year_index, month_index, day_index) = self.find_index(date_cur)
        else:
            try:
                year_index = self.workday_year_list.index(date_cur.year)
            except ValueError as e:
                return None
            # year = self.workday_year_list[year_index]
            month_index = date_cur.month - 1
            # import pdb; pdb.set_trace()
            day_list = self.workday_canlendar[date_cur.year][month_index]
            for index, day in reversed(list(enumerate(day_list))):
                if day < date_cur.day:
                    day_index = index
                    break
            if day_index is None:
                if month_index == 1:
# An old year
                    year_index -= 1
                    month_index = 11
                    day_index = len(self.workday_canlendar[date_cur.year - 1][month_index]) - 1
                else:
# An old month
                    month_index -= 1
                    day_index = len(self.workday_canlendar[date_cur.year - 1][month_index]) - 1
        # g_logger.debug("The date[%s] index: %d %d %d" % (CMN.to_date_only_str(date_cur), year_index, month_index, day_index))
        return (year_index, month_index, day_index)


    def get_nearest_prev_workday(self, date_cur):
        # import pdb; pdb.set_trace()
        index_tuple = self.find_nearest_prev_index(date_cur)
        if index_tuple is None:
            raise RuntimeError("Fail to find the nearest previous workday from the date: %s" % date_cur)
        (year_index, month_index, day_index) = index_tuple
        year = self.workday_year_list[year_index]
        date_nearest_prev = CMN.CLS.FinanceDate(year, month_index + 1, self.workday_canlendar[year][month_index][day_index])
        if not self.is_workday(date_cur):
            g_logger.debug("Find the nearest previous workday[%s]: %s" % (date_cur, date_nearest_prev))
        return date_nearest_prev


    @property
    def FirstWorkday(self):
        return self.get_first_workday()


    @property
    def FirstMonthOfWorkday(self):
        return self.get_first_month_of_workday()


    @property
    def LastWorkday(self):
        return self.get_last_workday()


    @property
    def LastMonthOfWorkday(self):
        return self.get_last_month_of_workday()


####################################################################################################

class WorkdayIterator(object):

    def __init__(self, date_start=None, date_end=None, ExceptionWhenStartLaterThanEnd=False):
        self.workday_canlendar_obj = WorkdayCanlendar.Instance()
        self.workday_canlendar = self.workday_canlendar_obj.workday_canlendar
        self.workday_year_list = self.workday_canlendar_obj.workday_year_list

        if date_start is None:
            date_start = self.workday_canlendar_obj.get_first_workday()
        assert (isinstance(date_start, CMN.CLS.FinanceDate)), "The type of start time[%s] is NOT FinanceDate" % date_start
        if date_end is None:
            date_end = self.workday_canlendar_obj.get_last_workday()
        assert (isinstance(date_end, CMN.CLS.FinanceDate)), "The type of end time[%s] is NOT FinanceDate" % date_end

        self.time_to_stop = False
        self.cur_year_index = None
        self.cur_month_index = None
        self.cur_day_index = None
        if date_start > date_end:
            if ExceptionWhenStartLaterThanEnd:
                raise ValueError("The start date[%s] should NOT be later than the end date[%s]" % (date_start, date_end))
            else:
                g_logger.debug("The start date[%s] is later than the end date[%s]" % (date_start, date_end))
            self.time_to_stop = True
        else:
            start_tuple = self.workday_canlendar_obj.find_index(date_start)
            if start_tuple is None:
                raise ValueError("The start date[%s] is NOT a workday" % date_start)
            (start_year_index, start_month_index, start_day_index) = start_tuple
            g_logger.debug("The start date[%s] index: %d %d %d" % (date_start, start_year_index, start_month_index, start_day_index))
            end_tuple = self.workday_canlendar_obj.find_index(date_end)
            if end_tuple is None:
                raise ValueError("The end date[%s] is NOT a workday" % date_end)
            (self.end_year_index, self.end_month_index, self.end_day_index) = end_tuple
            g_logger.debug("The end date[%s] index: %d %d %d" % (date_end, self.end_year_index, self.end_month_index, self.end_day_index))
            self.cur_year_index = start_year_index
            self.cur_month_index = start_month_index
            self.cur_day_index = start_day_index


    def __iter__(self):
        return self


    def next(self):
        # import pdb; pdb.set_trace()
        if self.time_to_stop:
            raise StopIteration
        if self.cur_year_index == self.end_year_index and self.cur_month_index == self.end_month_index and self.cur_day_index == self.end_day_index:
            self.time_to_stop = True
        year = self.workday_year_list[self.cur_year_index]
        date_cur = CMN.CLS.FinanceDate(year, self.cur_month_index + 1, self.workday_canlendar[year][self.cur_month_index][self.cur_day_index])
# Find the next element
        if self.cur_month_index == 11:
            if self.cur_day_index == len(self.workday_canlendar[year][self.cur_month_index]) - 1:
# A new year
                self.cur_year_index += 1
                self.cur_month_index = 0
                self.cur_day_index = 0
            else:
                self.cur_day_index += 1
        else:
            if self.cur_day_index == len(self.workday_canlendar[year][self.cur_month_index]) - 1:
# A new month
                self.cur_month_index += 1
                self.cur_day_index = 0
            else:
                self.cur_day_index += 1            
        return date_cur

####################################################################################################

class WorkdayNearestIterator(WorkdayIterator):

    def __init__(self, date_start=None, date_end=None, ExceptionWhenStartLaterThanEnd=False):
        # import pdb;pdb.set_trace()
        workday_canlendar_obj = WorkdayCanlendar.Instance()
        date_nearest_start = workday_canlendar_obj.get_nearest_next_workday(date_start) if date_start is not None else None
        date_nearest_end = workday_canlendar_obj.get_nearest_prev_workday(date_end) if date_end is not None else None
        super(WorkdayNearestIterator, self).__init__(date_nearest_start, date_nearest_end, ExceptionWhenStartLaterThanEnd)
