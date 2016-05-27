# -*- coding: utf8 -*-

import os
import sys
import re
import requests
import csv
import shutil
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


@CMN_CLS.Singleton
class WebScrapyWorkdayCanlendar(object):

    def __init__(self, datetime_start=None, datetime_end=None):
        self.url_format = "http://www.twse.com.tw/ch/trading/exchange/FMTQIK/genpage/Report{0}{1:02d}/{0}{1:02d}_F3_1_2.php?STK_NO=&myear={0}&mmon={1:02d}"
        self.encoding = "big5"
        self.select_flag = ".board_trad tr"

        self.datetime_start = datetime_start if datetime_start is not None else CMN.transform_string2datetime(CMN.DEF_WEB_SCRAPY_BEGIN_DATE_STR)
        self.datetime_end = None
        if datetime_end is None:
            datetime_now = datetime.today()
            datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
            datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, CMN.DEF_TODAY_DATA_EXIST_HOUR, CMN.DEF_TODAY_DATA_EXIST_MINUTE)
            if datetime_now >= datetime_threshold:
                self.datetime_end = datetime_today
            else:
                self.datetime_end = datetime_today + timedelta(days = -1)
        else:
            self.datetime_end = datetime_end
# The start/end time of scrapying data from the web
        self.datetime_start_from_web = self.datetime_start
        self.datetime_end_from_web = self.datetime_end

        self.datetime_start_year = self.datetime_start.year
        self.workday_canlendar = None
        self.first_workday_cfg = None
        self.latest_workday_cfg = None
        self.workday_year_list = None


    def initialize(self):
        # import pdb; pdb.set_trace()
        self.update_workday_canlendar()


    def is_workday(self, datetime_cfg):
        return self.__is_workday(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)


    def __is_workday(self, year, month, day):
        if self.workday_canlendar is None:
            raise RuntimeError("Workday Canlendar is NOT initialized")
        datetime_check = datetime(year, month, day)
        if not self.is_in_range(datetime_check):
            raise RuntimeError("The check date[%s] is OUT OF RANGE[%s %s]" % (datetime_check, self.datetime_start, self.datetime_end))
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
# Copy the config file to the finance_analyzer/finance_recorder_java project
            self.__copy_workday_canlendar_config_file()


    def __copy_workday_canlendar_config_file(self):
        current_path = os.path.dirname(os.path.realpath(__file__))
        [working_folder, project_name, lib_folder] = current_path.rsplit('/', 2)
        dst_folderpath_list = [
            "%s/%s/%s" % (working_folder, CMN.DEF_COPY_CONF_FILE_DST_PROJECT_NAME1, CMN.DEF_CONF_FOLDER),
            "%s/%s/%s" % (working_folder, CMN.DEF_COPY_CONF_FILE_DST_PROJECT_NAME2, CMN.DEF_CONF_FOLDER),
        ]
        src_filepath = "%s/%s/%s/%s" % (working_folder, project_name, CMN.DEF_CONF_FOLDER, CMN.DEF_WORKDAY_CANLENDAR_CONF_FILENAME)
        for dst_folderpath in dst_folderpath_list:
            if os.path.exists(dst_folderpath):
                g_logger.debug("Copy the file[%s] to %s" % (CMN.DEF_WORKDAY_CANLENDAR_CONF_FILENAME, dst_folderpath))
                shutil.copy2(src_filepath, dst_folderpath)


    def __update_workday_from_file(self):
        need_update_from_web = True
        current_path = os.path.dirname(os.path.realpath(__file__))
        [project_folder, lib_folder] = current_path.rsplit('/', 1)
        conf_filepath = "%s/%s/%s" % (project_folder, CMN.DEF_CONF_FOLDER, CMN.DEF_WORKDAY_CANLENDAR_CONF_FILENAME)
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
                        datetime_start_from_file = CMN.transform_string2datetime(mobj.group(1))
                        datetime_end_from_file = CMN.transform_string2datetime(mobj.group(2))
                        g_logger.debug("Origina\l Date Range in Workday Canlendar config file: %s, %s" % (mobj.group(1), mobj.group(2)))
                        if datetime_start_from_file > self.datetime_start:
                            raise RuntimeError("Incorrect start date; File: %s, Expected: %s", datetime_start_from_file, self.datetime_start)
# Check the time range in the file and adjust the range of scrapying data from the web
                        if datetime_end_from_file >= self.datetime_end:
                            g_logger.debug("The latest data has already existed in the file. It's no need to scrap data from the web")
                            need_update_from_web = False
                        else:
                            self.datetime_start_from_web = datetime_end_from_file + timedelta(days = 1)
                            g_logger.debug("Adjust the time range of web scrapy to %04d-%02d-%02d %04d-%02d-%02d" % (self.datetime_start_from_web.year, self.datetime_start_from_web.month, self.datetime_start_from_web.day, self.datetime_end_from_web.year, self.datetime_end_from_web.month, self.datetime_end_from_web.day))
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
        # import pdb; pdb.set_trace()
        g_logger.debug("Try to Acquire the Workday Canlendar data from the web......")
        if self.datetime_start_from_web.year == self.datetime_end_from_web.year and self.datetime_start_from_web.month == self.datetime_end_from_web.month:
            self.__update_workday_from_web_by_month(self.datetime_start_from_web.year, self.datetime_start_from_web.month, self.datetime_start_from_web.day, self.datetime_end_from_web.day)
        else:
            self.__update_workday_from_web_by_month(self.datetime_start_from_web.year, self.datetime_start_from_web.month, start_day=self.datetime_start_from_web.day)
            year = self.datetime_start_from_web.year
            month = self.datetime_start_from_web.month
            year_end = self.datetime_end_from_web.year
            month_end = self.datetime_end_from_web.month
            def get_next_month(cur_year, cur_month):
                next_year = cur_year + 1 if cur_month == 12 else cur_year
                next_month = 1 if cur_month == 12 else cur_month + 1
                return (next_year, next_month)
            while True:
                (year, month) = get_next_month(year, month)
                if year == year_end and month == month_end:
                    break
                self.__update_workday_from_web_by_month(year, month)
            self.__update_workday_from_web_by_month(self.datetime_end_from_web.year, self.datetime_end_from_web.month, end_day=self.datetime_end_from_web.day)
        self.workday_year_list = sorted(self.workday_canlendar.keys())


    def __update_workday_from_web_by_month(self, year, month, start_day=None, end_day=None):
# Check if scrapying the whole month data 
        whole_month_data = False
        if start_day is None and end_day is None:
            whole_month_data = True
        elif end_day is None and start_day == 1:
            whole_month_data = True
        elif start_day is None and end_day == CMN.get_month_last_day(year, month):
            whole_month_data = True

# Assemble the URL
        url = self.url_format.format(*(year, month))
# Scrap the web data
        try:
            # g_logger.debug("Try to Scrap data [%s]" % url)
            res = requests.get(url, timeout=CMN.DEF_SCRAPY_WAIT_TIMEOUT)
        except requests.exceptions.Timeout as e:
            # g_logger.debug("Try to Scrap data [%s]... Timeout" % url)
            fail_to_scrap = False
            for index in range(self.SCRAPY_RETRY_TIMES):
                time.sleep(randint(1,3))
                try:
                    res = requests.get(url, timeout=CMN.DEF_SCRAPY_WAIT_TIMEOUT)
                except requests.exceptions.Timeout as ex:
                    fail_to_scrap = True
                if not fail_to_scrap:
                    break
            if fail_to_scrap:
                g_logger.error("Fail to scrap workday list data even retry for %d times !!!!!!" % self.SCRAPY_RETRY_TIMES)
                raise e
# Select the section we are interested in
        res.encoding = self.encoding
        # print res.text
        soup = BeautifulSoup(res.text)
        web_data = soup.select(self.select_flag)
        workday_list = []
# Parse the web data and obtain the workday list
        if len(web_data) != 0:
        # print "len: %d" % data_len
            for tr in web_data[2:]:
                td = tr.select('td')
                date_list = td[0].text.split('/')
                if len(date_list) != 3:
                    raise RuntimeError("The date format is NOT as expected: %s", date_list)
                cur_day = int(date_list[2])
                if not whole_month_data and cur_day < start_day:
# Caution: It's NO need to consider the end date since the latest data is always today
                    continue
                workday_list.append(cur_day)
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
        current_path = os.path.dirname(os.path.realpath(__file__))
        [project_folder, lib_folder] = current_path.rsplit('/', 1)
        conf_filepath = "%s/%s/%s" % (project_folder, CMN.DEF_CONF_FOLDER, CMN.DEF_WORKDAY_CANLENDAR_CONF_FILENAME)
        g_logger.debug("Write the Workday Canlendar data to the file: %s......" % conf_filepath)
        try:
            date_range_str = None
            with open(conf_filepath, 'w') as fp:
                g_logger.debug("Start to write workday into %s", CMN.DEF_WORKDAY_CANLENDAR_CONF_FILENAME)
                fp.write("%04d-%02d-%02d %04d-%02d-%02d\n" % (self.datetime_start.year, self.datetime_start.month, self.datetime_start.day, self.datetime_end.year, self.datetime_end.month, self.datetime_end.day))
                year_list = sorted(self.workday_canlendar)
                for year in year_list:
                    workday_month_list = self.workday_canlendar[year]
                    # sys.stderr.write("%s" % ("[%04d]" % year + ";".join(["%d:%s" % (month + 1, ",".join([str(workday) for workday in workday_month_list[month]])) for month in range(12)]) + "\n"))
                    workday_each_year_str = "[%04d]" % year + ";".join(["%d:%s" % (month + 1, ",".join([str(workday) for workday in workday_month_list[month]])) for month in range(12) if len(workday_month_list[month]) != 0]) + "\n"
                    # for month in range(12):
                    #     workday_each_year_str += "%d:%s;" % (month + 1, ",".join([str(workday) for workday in workday_month_list[month]]))
                    # workday_each_year_str += "\n"
                    g_logger.debug("Write data: %s" % workday_each_year_str)
                    fp.write(workday_each_year_str)

        except Exception as e:
            g_logger.error("Error occur while writing Workday Canlendar into config file, due to %s" % str(e))
            raise e


    def get_first_workday(self):
        if self.first_workday_cfg is None:
            if self.workday_canlendar is None:
                raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
            year = self.workday_year_list[0]
            for month in range(12):
                if len(self.workday_canlendar[year][month]) != 0:
                    self.first_workday_cfg = datetime(year, month + 1, self.workday_canlendar[year][month][0]) 
                    break
            if self.first_workday_cfg is None:
                raise RuntimeError("Fail to find the frst workday in the canlendar") 
        return self.first_workday_cfg


    def get_latest_workday(self):
        if self.latest_workday_cfg is None:
            if self.workday_canlendar is None:
                raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
            # import pdb; pdb.set_trace()
            workday_year_list = sorted(self.workday_canlendar.keys())
            year = workday_year_list[-1]
            for month in range(12):
                if len(self.workday_canlendar[year][month]) == 0:
                    self.latest_workday_cfg = datetime(year, month, self.workday_canlendar[year][month - 1][-1]) 
                    break
            if self.latest_workday_cfg is None:
                month = 12
                self.latest_workday_cfg = datetime(year, month, self.workday_canlendar[year][month - 1][-1]) 
        return self.latest_workday_cfg


    def is_in_range(self, datetime_cur):
        datetime_first = self.get_first_workday()
        datetime_last = self.get_latest_workday()
        result = (datetime_cur >= datetime_first and datetime_cur <= datetime_last)
        if not result:
            g_logger.debug("Date[%s] is NOT in range[%s, %s]" % (CMN.to_date_only_str(datetime_cur), CMN.to_date_only_str(datetime_first), CMN.to_date_only_str(datetime_last)))
        return result


    def find_index(self, datetime_cur):
        # import pdb; pdb.set_trace()
        if self.workday_canlendar is None:
            raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
        year_index = None
        month_index = None
        day_index = None
        try:
            year_index = self.workday_year_list.index(datetime_cur.year)
        except ValueError as e:
            return None
        year = self.workday_year_list[year_index]
        if len(self.workday_canlendar[year][datetime_cur.month - 1]) == 0:
            return None
        month_index = datetime_cur.month - 1
        try:
            day_index = self.workday_canlendar[year][month_index].index(datetime_cur.day)
        except ValueError as e:
            return None

        # g_logger.debug("The date[%s] index: %d %d %d" % (CMN.to_date_only_str(datetime_cur), year_index, month_index, day_index))
        return (year_index, month_index, day_index)


    def find_nearest_next_index(self, datetime_cur):
        if self.workday_canlendar is None:
            raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
        year_index = None
        month_index = None
        day_index = None
        if self.is_workday(datetime_cur):
            (year_index, month_index, day_index) = self.find_index(datetime_cur)
        else:
            if not self.is_in_range(datetime_cur):
                return None
            try:
                year_index = self.workday_year_list.index(datetime_cur.year)
            except ValueError as e:
                return None
            # year = self.workday_year_list[year_index]
            month_index = datetime_cur.month - 1
            day_list = self.workday_canlendar[datetime_cur.year][month_index]
            for index, day in enumerate(day_list):
                if day > datetime_cur.day:
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
        # g_logger.debug("The date[%s] index: %d %d %d" % (CMN.to_date_only_str(datetime_cur), year_index, month_index, day_index))
        return (year_index, month_index, day_index)


    def get_nearest_next_workday(self, datetime_cur):
        index_tuple = self.find_nearest_next_index(datetime_cur)
        if index_tuple is None:
            raise RuntimeError("Fail to find the nearest next workday from the date: %s" % CMN.to_date_only_str(datetime_cur))
        (year_index, month_index, day_index) = index_tuple
        year = self.workday_year_list[year_index]
        datetime_nearest_next = datetime(year, month_index + 1, self.workday_canlendar[year][month_index][day_index])
        if not self.is_workday(datetime_cur):
            g_logger.debug("Find the nearest next workday[%s]: %s" % (CMN.to_date_only_str(datetime_cur), CMN.to_date_only_str(datetime_nearest_next)))
        return datetime_nearest_next


    def find_nearest_prev_index(self, datetime_cur):
        if self.workday_canlendar is None:
            raise RuntimeError("Incorrect Operation: self.workday_canlendar is None")
        year_index = None
        month_index = None
        day_index = None
        if self.is_workday(datetime_cur):
            (year_index, month_index, day_index) = self.find_index(datetime_cur)
        else:
            if not self.is_in_range(datetime_cur):
                return None
            try:
                year_index = self.workday_year_list.index(datetime_cur.year)
            except ValueError as e:
                return None
            # year = self.workday_year_list[year_index]
            month_index = datetime_cur.month - 1
            # import pdb; pdb.set_trace()
            day_list = self.workday_canlendar[datetime_cur.year][month_index]
            for index, day in reversed(list(enumerate(day_list))):
                if day < datetime_cur.day:
                    day_index = index
                    break
            if day_index is None:
                if month_index == 1:
# An old year
                    year_index -= 1
                    month_index = 11
                    day_index = len(self.workday_canlendar[datetime_cur.year - 1][month_index]) - 1
                else:
# An old month
                    month_index -= 1
                    day_index = len(self.workday_canlendar[datetime_cur.year - 1][month_index]) - 1
        # g_logger.debug("The date[%s] index: %d %d %d" % (CMN.to_date_only_str(datetime_cur), year_index, month_index, day_index))
        return (year_index, month_index, day_index)


    def get_nearest_prev_workday(self, datetime_cur):
        index_tuple = self.find_nearest_prev_index(datetime_cur)
        if index_tuple is None:
            raise RuntimeError("Fail to find the nearest previous workday from the date: %s" % CMN.to_date_only_str(datetime_cur))
        (year_index, month_index, day_index) = index_tuple
        year = self.workday_year_list[year_index]
        datetime_nearest_prev = datetime(year, month_index + 1, self.workday_canlendar[year][month_index][day_index])
        if not self.is_workday(datetime_cur):
            g_logger.debug("Find the nearest previous workday[%s]: %s" % (CMN.to_date_only_str(datetime_cur), CMN.to_date_only_str(datetime_nearest_prev)))
        return datetime_nearest_prev

####################################################################################################

class WebScrapyWorkdayCanlendarIterator(object):

    def __init__(self, datetime_start=None, datetime_end=None):
        self.workday_canlendar_obj = WebScrapyWorkdayCanlendar.Instance()
        self.workday_canlendar = self.workday_canlendar_obj.workday_canlendar
        self.workday_year_list = self.workday_canlendar_obj.workday_year_list

        if datetime_start is None:
            datetime_start = self.workday_canlendar_obj.get_first_workday()
        if datetime_end is None:
            datetime_end = self.workday_canlendar_obj.get_latest_workday()
        start_tuple = self.workday_canlendar_obj.find_index(datetime_start)
        if start_tuple is None:
            raise ValueError("The start date[%s] is NOT a workday" % CMN.to_date_only_str(datetime_start))
        (start_year_index, start_month_index, start_day_index) = start_tuple
        g_logger.debug("The start date[%s] index: %d %d %d" % (CMN.to_date_only_str(datetime_start), start_year_index, start_month_index, start_day_index))
        end_tuple = self.workday_canlendar_obj.find_index(datetime_end)
        if end_tuple is None:
            raise ValueError("The end date[%s] is NOT a workday" % CMN.to_date_only_str(datetime_end))
        (self.end_year_index, self.end_month_index, self.end_day_index) = end_tuple
        g_logger.debug("The end date[%s] index: %d %d %d" % (CMN.to_date_only_str(datetime_end), self.end_year_index, self.end_month_index, self.end_day_index))
        self.cur_year_index = start_year_index
        self.cur_month_index = start_month_index
        self.cur_day_index = start_day_index
        self.time_to_stop = False


    def __iter__(self):
        return self


    def next(self):
        # import pdb; pdb.set_trace()
        if self.time_to_stop:
            raise StopIteration
        if self.cur_year_index == self.end_year_index and self.cur_month_index == self.end_month_index and self.cur_day_index == self.end_day_index:
            self.time_to_stop = True
        year = self.workday_year_list[self.cur_year_index]
        datetime_cur = datetime(year, self.cur_month_index + 1, self.workday_canlendar[year][self.cur_month_index][self.cur_day_index])
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
        return datetime_cur


####################################################################################################

class WebScrapyWorkdayCanlendarNearestIterator(WebScrapyWorkdayCanlendarIterator):

    def __init__(self, datetime_start=None, datetime_end=None):
        workday_canlendar_obj = WebScrapyWorkdayCanlendar.Instance()
        datetime_nearest_start = workday_canlendar_obj.get_nearest_next_workday(datetime_start) if datetime_start is not None else None
        datetime_nearest_end = workday_canlendar_obj.get_nearest_prev_workday(datetime_end) if datetime_end is not None else None
        super(WebScrapyWorkdayCanlendarNearestIterator, self).__init__(datetime_nearest_start, datetime_nearest_end)
