# -*- coding: utf8 -*-

import os
import re
import requests
import csv
import time
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebScrapyBase(object):

    def __init__(self, url_format, cur_file_path, encoding, select_flag, datetime_range_start=None, datetime_range_end=None, enable_time_range_mode=False):
        self.scrap_web_to_csv_func_ptr = [self.__scrap_web_to_csv_one_month_per_file, self.__scrap_web_to_csv_one_day_per_file]

        self.url_format = url_format
        cur_module_name = re.sub(CMN.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX, "", CMN.get_cur_module_name(cur_file_path))
        # g_logger.debug("Current module name (w/o prefix): %s" % cur_module_name)
        self.data_source_index = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index(cur_module_name)

        self.encoding = encoding
        self.select_flag = select_flag
        self.datetime_range_list = []
        # self.workday_datatime_range_list = []
  
        self.datetime_startday = None
        self.datetime_endday = None
        self.description = None

        if CMN.DEF_DATA_SOURCE_WRITE2CSV_METHOD[self.data_source_index] == CMN.WRITE2CSV_ONE_MONTH_PER_FILE:
            csv_filename_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[self.data_source_index] + "_%s.csv"
            self.csv_filename = csv_filename_format % self. __generate_time_string_filename(datetime_range_start)
            self.csv_filepath = "%s/%s" % (CMN.DEF_CSV_FILE_PATH, self.csv_filename)
            g_logger.debug("Write data[%s] to CSV file: %s" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], self.csv_filepath))
        elif CMN.DEF_DATA_SOURCE_WRITE2CSV_METHOD[self.data_source_index] == CMN.WRITE2CSV_ONE_DAY_PER_FILE:
            csv_foldername_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[self.data_source_index] + "_%s"
            self.csv_foldername = csv_foldername_format % self. __generate_time_string_filename(datetime_range_start)
            self.csv_folderpath = "%s/%s" % (CMN.DEF_CSV_FILE_PATH, self.csv_foldername)
            g_logger.debug("Write data[%s] to CSV folder: %s" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], self.csv_folderpath))

        self.enable_time_range_mode = enable_time_range_mode

        self.workday_canlendar = WorkdayCanlendar.WebScrapyWorkdayCanlendar.Instance()

        if not self.enable_time_range_mode:
            self.__generate_day_time_list(datetime_range_start, datetime_range_end)
            g_logger.debug("There are totally %d day(s) to be downloaded" % len(self.datetime_range_list))

            if len(self.datetime_range_list) == 1:
                self.datetime_startday = self.datetime_endday = self.datetime_range_list[0]
            else:
                self.datetime_startday = self.datetime_range_list[0]
                self.datetime_endday = self.datetime_range_list[-1]
        else:
            (self.datetime_startday, self.datetime_endday) = self.__check_datetime_input(datetime_range_start, datetime_range_end)

        if self.datetime_startday == self.datetime_endday:
            self.description = "%s[%04d%02d%02d]" % (
                CMN.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], 
                self.datetime_startday.year, 
                self.datetime_startday.month, 
                self.datetime_startday.day
            )
        else:
            self.description = "%s[%04d%02d%02d-%04d%02d%02d]" % (
                CMN.DEF_DATA_SOURCE_INDEX_MAPPING[self.data_source_index], 
                self.datetime_startday.year, 
                self.datetime_startday.month, 
                self.datetime_startday.day,
                self.datetime_endday.year, 
                self.datetime_endday.month, 
                self.datetime_endday.day
            )   


    def get_real_datetime_start(self):
        return self.datetime_startday


    def get_real_datetime_end(self):
        return self.datetime_endday


    def get_description(self):
        return self.description


    def get_data_source_index(self):
        return self.data_source_index


    def get_datetime_startday(self):
        return self.datetime_startday


    def get_datetime_endday(self):
        return self.datetime_endday


    def __generate_time_string_filename(self, datetime_cfg=None):
    	if datetime_cfg is None:
    		datetime_cfg = datetime.today()
    	return "%04d%02d" % (datetime_cfg.year, datetime_cfg.month)


    def __get_web_data(self, url):
        # res = requests.get(url)
        try:
            # g_logger.debug("Try to Scrap data [%s]" % url)
            res = requests.get(url, timeout=CMN.DEF_SCRAPY_WAIT_TIMEOUT)
        except requests.exceptions.Timeout as e:
            # g_logger.debug("Try to Scrap data [%s]... Timeout" % url)
            fail_to_scrap = False
            for index in range(CMN.DEF_SCRAPY_RETRY_TIMES):
                time.sleep(randint(3,9))
                try:
                    # g_logger.debug("Retry to scrap web data [%s]......%d" % (url, index))
                    res = requests.get(url, timeout=CMN.DEF_SCRAPY_WAIT_TIMEOUT)
                except requests.exceptions.Timeout as ex:
                    # g_logger.debug("Retry to scrap web data [%s]......%d, FAIL!!!" % (url, index))
                    fail_to_scrap = True
                if not fail_to_scrap:
                    break
            if fail_to_scrap:
                # import pdb; pdb.set_trace()
                g_logger.error("Fail to scrap web data [%s] even retry for %d times !!!!!!" % (url, self.SCRAPY_RETRY_TIMES))
                raise e

        res.encoding = self.encoding
        # print res.text
        soup = BeautifulSoup(res.text)
        web_data = soup.select(self.select_flag)

        return web_data


    def __check_datetime_input(self, datetime_range_start, datetime_range_end):
        datetime_tmp = datetime.today()
        datetime_today = datetime(datetime_tmp.year, datetime_tmp.month, datetime_tmp.day)
        datetime_start = None
        datetime_end = None
        if datetime_range_start is None:
            if datetime_range_end is not None:
                raise ValueError("datetime_range_start is None but datetime_range_end is NOT None")
            else:
                datetime_start = datetime_end = datetime_today
                g_logger.debug("Only grab the data today[%s-%s-%s]" % (datetime_today.year, datetime_today.month, datetime_today.day))
        else:
            datetime_start = datetime_range_start
            if datetime_range_end is not None:
                datetime_end = datetime_range_end
            else:
                datetime_end = datetime_today
            g_logger.debug("Grab the data from date[%s-%s-%s] to date[%s-%s-%s]" % (datetime_start.year, datetime_start.month, datetime_start.day, datetime_end.year, datetime_end.month, datetime_end.day))

        return (datetime_start, datetime_end)


    def __generate_day_time_list(self, datetime_range_start=None, datetime_range_end=None):
        # import pdb; pdb.set_trace()
        (datetime_start, datetime_end) = self.__check_datetime_input(datetime_range_start, datetime_range_end)
        day_offset = 1
        datetime_offset = datetime_start
        while True: 
            self.datetime_range_list.append(datetime_offset)
            datetime_offset = datetime_offset + timedelta(days = day_offset)
            if datetime_offset > datetime_end:
            	break


    def assemble_web_url(self, datetime_cfg):
        raise NotImplementedError


    def parse_web_data(self, web_data):
        raise NotImplementedError


    def __scrap_web_by_time_range(self, csv_data_list):
        url = self.assemble_web_url(None)
        g_logger.debug("Get the data from by time range from URL: %s" % url)
        try:
# Grab the data from website and assemble the data to the entry of CSV
            web_data_list = self.parse_web_data(self.__get_web_data(url))
            if web_data_list is None:
                raise RuntimeError(url)
            for web_data in web_data_list:
            # g_logger.debug("Write the data[%s] to %s" % (web_data, self.csv_filename))
                csv_data_list.append(web_data)
        except requests.exceptions.Timeout as e:
            g_logger.error("Fail to scrap URL[%s], due to: Time-out" % url)
            raise e
        except Exception as e:
            g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))


    def __scrap_web_by_day(self, csv_data_list):
        # import pdb; pdb.set_trace()
        # datetime_first_day_cfg = self.datetime_range_list[0]
        # day_of_week = datetime_first_day_cfg.weekday()
        is_weekend = False
        for datetime_cfg in self.datetime_range_list:
            # if day_of_week in [5, 6]: # 5: Saturday, 6: Sunday
            #     is_weekend = True
            # else:
            #     is_weekend = False
            # day_of_week = (day_of_week + 1) % 7
            # if is_weekend:
            #     g_logger.debug("%04d-%02d-%02d is weekend, Skip..." % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
            #     continue
            if not self.workday_canlendar.is_workday(datetime_cfg):
                g_logger.debug("%04d-%02d-%02d is NOT a workday, Skip..." % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
                continue

            url = self.assemble_web_url(datetime_cfg)
            g_logger.debug("Get the data by date from URL: %s" % url)
            try:
# Grab the data from website and assemble the data to the entry of CSV
                web_data = self.parse_web_data(self.__get_web_data(url))
                if web_data is None:
                    raise RuntimeError(url)
                csv_data = ["%04d-%02d-%02d" % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)] + web_data
                # g_logger.debug("Write the data[%s] to %s" % (csv_data, self.csv_filename))
                csv_data_list.append(csv_data)
            except requests.exceptions.Timeout as e:
                g_logger.error("Fail to scrap URL[%s], due to: Time-out" % url)
                raise e
            except Exception as e:
                g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))


    def __scrap_web_to_csv_one_month_per_file(self):
        csv_data_list = []
        web_data = None
        # ret = CMN.RET_SUCCESS
        import pdb; pdb.set_trace()
        with open(self.csv_filepath, 'w') as fp:
            fp_writer = csv.writer(fp, delimiter=',')
            filtered_web_data_date = None
            filtered_web_data = None
# Scrap web data due to different time range
            if self.enable_time_range_mode:
                self.__scrap_web_by_time_range(csv_data_list)
            else:
                self.__scrap_web_by_day(csv_data_list)
# Write the web data into CSV
            if len(csv_data_list) > 0:
                g_logger.debug("Write %d data to %s" % (len(csv_data_list), self.csv_filepath))
                fp_writer.writerows(csv_data_list)
            else:
                g_logger.warn("Emtpy data, remove the CSV file: %s" % self.csv_filepath)
                try:
                    os.remove(self.csv_filepath)
                except OSError as e:  ## if failed, report it back to the user ##
                    g_logger.error("Fail to remove the CSV file: %s, due to: %s" % (self.csv_filepath, str(e)))

        return CMN.RET_SUCCESS


    def __scrap_web_to_csv_one_day_per_file(self):
        # import pdb; pdb.set_trace()
        CMN.create_folder_if_not_exist(self.csv_folderpath)

        for datetime_cfg in self.datetime_range_list:
            if not self.workday_canlendar.is_workday(datetime_cfg):
                # g_logger.debug("%04d-%02d-%02d is NOT a workday, Skip..." % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
                continue
            csv_filepath = "%s/%s.csv" % (self.csv_folderpath, CMN.transform_datetime_cfg2string(datetime_cfg))

            url = self.assemble_web_url(datetime_cfg)
            g_logger.debug("Get the data by date from URL: %s" % url)
            try:
# Grab the data from website and assemble the data to the entry of CSV
                csv_data_list = self.parse_web_data(self.__get_web_data(url))
                if csv_data_list is None:
                    raise RuntimeError(url)

                g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
                with open(csv_filepath, 'w') as fp:
                    fp_writer = csv.writer(fp, delimiter=',')
                    for csv_data in csv_data_list:
# Write the web data into CSV
                        fp_writer.writerows(csv_data)
            except Exception as e:
                g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))

        return CMN.RET_SUCCESS


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
        return (self.scrap_web_to_csv_func_ptr[CMN.DEF_DATA_SOURCE_WRITE2CSV_METHOD[self.data_source_index]])()
