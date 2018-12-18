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
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
from libs import web_scrapy_timeslice_generator as TimeSliceGenerator
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebScrapyBase(object):

    def __init__(self, cur_file_path, **kwargs):
        # self.scrape_web_to_csv_func_ptr = [self.__scrape_web_to_csv_one_month_per_file, self.__scrape_web_to_csv_one_day_per_file]
        # self.scrape_web_to_csv_func_ptr = [self.__scrap_multiple_web_data_to_single_csv_file, self.__scrap_single_web_data_to_single_csv_file]

        cur_module_name = re.sub(CMN.SCRAPY_MODULE_NAME_PREFIX, "", CMN.get_cur_module_name(cur_file_path))
        # g_logger.debug("Current module name (w/o prefix): %s" % cur_module_name)
        self.data_source_index = CMN.SCRAPY_MODULE_NAME_MAPPING.index(cur_module_name)
# # Check Start/End time range
#         if kwargs.get("datetime_range_start", None) is None:
#             datetime_range_start = CMN.DATA_SOURCE_START_DATE_CFG[self.data_source_index]
#         if kwargs.get("datetime_range_end", None) is None:
#             workday_canlendar_obj = WorkdayCanlendar.WebScrapyWorkdayCanlendar()
#             datetime_range_end = workday_canlendar_obj.get_latest_workday()
        
        self.PARSE_URL_DATA_FUNC_PTR = [self.__select_web_data_by_bs4, self.__select_web_data_by_json]

        self.source_url_parsing_cfg = CMN.SOURCE_URL_PARSING[self.data_source_index]
        # self.url_format = source_url_parsing_cfg.get("url_format", None)
        # self.url_timeslice = source_url_parsing_cfg.get("url_timeslice", None)
        # self.url_encoding = source_url_parsing_cfg.get("url_encoding", None)
        # self.url_parsing_method = source_url_parsing_cfg.get("url_parsing_method", None)
        # self.url_data_selector = source_url_parsing_cfg.get("url_data_selector", None)

        # self.parse_url_data_type_obj = source_url_parsing_cfg["parse_url_data_obj"]
        self.timeslice_generate_method = self.source_url_parsing_cfg["url_timeslice"]
        # self.select_web_data = self.__select_web_data_by_bs4
        # source_data_time_unit_cfg = CMN.CSV_TIME_UNIT[self.data_source_index]
        csv_time_unit = CMN.CSV_TIME_UNIT[self.data_source_index]
        url_time_unit = CMN.TIMESLICE_TO_TIME_UNIT_MAPPING[self.timeslice_generate_method]
        self.scrape_web_to_csv_func_ptr = self.__scrap_multiple_web_data_to_single_csv_file if url_time_unit == csv_time_unit self.__scrap_single_web_data_to_single_csv_file
        # if url_time_unit == csv_time_unit:
        #     self.scrape_web_to_csv_func_ptr = self.__scrap_single_web_data_to_single_csv_file
        # else:
        #     self.scrape_web_to_csv_func_ptr = self.__scrap_multiple_web_data_to_single_csv_file
        # self.timeslice_generator_obj = TimeSliceGenerator.WebScrapyTimeSliceGenerator.Instance()
        self.timeslice_iterable = timeslice_generator_obj.generate_time_slice(self.timeslice_generate_method, **kwargs)
        # self.generate_day_time_list_rule = None
        # self.datetime_range_list = []
        # self.workday_canlendar = WorkdayCanlendar.WebScrapyWorkdayCanlendar.Instance()
        self.description = None

        # if CMN.DATA_SOURCE_WRITE2CSV_METHOD[self.data_source_index] == CMN.WRITE2CSV_ONE_MONTH_PER_FILE:
        #     csv_filename_format = CMN.SCRAPY_MODULE_NAME_MAPPING[self.data_source_index] + "_%s.csv"
        #     self.csv_filename = csv_filename_format % self. __generate_time_string_filename(datetime_range_start)
        #     self.csv_filepath = "%s/%s" % (CMN.CSV_FILE_PATH, self.csv_filename)
        #     g_logger.debug("Write data[%s] to CSV file: %s" % (CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], self.csv_filepath))
        # elif CMN.DATA_SOURCE_WRITE2CSV_METHOD[self.data_source_index] == CMN.WRITE2CSV_ONE_DAY_PER_FILE:
        #     csv_foldername_format = CMN.SCRAPY_MODULE_NAME_MAPPING[self.data_source_index] + "_%s"
        #     self.csv_foldername = csv_foldername_format % self. __generate_time_string_filename(datetime_range_start)
        #     self.csv_folderpath = "%s/%s" % (CMN.CSV_FILE_PATH, self.csv_foldername)
        #     g_logger.debug("Write data[%s] to CSV folder: %s" % (CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], self.csv_folderpath))

        # self.enable_time_range_mode = enable_time_range_mode

        self.timeslice_list_generated = False
        self.timeslice_buffer = None
        self.timeslice_buffer_len = -1
# It's required to create the time slice list to setup the following variables
        self.timeslice_start = None
        self.timeslice_end = None
        self.timeslice_list_description = None
        self.timeslice_cnt = None

        # if not self.enable_time_range_mode:
        #     self.__generate_day_time_list(datetime_range_start, datetime_range_end)
        #     g_logger.debug("There are totally %d day(s) to be downloaded" % len(self.datetime_range_list))

        #     if len(self.datetime_range_list) == 1:
        #         self.datetime_startday = self.datetime_endday = self.datetime_range_list[0]
        #     else:
        #         self.datetime_startday = self.datetime_range_list[0]
        #         self.datetime_endday = self.datetime_range_list[-1]
        # else:
        #     (self.datetime_startday, self.datetime_endday) = self.__check_datetime_input(datetime_range_start, datetime_range_end)

        # if self.datetime_startday == self.datetime_endday:
        #     self.description = "%s[%04d%02d%02d]" % (
        #         CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], 
        #         self.datetime_startday.year, 
        #         self.datetime_startday.month, 
        #         self.datetime_startday.day
        #     )
        # else:
        #     self.description = "%s[%04d%02d%02d-%04d%02d%02d]" % (
        #         CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], 
        #         self.datetime_startday.year, 
        #         self.datetime_startday.month, 
        #         self.datetime_startday.day,
        #         self.datetime_endday.year, 
        #         self.datetime_endday.month, 
        #         self.datetime_endday.day
        #     )   


    # def get_real_datetime_start(self):
    #     return self.datetime_startday


    # def get_real_datetime_end(self):
    #     return self.datetime_endday
    def __generate_timeslice_list(self):
        if not self.timeslice_list_generated:
            timeslice_list = list(self.timeslice_iterable)
            self.timeslice_start = timeslice_list[0]
            self.timeslice_end = timeslice_list[-1]
            self.timeslice_cnt = len(timeslice_list)
            self.timeslice_list_generated = True
            if self.timeslice_start == self.timeslice_end:
                msg = "%s: %04d-%02d-%02d" % (CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], config['start'].year, config['start'].month, config['start'].day)
            else:
                msg = "%s: %04d-%02d-%02d:%04d-%02d-%02d" % (CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], config['start'].year, config['start'].month, config['start'].day, config['end'].year, config['end'].month, config['end'].day)
            g_logger.info(msg)


    def get_description(self, show_detail=False):
        if self.description is None:
            if show_detail:
                if not self.timeslice_list_generated:
                    self.__generate_timeslice_list()
                if self.timeslice_start == self.timeslice_end:
                    self.description = "%s[%s]" % (
                        CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], 
                        self.timeslice_start.to_string()
                    )
                else:
                    self.description = "%s[%s-%s]" % (
                        CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index], 
                        self.timeslice_start.to_string(), 
                        self.timeslice_end.to_string()
                    )
            else:
                self.description = "%s" % CMN.SCRAPY_METHOD_DESCRIPTION[self.data_source_index]
        return self.description


    def get_timeslice_list_description(self):
        if self.timeslice_list_description is None:
            if not self.timeslice_list_generated:
                self.__generate_timeslice_list()
            self.timeslice_list_description = "Range[%s %s]; Totally %d data" % (self.timeslice_start.to_string, self.timeslice_end.to_string(), self.timeslice_cnt)
        return self.timeslice_list_description


    def get_data_source_index(self):
        return self.data_source_index


    # def get_datetime_startday(self):
    #     return self.datetime_startday


    # def get_datetime_endday(self):
    #     return self.datetime_endday


    # def __generate_time_string_filename(self, datetime_cfg=None):
    # 	if datetime_cfg is None:
    # 		datetime_cfg = datetime.today()
    # 	return "%04d%02d" % (datetime_cfg.year, datetime_cfg.month)


    def __get_web_data(self, url):
        # res = requests.get(url)
        try:
            # g_logger.debug("Try to Scrap data [%s]" % url)
            res = requests.get(url, timeout=CMN.SCRAPY_WAIT_TIMEOUT)
        except requests.exceptions.Timeout as e:
            # g_logger.debug("Try to Scrap data [%s]... Timeout" % url)
            fail_to_scrap = False
            for index in range(CMN.SCRAPY_RETRY_TIMES):
                time.sleep(randint(3,9))
                try:
                    # g_logger.debug("Retry to scrap web data [%s]......%d" % (url, index))
                    res = requests.get(url, timeout=CMN.SCRAPY_WAIT_TIMEOUT)
                except requests.exceptions.Timeout as ex:
                    # g_logger.debug("Retry to scrap web data [%s]......%d, FAIL!!!" % (url, index))
                    fail_to_scrap = True
                if not fail_to_scrap:
                    break
            if fail_to_scrap:
                # import pdb; pdb.set_trace()
                g_logger.error("Fail to scrap web data [%s] even retry for %d times !!!!!!" % (url, self.SCRAPY_RETRY_TIMES))
                raise e
        parse_url_data_type = self.parse_url_data_type_obj.get_type()

        return (self.PARSE_URL_DATA_FUNC_PTR[parse_url_data_type])(res)


    def __select_web_data_by_bs4(self, url_data):
        # import pdb; pdb.set_trace()
        if not isinstance(self.parse_url_data_type_obj, CMN_CLS.ParseURLDataByBS4):
            raise RuntimeError("Incorrect object type, should be CMN_CLS.ParseURLDataByBS4")
        url_data.encoding = getattr(self.parse_url_data_type_obj, "encoding")
        soup = BeautifulSoup(url_data.text)
        web_data = soup.select(getattr(self.parse_url_data_type_obj, "select_flag"))

        return web_data


    def __select_web_data_by_json(self, url_data):
        # import pdb; pdb.set_trace()
        if not isinstance(self.parse_url_data_type_obj, CMN_CLS.ParseURLDataByJSON):
            raise RuntimeError("Incorrect object type, should be CMN_CLS.ParseURLDataByJSON")
        json_url_data = json.loads(url_data.text)
        web_data = json_url_data[getattr(self.parse_url_data_type_obj, "data_field_name")]

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
                g_logger.debug("Only grab the data today[%s]" % CMN.to_date_only_str(datetime_today))
        else:
            datetime_start = datetime_range_start
            if datetime_range_end is not None:
                datetime_end = datetime_range_end
            else:
                datetime_end = datetime_today
            g_logger.debug("Grab the data from date[%s] to date[%s]" % (CMN.to_date_only_str(datetime_start), CMN.to_date_only_str(datetime_end)))

        return (datetime_start, datetime_end)


    # def __generate_day_time_list(self, datetime_range_start=None, datetime_range_end=None):
    #     # import pdb; pdb.set_trace()
    #     (datetime_start, datetime_end) = self.__check_datetime_input(datetime_range_start, datetime_range_end)
    #     day_offset = 1
    #     datetime_offset = datetime_start
    #     while True: 
    #         if self.generate_day_time_list_rule is not None and not self.generate_day_time_list_rule(datetime_offset):
    #             continue
    #         self.datetime_range_list.append(datetime_offset)
    #         datetime_offset = datetime_offset + timedelta(days = day_offset)
    #         if datetime_offset > datetime_end:
    #         	break


    # def assemble_web_url(self, datetime_cfg):
    #     raise NotImplementedError


    def parse_web_data(self, web_data):
        raise NotImplementedError


    def is_same_time_unit(self, timeslice):
        raise NotImplementedError


    def assemble_csv_filepath(self, timeslice):
        raise NotImplementedError


    def __reset_time_slice_buffer(self):
        self.timeslice_buffer = []
        self.timeslice_buffer_len = 0


    def __add_to_time_slice_buffer(self, timeslice):
        assert (elf.timeslice_buffer is not None), "self.timeslice_buffer is None"
        timeslice_buffer = None
        if self.timeslice_buffer_len != 0 && not self.is_same_time_unit(timeslice):
            timeslice_buffer = self.timeslice_buffer
            self.__reset_time_slice_buffer()
        self.timeslice_buffer.append(timeslice)
        self.timeslice_buffer_len += 1
        return timeslice_buffer


#     def __scrap_web_by_time_range(self, csv_data_list):
#         url = self.assemble_web_url(None)
#         g_logger.debug("Get the data from by time range from URL: %s" % url)
#         try:
# # Grab the data from website and assemble the data to the entry of CSV
#             web_data_list = self.parse_web_data(self.__get_web_data(url))
#             if web_data_list is None:
#                 raise RuntimeError(url)
#             for web_data in web_data_list:
#             # g_logger.debug("Write the data[%s] to %s" % (web_data, self.csv_filename))
#                 csv_data_list.append(web_data)
#         except requests.exceptions.Timeout as e:
#             g_logger.error("Fail to scrap URL[%s], due to: Time-out" % url)
#             raise e
#         except Exception as e:
#             g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))


#     def __scrap_web_by_day(self, csv_data_list):
#         # import pdb; pdb.set_trace()
#         # datetime_first_day_cfg = self.datetime_range_list[0]
#         # day_of_week = datetime_first_day_cfg.weekday()
#         is_weekend = False
#         for datetime_cfg in self.datetime_range_list:
#             # if day_of_week in [5, 6]: # 5: Saturday, 6: Sunday
#             #     is_weekend = True
#             # else:
#             #     is_weekend = False
#             # day_of_week = (day_of_week + 1) % 7
#             # if is_weekend:
#             #     g_logger.debug("%04d-%02d-%02d is weekend, Skip..." % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
#             #     continue
#             if not self.workday_canlendar.is_workday(datetime_cfg):
#                 g_logger.debug("%04d-%02d-%02d is NOT a workday, Skip..." % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
#                 continue

#             url = self.assemble_web_url(datetime_cfg)
#             g_logger.debug("Get the data by date from URL: %s" % url)
#             try:
# # Grab the data from website and assemble the data to the entry of CSV
#                 web_data = self.parse_web_data(self.__get_web_data(url))
#                 if web_data is None:
#                     raise RuntimeError(url)
#                 csv_data = ["%04d-%02d-%02d" % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)] + web_data
#                 # g_logger.debug("Write the data[%s] to %s" % (csv_data, self.csv_filename))
#                 csv_data_list.append(csv_data)
#             except requests.exceptions.Timeout as e:
#                 g_logger.error("Fail to scrap URL[%s], due to: Time-out" % url)
#                 raise e
#             except Exception as e:
#                 g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))


#     def __scrape_web_to_csv_one_month_per_file(self):
#         csv_data_list = []
#         web_data = None
#         # ret = CMN.RET_SUCCESS
#         # import pdb; pdb.set_trace()
#         with open(self.csv_filepath, 'w') as fp:
#             fp_writer = csv.writer(fp, delimiter=',')
#             filtered_web_data_date = None
#             filtered_web_data = None
# # Scrap web data due to different time range
#             if self.enable_time_range_mode:
#                 self.__scrap_web_by_time_range(csv_data_list)
#             else:
#                 self.__scrap_web_by_day(csv_data_list)
# # Write the web data into CSV
#             if len(csv_data_list) > 0:
#                 g_logger.debug("Write %d data to %s" % (len(csv_data_list), self.csv_filepath))
#                 fp_writer.writerows(csv_data_list)
#             else:
#                 g_logger.warn("Emtpy data, remove the CSV file: %s" % self.csv_filepath)
#                 try:
#                     os.remove(self.csv_filepath)
#                 except OSError as e:  ## if failed, report it back to the user ##
#                     g_logger.error("Fail to remove the CSV file: %s, due to: %s" % (self.csv_filepath, str(e)))

#         return CMN.RET_SUCCESS


#     def __scrape_web_to_csv_one_day_per_file(self):
#         # import pdb; pdb.set_trace()
#         CMN.create_folder_if_not_exist(self.csv_folderpath)

#         for datetime_cfg in self.datetime_range_list:
#             if not self.workday_canlendar.is_workday(datetime_cfg):
#                 # g_logger.debug("%04d-%02d-%02d is NOT a workday, Skip..." % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
#                 continue
#             csv_filepath = "%s/%s.csv" % (self.csv_folderpath, CMN.transform_datetime_cfg2string(datetime_cfg))

#             url = self.assemble_web_url(datetime_cfg)
#             g_logger.debug("Get the data by date from URL: %s" % url)
#             try:
# # Grab the data from website and assemble the data to the entry of CSV
#                 csv_data_list = self.parse_web_data(self.__get_web_data(url))
#                 if csv_data_list is None:
#                     raise RuntimeError(url)

#                 g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
#                 with open(csv_filepath, 'w') as fp:
#                     fp_writer = csv.writer(fp, delimiter=',')
# # Write the web data into CSV
#                     fp_writer.writerows(csv_data_list)
#             except Exception as e:
#                 g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))

#         return CMN.RET_SUCCESS


    def __scrap_multiple_web_data_to_single_csv_file(self):
        # if not isinstance(timeslice_list, list):
        #     raise ValueError("timeslice_list is NOT a list")
        self.__reset_time_slice_buffer()
        for timeslice in self.timeslice_iterable:
            timeslice_buffer = self.__add_to_time_slice_buffer(timeslice)
            if timeslice_buffer is None:
                continue
# Write multiple ULR data into one CSV file
            csv_filepath = self.assemble_csv_filepath(timeslice_buffer)
            total_csv_data_cnt = 0
            with open(csv_filepath, 'w') as fp:
                fp_writer = csv.writer(fp, delimiter=',')
                for same_timeunit_timeslice in self.timeslice_iterable:
                    url = self.assemble_web_url(same_timeunit_timeslice)
                    g_logger.debug("Get the data from URL: %s" % url)
                    try:
 # Grab the data from website in each time slice and assemble the data to the entry of CSV
                        web_data_list = self.parse_web_data(self.__get_web_data(url))
                        if web_data_list is None:
                            raise RuntimeError(url)
                        csv_data_list = []
                        for web_data in web_data_list:
                        # g_logger.debug("Write the data[%s] to %s" % (web_data, self.csv_filename))
                            csv_data_list.append(web_data)
# Write the web data into CSV
                        csv_data_list_len = len(csv_data_list)
                        if csv_data_list_len > 0:
                            g_logger.debug("Write %d data to %s" % (csv_data_list_len, csv_filepath))
                            fp_writer.writerows(csv_data_list)
                            total_csv_data_cnt += csv_data_list_len

                except requests.exceptions.Timeout as e:
                        g_logger.error("Fail to scrap URL[%s], due to: Time-out" % url)
                        raise e
                    except Exception as e:
                        g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e))) 

            if total_csv_data_cnt == 0:
                g_logger.warn("Emtpy data, remove the CSV file: %s" % csv_filepath)
                try:
                    os.remove(csv_filepath)
                except OSError as e:  ## if failed, report it back to the user ##
                    g_logger.error("Fail to remove the CSV file: %s, due to: %s" % (csv_filepath, str(e)))

        return CMN.RET_SUCCESS


    def __scrap_single_web_data_to_single_csv_file(self):
        # if isinstance(timeslice, list):
        #     raise ValueError("timeslice should NOT be a list")
        for timeslice in self.timeslice_iterable:
            url = self.assemble_web_url(timeslice)
            g_logger.debug("Get the data by date from URL: %s" % url)
            try:
# Grab the data from website and assemble the data to the entry of CSV
                csv_data_list = self.parse_web_data(self.__get_web_data(url))
                if csv_data_list is None:
                    raise RuntimeError(url)
                csv_filepath = self.assemble_csv_filepath(timeslice)
                g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
                with open(csv_filepath, 'w') as fp:
                    fp_writer = csv.writer(fp, delimiter=',')
# Write the web data into CSV
                    fp_writer.writerows(csv_data_list)
            except Exception as e:
                g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))

        return CMN.RET_SUCCESS


    def scrape_web_to_csv(self):
        # import pdb; pdb.set_trace()
        return self.scrape_web_to_csv_func_ptr()


#############################################################################################


class WebScrapyMarketBase(WebScrapyBase):

    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyMarketBase, self).__init__(cur_file_path, **kwargs)


    def assemble_web_url(self, datetime_cfg):
        raise NotImplementedError

#############################################################################################


class WebScrapyStockBase(WebScrapyBase):

    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyStockBase, self).__init__(cur_file_path, **kwargs)


    def assemble_web_url(self, datetime_cfg, company_code_number):
        raise NotImplementedError