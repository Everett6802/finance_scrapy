# -*- coding: utf8 -*-

import re
import requests
# import csv
# from bs4 import BeautifulSoup
import json
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
# import web_scrapy_company_profile as CompanyProfile
import scrapy_stock_base as ScrapyStockBase
g_logger = CMN.LOG.get_logger()


# 個股日股價及成交量
class DailyStockPriceAndVolumeBase(ScrapyStockBase.ScrapyStockBase):

    __metaclass__ = ABCMeta

    URL_DATA_EXIST_SELECTOR = None
    START_DATE_OBJ = None

#     @classmethod
#     def init_class_customized_variables(cls):
#         # import pdb; pdb.set_trace()
# # CAUTION: This function MUST be called by the LEAF derived class
#         if cls.URL_DATA_EXIST_SELECTOR is None:
#             cls.URL_DATA_EXIST_SELECTOR = cls.CLASS_CONSTANT_CFG["url_data_exist_selector"]
#         if cls.START_DATE_OBJ is None:
#             cls.START_DATE_OBJ = CMN.CLS.FinanceDate.from_string(CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR)


    # @classmethod
    # def assemble_web_url(cls, timeslice, company_code_number, *args):
    #     url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, company_code_number))
    #     return url


    @classmethod
    def _transform_data_for_writing_to_csv(cls, csv_data_list):
        return cls._transform_multi_data_in_one_page(csv_data_list)


    @classmethod
    def pre_check_web_data(cls, req):
        json_url_data = json.loads(req.text)
        json_url_stat_data = json_url_data[cls.URL_DATA_EXIST_SELECTOR]
        if not re.search(r"OK", json_url_stat_data, re.U):
            # import pdb; pdb.set_trace()
            if re.search(r"沒有符合條件的資料", json_url_stat_data.encode(CMN.DEF.URL_ENCODING_UTF8), re.U):
                raise CMN.EXCEPTION.WebScrapyNotFoundException("The data does NOT exist on the web")
            else:
                raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Unkown JSON URL data state: %s" % json_url_stat_data)


    @classmethod
    def get_first_web_data_time(cls, web_data):
        # import pdb; pdb.set_trace()
        assert len(web_data) != 0, "The web_data shuld NOT be empty"
        first_web_data_entry = web_data[0]
        date_list = str(first_web_data_entry[0]).split('/')
        if len(date_list) != 3:
            raise RuntimeError("The date format is NOT as expected: %s", date_list)
        return CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))


    # @classmethod
    # def find_time_range_start(cls, company_code_number):
    #     # import pdb; pdb.set_trace()
    #     profile_lookup = BASE.CP.CompanyProfile.Instance()
    #     listing_date_str = None
    #     try:
    #         listing_date_unicode = profile_lookup.lookup_company_listing_date(company_code_number)
    #         listing_date_str = str(listing_date_unicode)
    #         if CMN.CLS.FinanceDate.from_string(listing_date_str) < cls.START_DATE_OBJ:
    #             listing_date_str = CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR
    #     except ValueError:
    #         g_logger.debug("The profile of the company code number does NOT exist")
    #     return listing_date_str


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(DailyStockPriceAndVolumeBase, self).__init__(**kwargs)
        self.whole_month_data = True
        self.time_duration_start_after_adjustment = self.xcfg["time_duration_start"]
        self.time_duration_end_after_adjustment = self.xcfg["time_duration_end"]
        self.data_not_whole_month_list = None


    def _adjust_config_before_scrapy(self, *args):
        # import pdb; pdb.set_trace()
# args[0]: time duration start
# args[1]: time duration end
        web2csv_time_duration_update = args[0]
        self.time_duration_start_after_adjustment = web2csv_time_duration_update.NewWebStart
        self.time_duration_end_after_adjustment = web2csv_time_duration_update.NewWebEnd
        self.data_not_whole_month_list = CMN.FUNC.get_data_not_whole_month_list(self.time_duration_start_after_adjustment, self.time_duration_end_after_adjustment)


    def _scrape_web_data(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        assert isinstance(timeslice, CMN.CLS.FinanceMonth), "The input time duration time unit is %s, not FinanceMonth" % type(timeslice)
# Check if it's no need to acquire the whole month data in this month
        try:
            index = self.data_not_whole_month_list.index(timeslice)
            self.whole_month_data = False
        except ValueError:
            self.whole_month_data = True
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = []
        for data_entry in web_data:
            date_list = str(data_entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            entry = [CMN.FUNC.transform_date_str(int(date_list[0]) + 1911, int(date_list[1]), int(date_list[2])),]
            if not self.whole_month_data:
                date_cur = CMN.CLS.FinanceDate.from_string(entry[0])
                if date_cur < self.time_duration_start_after_adjustment:
                    continue
                elif date_cur > self.time_duration_end_after_adjustment:
                    break
            for index in range(1, 9):
                entry.append(str(data_entry[index]).replace(',', ''))
            data_list.append(entry)
        return data_list
# "日期",
# "成交股數",
# "成交金額",
# "開盤價",
# "最高價",
# "最低價",
# "收盤價",
# "漲跌價差",
# "成交筆數",

#####################################################################

# 上市個股日股價及成交量
class DailyStockPriceAndVolume(DailyStockPriceAndVolumeBase):

    # URL_DATA_EXIST_SELECTOR = None
    # START_DATE_OBJ = None

    @classmethod
    def init_class_customized_variables(cls):
        # import pdb; pdb.set_trace()
# CAUTION: This function MUST be called by the LEAF derived class
        if cls.URL_DATA_EXIST_SELECTOR is None:
            cls.URL_DATA_EXIST_SELECTOR = cls.CLASS_CONSTANT_CFG["url_data_exist_selector"]
        # if cls.START_DATE_OBJ is None:
        #     cls.START_DATE_OBJ = CMN.CLS.FinanceDate.from_string(CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR)


    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, company_code_number))
        return url


    @classmethod
    def pre_check_web_data(cls, req):
        json_url_data = json.loads(req.text)
        json_url_stat_data = json_url_data[cls.URL_DATA_EXIST_SELECTOR]
        if not re.search(r"OK", json_url_stat_data, re.U):
            # import pdb; pdb.set_trace()
            if re.search(r"沒有符合條件的資料", json_url_stat_data.encode(CMN.DEF.URL_ENCODING_UTF8), re.U):
                raise CMN.EXCEPTION.WebScrapyNotFoundException("The data does NOT exist on the web")
            else:
                raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Unkown JSON URL data state: %s" % json_url_stat_data)


    # @classmethod
    # def get_first_web_data_time(cls, web_data):
    #     # import pdb; pdb.set_trace()
    #     assert len(web_data) != 0, "The web_data shuld NOT be empty"
    #     first_web_data_entry = web_data[0]
    #     date_list = str(first_web_data_entry[0]).split('/')
    #     if len(date_list) != 3:
    #         raise RuntimeError("The date format is NOT as expected: %s", date_list)
    #     return CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))


    # @classmethod
    # def find_time_range_start(cls, company_code_number):
    #     # import pdb; pdb.set_trace()
    #     profile_lookup = BASE.CP.CompanyProfile.Instance()
    #     listing_date_str = None
    #     try:
    #         listing_date_unicode = profile_lookup.lookup_company_listing_date(company_code_number)
    #         listing_date_str = str(listing_date_unicode)
    #         if CMN.CLS.FinanceDate.from_string(listing_date_str) < cls.START_DATE_OBJ:
    #             listing_date_str = CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR
    #     except ValueError:
    #         g_logger.debug("The profile of the company code number does NOT exist")
    #     return listing_date_str


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(DailyStockPriceAndVolume, self).__init__(**kwargs)
        # self.whole_month_data = True
        # self.time_duration_start_after_adjustment = self.xcfg["time_duration_start"]
        # self.time_duration_end_after_adjustment = self.xcfg["time_duration_end"]
        # self.data_not_whole_month_list = None


#     def _adjust_config_before_scrapy(self, *args):
#         # import pdb; pdb.set_trace()
# # args[0]: time duration start
# # args[1]: time duration end
#         web2csv_time_duration_update = args[0]
#         self.time_duration_start_after_adjustment = web2csv_time_duration_update.NewWebStart
#         self.time_duration_end_after_adjustment = web2csv_time_duration_update.NewWebEnd
#         self.data_not_whole_month_list = CMN.FUNC.get_data_not_whole_month_list(self.time_duration_start_after_adjustment, self.time_duration_end_after_adjustment)


#     def _scrape_web_data(self, timeslice, company_code_number):
#         # import pdb; pdb.set_trace()
#         assert isinstance(timeslice, CMN.CLS.FinanceMonth), "The input time duration time unit is %s, not FinanceMonth" % type(timeslice)
# # Check if it's no need to acquire the whole month data in this month
#         try:
#             index = self.data_not_whole_month_list.index(timeslice)
#             self.whole_month_data = False
#         except ValueError:
#             self.whole_month_data = True
#         url = self.assemble_web_url(timeslice, company_code_number)
#         web_data = self.try_get_web_data(url)
#         return web_data


    # def _parse_web_data(self, web_data):
    #     # import pdb; pdb.set_trace()
    #     data_list = []
    #     for data_entry in web_data:
    #         date_list = str(data_entry[0]).split('/')
    #         if len(date_list) != 3:
    #             raise RuntimeError("The date format is NOT as expected: %s", date_list)
    #         entry = [CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2])),]
    #         if not self.whole_month_data:
    #             date_cur = CMN.CLS.FinanceDate.from_string(entry[0])
    #             if date_cur < self.time_duration_start_after_adjustment:
    #                 continue
    #             elif date_cur > self.time_duration_end_after_adjustment:
    #                 break
    #         for index in range(1, 9):
    #             entry.append(str(data_entry[index]).replace(',', ''))
    #         data_list.append(entry)
    #     return data_list
# "日期",
# "成交股數",
# "成交金額",
# "開盤價",
# "最高價",
# "最低價",
# "收盤價",
# "漲跌價差",
# "成交筆數",


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20170501&stockNo=1589")
        # print res.text
        res.encoding = 'utf-8'
        g_data = json.loads(res.text)['data']
        for entry in g_data:
            date_list = str(entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            date_str = CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))
            if not silent_mode: print date_str, entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7], entry[8]
# 106-05-02 1,131,650 104,036,430 92.60 93.10 91.10 91.10 -0.40 893
# 106-05-03 512,561 46,508,751 91.80 91.80 90.10 90.50 -0.60 439
# 106-05-04 807,488 74,133,393 90.90 92.70 90.20 91.70 +1.20 658
# 106-05-05 942,780 85,324,201 91.00 91.10 89.80 90.80 -0.90 717
# 106-05-08 1,441,200 127,662,560 90.50 91.80 86.20 87.00 -3.80 1,136
# 106-05-09 1,808,250 152,720,197 87.10 87.10 83.10 83.10 -3.90 1,374
# 106-05-10 1,722,810 143,283,730 82.70 85.30 81.90 82.70 -0.40 1,169
# 106-05-11 706,450 59,077,755 83.30 84.50 83.10 83.30 +0.60 587
# 106-05-12 965,200 80,008,560 83.30 84.10 82.10 82.40 -0.90 706
# 106-05-15 1,120,228 94,990,008 82.40 85.80 82.40 85.70 +3.30 820
# 106-05-16 1,060,123 90,031,641 86.80 86.90 84.40 84.80 -0.90 728
# 106-05-17 879,615 74,084,365 85.30 85.30 83.70 84.60 -0.20 782
# 106-05-18 1,003,111 84,048,068 84.60 84.70 83.00 83.00 -1.60 632
# 106-05-19 453,540 37,832,256 83.80 84.30 83.20 83.80 +0.80 396
# 106-05-22 317,655 26,693,520 83.50 84.40 83.50 84.20 +0.40 266
# 106-05-23 510,739 43,481,988 84.20 85.60 84.20 85.10 +0.90 418
# 106-05-24 363,107 30,948,648 85.40 85.60 85.00 85.30 +0.20 339
# 106-05-25 1,891,733 167,076,702 85.30 90.00 85.30 89.70 +4.40 1,419
# 106-05-26 1,263,469 112,229,506 90.00 90.60 87.70 88.10 -1.60 952
# 106-05-31 578,415 50,673,744 88.20 89.10 86.90 86.90 -1.20 374

#####################################################################

# 上櫃個股日股價及成交量
class OTCDailyStockPriceAndVolume(DailyStockPriceAndVolumeBase):

    # URL_DATA_EXIST_SELECTOR = None
    # START_DATE_OBJ = None

    @classmethod
    def init_class_customized_variables(cls):
        # import pdb; pdb.set_trace()
# CAUTION: This function MUST be called by the LEAF derived class
        if cls.URL_DATA_EXIST_SELECTOR is None:
            cls.URL_DATA_EXIST_SELECTOR = cls.CLASS_CONSTANT_CFG["url_data_exist_selector"]
        # if cls.START_DATE_OBJ is None:
        #     cls.START_DATE_OBJ = CMN.CLS.FinanceDate.from_string(CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR)


    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year - 1911, timeslice.month, company_code_number))
        return url


    @classmethod
    def pre_check_web_data(cls, req):
        json_url_data = json.loads(req.text)
        json_url_stat_data = json_url_data[cls.URL_DATA_EXIST_SELECTOR]
        if json_url_stat_data == 0:
            raise CMN.EXCEPTION.WebScrapyNotFoundException("The data does NOT exist on the web")


    # @classmethod
    # def get_first_web_data_time(cls, web_data):
    #     # import pdb; pdb.set_trace()
    #     assert len(web_data) != 0, "The web_data shuld NOT be empty"
    #     first_web_data_entry = web_data[0]
    #     date_list = str(first_web_data_entry[0]).split('/')
    #     if len(date_list) != 3:
    #         raise RuntimeError("The date format is NOT as expected: %s", date_list)
    #     return CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))


    # @classmethod
    # def find_time_range_start(cls, company_code_number):
    #     # import pdb; pdb.set_trace()
    #     profile_lookup = BASE.CP.CompanyProfile.Instance()
    #     listing_date_str = None
    #     try:
    #         listing_date_unicode = profile_lookup.lookup_company_listing_date(company_code_number)
    #         listing_date_str = str(listing_date_unicode)
    #         if CMN.CLS.FinanceDate.from_string(listing_date_str) < cls.START_DATE_OBJ:
    #             listing_date_str = CMN.DEF.DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR
    #     except ValueError:
    #         g_logger.debug("The profile of the company code number does NOT exist")
    #     return listing_date_str


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(OTCDailyStockPriceAndVolume, self).__init__(**kwargs)
        # self.whole_month_data = True
        # self.time_duration_start_after_adjustment = self.xcfg["time_duration_start"]
        # self.time_duration_end_after_adjustment = self.xcfg["time_duration_end"]
        # self.data_not_whole_month_list = None


#     def _adjust_config_before_scrapy(self, *args):
#         # import pdb; pdb.set_trace()
# # args[0]: time duration start
# # args[1]: time duration end
#         web2csv_time_duration_update = args[0]
#         self.time_duration_start_after_adjustment = web2csv_time_duration_update.NewWebStart
#         self.time_duration_end_after_adjustment = web2csv_time_duration_update.NewWebEnd
#         self.data_not_whole_month_list = CMN.FUNC.get_data_not_whole_month_list(self.time_duration_start_after_adjustment, self.time_duration_end_after_adjustment)


#     def _scrape_web_data(self, timeslice, company_code_number):
#         # import pdb; pdb.set_trace()
#         assert isinstance(timeslice, CMN.CLS.FinanceMonth), "The input time duration time unit is %s, not FinanceMonth" % type(timeslice)
# # Check if it's no need to acquire the whole month data in this month
#         try:
#             index = self.data_not_whole_month_list.index(timeslice)
#             self.whole_month_data = False
#         except ValueError:
#             self.whole_month_data = True
#         url = self.assemble_web_url(timeslice, company_code_number)
#         web_data = self.try_get_web_data(url)
#         return web_data


    # def _parse_web_data(self, web_data):
    #     # import pdb; pdb.set_trace()
    #     data_list = []
    #     for data_entry in web_data:
    #         date_list = str(data_entry[0]).split('/')
    #         if len(date_list) != 3:
    #             raise RuntimeError("The date format is NOT as expected: %s", date_list)
    #         entry = [CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2])),]
    #         if not self.whole_month_data:
    #             date_cur = CMN.CLS.FinanceDate.from_string(entry[0])
    #             if date_cur < self.time_duration_start_after_adjustment:
    #                 continue
    #             elif date_cur > self.time_duration_end_after_adjustment:
    #                 break
    #         for index in range(1, 9):
    #             entry.append(str(data_entry[index]).replace(',', ''))
    #         data_list.append(entry)
    #     return data_list
# "日期",
# "成交股數",
# "成交金額",
# "開盤價",
# "最高價",
# "最低價",
# "收盤價",
# "漲跌價差",
# "成交筆數",


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=106/07&stkno=4739")
        # print res.text
        res.encoding = 'utf-8'
        g_data = json.loads(res.text)['aaData']
        for entry in g_data:
            date_list = str(entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            date_str = CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))
            if not silent_mode: print date_str, entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7], entry[8]
# 106-07-03 2,297 159,522 69.00 70.10 68.50 69.90 1.40 987
# 106-07-04 2,288 156,224 68.00 69.10 67.60 67.80 0.60 1,420
# 106-07-05 2,139 147,285 68.60 69.20 68.30 68.50 0.70 1,483
# 106-07-06 1,015 69,309 68.20 68.70 67.70 68.10 -0.40 662
# 106-07-07 1,923 128,558 67.70 68.20 65.90 66.40 -1.70 1,168
# 106-07-10 888 59,086 67.00 67.00 66.00 66.00 -0.40 592
# 106-07-11 1,548 100,858 66.20 66.20 64.40 65.10 -0.90 982
# 106-07-12 3,051 206,717 65.80 69.00 65.80 68.50 3.40 1,930
# 106-07-13 1,617 111,017 68.80 69.20 68.00 68.80 0.30 952
# 106-07-14 971 66,844 69.60 69.60 68.30 68.50 -0.30 622
# 106-07-17 708 48,770 68.90 69.30 68.50 69.00 0.50 437
# 106-07-18 7,879 579,960 69.30 75.90 68.80 75.90 6.90 4,719
# 106-07-19 12,183 952,310 77.90 79.60 76.20 77.60 1.70 7,477
# 106-07-20 7,472 591,813 77.50 80.20 77.10 79.20 1.60 4,605
# 106-07-21 3,909 309,331 79.50 80.60 78.10 78.90 -0.30 2,281
# 106-07-24 3,004 239,052 78.30 80.50 78.30 80.00 1.10 1,947
# 106-07-25 3,247 259,887 80.70 81.50 79.10 79.10 -0.90 2,091
# 106-07-26 4,170 335,071 79.50 81.70 78.60 80.40 1.30 2,626
# 106-07-27 4,701 383,022 82.00 83.30 80.00 80.00 -0.40 2,983
# 106-07-28 2,404 189,424 79.60 79.90 78.00 78.00 -2.00 1,511
# 106-07-31 2,512 199,323 78.00 80.60 78.00 80.60 2.60 1,440
