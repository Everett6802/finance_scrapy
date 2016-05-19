# -*- coding: utf8 -*-

import os
import sys
import re
import requests
# import csv
# import shutil
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


@CMN_CLS.Singleton
class WebScrapyTimeSliceGenerator(object):

    def __init__(self):
        self.workday_canlendar = None
        self.generate_time_slice_func_ptr = [
            self__generate_time_slice_by_day,
            self__generate_time_slice_by_company_foreign_investors_shareholder,
            self__generate_time_slice_by_month,
            self__generate_time_slice_by_quarter,
        ]
        self.company_foreign_investors_shareholder_timeslice_list = None


    def initialize(self):
        pass
        # import pdb; pdb.set_trace()


    def __generate_time_slice_by_day(self, start_time_cfg, end_time_cfg):
        pass


    def __generate_time_slice_by_company_foreign_investors_shareholder(self, start_time_cfg, end_time_cfg):
# The data type in the list is datetime
        if self.company_foreign_investors_shareholder_timeslice_list is None:
            res = requests.get("https://www.tdcc.com.tw/smWeb/QryStock.jsp?SqlMethod=StockNo&StockNo=2347&StockName=&sub=%ACd%B8%DF")
            res.encoding = 'big5'
            soup = BeautifulSoup(res.text)
            g_data = soup.select('table tbody tr option')
            self.company_foreign_investors_shareholder_timeslice_list = []
            for index in range(len(g_data)):
                time_str = g_data[index].text
                year = int(time_str[0:4])
                month = int(time_str[4:6])
                day = int(time_str[6:8])
                self.company_foreign_investors_shareholder_timeslice_list.append(datetime(year, month, day))
        timeslice_list = []
        for timeslice in self.company_foreign_investors_shareholder_timeslice_list:
            timeslice_list.append(timeslice)
        return timeslice_list


    def __generate_time_slice_by_month(self, start_time_cfg, end_time_cfg):
# The data type in the list is datetime
        cur_datetime_cfg = start_time_cfg
        timeslice_list = []
        while True:
            timeslice_list.append(datetime(cur_datetime_cfg.year, cur_datetime_cfgr.month, 1))
            last_day = CMN.get_cfg_month_last_day(cur_datetime_cfg)
            if datetime_range_end.year == datetime_cur.year and datetime_range_end.month == datetime_cur.month:
                break
            datetime_cur += timedelta(days = last_day)
        return timeslice_list


    def __generate_time_slice_by_quarter(self, start_time_cfg, end_time_cfg):
        pass


    def generate_time_slice(self, time_slice_type, start_time_cfg, end_time_cfg):
        if start_time_cfg > end_time_cfg:
            raise RuntimeError("The Start Time[%s] should NOT be greater than the End Time[%s]" % (start_time_cfg, end_time_cfg))
        return (self.generate_time_slice_func_ptr[time_slice_type])(start_time_cfg, end_time_cfg)