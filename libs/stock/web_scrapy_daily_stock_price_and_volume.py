# -*- coding: utf8 -*-

import re
import requests
# import csv
# from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 個股日股價及成交量
class WebScrapyDailyStockPriceAndVolume(WebScrapyStockBase.WebScrapyStockBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month))
        return url


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyDailyStockPriceAndVolume, self).__init__(**kwargs)
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


    def prepare_for_scrapy(self, timeslice):
        # import pdb; pdb.set_trace()
        assert isinstance(timeslice, CMN.CLS.FinanceMonth), "The input time duration time unit is %s, not FinanceMonth" % type(timeslice)
        url = self.assemble_web_url(timeslice)
# Check if it's no need to acquire the whole month data in this month
        try:
            index = self.data_not_whole_month_list.index(timeslice)
            self.whole_month_data = False
        except ValueError:
            self.whole_month_data = True
        return url


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = []
        for data_entry in web_data:
            date_list = str(data_entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            entry = [CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2])),]
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
