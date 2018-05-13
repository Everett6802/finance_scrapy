# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import scrapy_market_base as ScrapyMarketBase
g_logger = CMN.LOG.get_logger()


# 臺指選擇權買賣權未平倉口數與契約金額
class OptionTop3LegalPersonsBuyAndSellOptionOpenInterest(ScrapyMarketBase.ScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, timeslice.day))
        return url


    def __init__(self, **kwargs):
        super(OptionTop3LegalPersonsBuyAndSellOptionOpenInterest, self).__init__(**kwargs)
        self.cur_date_str = None


    def _scrape_web_data(self, timeslice):
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        data_list = [self.cur_date_str,]
        # start_index_list = [4, 1, 1, 2, 1, 1]
        # column_num = 12
        start_index_list = [10, 7, 7, 8, 7, 7]
        column_num = 6
        row_index = 0
        for tr in web_data[3:9]:
            start_index = start_index_list[row_index]
            td = tr.select('td')
            for i in range(start_index, start_index + column_num):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
            row_index += 1
        return data_list
# *** 未平倉餘額 ***
# "日期",
# "買權_自營商_買方_口數",
# "買權_自營商_買方_契約金額",
# "買權_自營商_賣方_口數",
# "買權_自營商_賣方_契約金額",
# "買權_自營商_買賣差額_口數",
# "買權_自營商_買賣差額_契約金額",
# "買權_投信_買方_口數",
# "買權_投信_買方_契約金額",
# "買權_投信_賣方_口數",
# "買權_投信_賣方_契約金額",
# "買權_投信_買賣差額_口數",
# "買權_投信_買賣差額_契約金額",
# "買權_外資_買方_口數",
# "買權_外資_買方_契約金額",
# "買權_外資_賣方_口數",
# "買權_外資_賣方_契約金額",
# "買權_外資_買賣差額_口數",
# "買權_外資_買賣差額_契約金額",
# "賣權_自營商_買方_口數",
# "賣權_自營商_買方_契約金額",
# "賣權_自營商_賣方_口數",
# "賣權_自營商_賣方_契約金額",
# "賣權_自營商_買賣差額_口數",
# "賣權_自營商_買賣差額_契約金額",
# "賣權_投信_買方_口數",
# "賣權_投信_買方_契約金額",
# "賣權_投信_賣方_口數",
# "賣權_投信_賣方_契約金額",
# "賣權_投信_買賣差額_口數",
# "賣權_投信_買賣差額_契約金額",
# "賣權_外資_買方_口數",
# "賣權_外資_買方_契約金額",
# "賣權_外資_賣方_口數",
# "賣權_外資_賣方_契約金額",
# "賣權_外資_買賣差額_口數",
# "賣權_外資_買賣差額_契約金額",


    @staticmethod
    def do_debug(silent_mode=False):
        # res = requests.get("http://www.taifex.com.tw/chinese/3/7_12_5.asp?goday=&DATA_DATE_Y=2015&DATA_DATE_M=11&DATA_DATE_D=3&syear=2015&smonth=11&sday=3&datestart=2015%2F11%2F3&COMMODITY_ID=TXO")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.taifex.com.tw/chinese/3/7_12_5.asp?goday=&DATA_DATE_Y=2015&DATA_DATE_M=11&DATA_DATE_D=3&syear=2015&smonth=11&sday=3&datestart=2015%2F11%2F3&COMMODITY_ID=TXO")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_f tr')
        start_index_list = [4, 1, 1, 2, 1, 1]
        column_num = 12
        row_index = 0
        for tr in g_data[3:9]:
            start_index = start_index_list[row_index]
            data_str = ""
            td = tr.select('td')
            for i in range(start_index, start_index + column_num):
                data_str += "%s " % td[i].text
            row_index += 1
            if not silent_mode: print data_str
# ==== result: ====
# 169,747 379,279 158,878 326,883 10,869 52,396 80,822 456,221 105,149 300,919 -24,327 155,302 
# 15 54 12 6 3 48 207 2,090 173 1,186 34 904 
# 35,573 124,186 33,770 117,497 1,803 6,689 142,950 422,246 65,224 272,303 77,726 149,943 
# 162,454 215,529 177,648 202,376 -15,194 13,153 99,565 186,431 141,531 311,393 -41,966 -124,962 
# 6 13 0 0 6 13 100 151 159 141 -59 10 
# 30,671 61,909 23,985 52,580 6,686 9,330 118,567 507,286 48,024 279,445 70,543 227,841 
