# -*- coding: utf8 -*-

import re
import requests
# import csv
# from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import libs.common as CMN
import scrapy_market_base as ScrapyMarketBase
g_logger = CMN.LOG.get_logger()


# 三大法人買賣金額統計表
class StockTop3LegalPersonsNetBuyOrSell(ScrapyMarketBase.ScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        # import pdb; pdb.set_trace()
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, timeslice.day))
        return url


    def __init__(self, **kwargs):
        super(StockTop3LegalPersonsNetBuyOrSell, self).__init__(**kwargs)
        self.cur_date_str = None


    def _scrape_web_data(self, timeslice):
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = [self.cur_date_str,]
        for entry in web_data[:4]:
            for i in range(1, 4):
                element = str(entry[i]).replace(',', '')
                data_list.append(element)
        return data_list
# 日期
# 自營商(自行買賣)_買進金額
# 自營商(自行買賣)_賣出金額
# 自營商(自行買賣)_買賣差額
# 自營商(避險)_買進金額
# 自營商(避險)_賣出金額
# 自營商(避險)_買賣差額
# 投信_買進金額
# 投信_賣出金額
# 投信_買賣差額
# 外資及陸資_買進金額
# 外資及陸資_賣出金額
# 外資及陸資_買賣差額


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # # res = requests.get("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=19790904&m_date=19790904")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=19790904&m_date=19790904")
        # res.encoding = 'big5'
        # soup = BeautifulSoup(res.text)
        # # print soup
        # for tr in soup.select('.board_trad tr')[2:6]:
        #     td = tr.select('td')
        #     if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text 
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/fund/BFI82U?response=json&dayDate=20170601&type=day")
        res.encoding = 'utf-8'
        g_data = json.loads(res.text)['data']
        for entry in g_data:
            if not silent_mode: print entry[0], entry[1], entry[2], entry[3]
# ==== result: ====
# 自營商(自行買賣) 1,145,643,710 1,160,896,650 -15,252,940
# 自營商(避險) 4,252,785,932 3,252,168,465 1,000,617,467
# 投信 843,775,750 1,234,385,726 -390,609,976
# 外資及陸資 25,127,388,391 22,560,979,224 2,566,409,167
# 合計 31,369,593,783 28,208,430,065 3,161,163,718
