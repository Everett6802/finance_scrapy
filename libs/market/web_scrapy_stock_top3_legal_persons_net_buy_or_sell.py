# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


# 三大法人買賣金額統計表
class WebScrapyStockTop3LegalPersonsNetBuyOrSell(web_scrapy_base.WebScrapyMarketBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyStockTop3LegalPersonsNetBuyOrSell, self).__init__(
    #         # "http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date={0}%2F{1}%2F{2}&mSubmit=%ACd%B8%DF&yr=1979&w_date=19790904&m_date=19790904", 
    #         __file__
    #         # CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    #         # datetime_range_start, 
    #         # datetime_range_end
    #     )
    def __init__(self, **kwargs):
        super(WebScrapyStockTop3LegalPersonsNetBuyOrSell, self).__init__(__file__, **kwargs)


    def assemble_web_url(self, timeslice):
        url = self.url_format.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        data_list = []
        for tr in web_data[2:6]:
            td = tr.select('td')
            for i in range(1, 4):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
        return data_list
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


    def do_debug(self):
        res = requests.get("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=19790904&m_date=19790904")
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        # print soup
        for tr in soup.select('.board_trad tr')[2:6]:
            td = tr.select('td')
            print td[0].text, td[1].text, td[2].text, td[3].text 
# ==== result: ====
# 自營商(自行買賣) 976,637,210 830,450,307 146,186,903
# 自營商(避險) 4,858,793,774 5,360,634,883 -501,841,109
# 投信 842,037,060 1,269,238,502 -427,201,442
# 外資及陸資 12,382,715,256 15,582,342,791 -3,199,627,535