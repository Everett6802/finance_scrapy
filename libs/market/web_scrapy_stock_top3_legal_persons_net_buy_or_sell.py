# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 三大法人買賣金額統計表
class WebScrapyStockTop3LegalPersonsNetBuyOrSell(WebScrapyMarketBase.WebScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = self.URL_FORMAT.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, **kwargs):
        super(WebScrapyStockTop3LegalPersonsNetBuyOrSell, self).__init__(__file__, **kwargs)
        self.cur_date_str = None


    def prepare_for_scrapy(self, timeslice):
        url = self.assemble_web_url(timeslice)
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        return url


    def _parse_web_data(self, web_data):
        data_list = [self.cur_date_str,]
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


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=19790904&m_date=19790904")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=19790904&m_date=19790904")
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        # print soup
        for tr in soup.select('.board_trad tr')[2:6]:
            td = tr.select('td')
            if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text 
# ==== result: ====
# 自營商(自行買賣) 976,637,210 830,450,307 146,186,903
# 自營商(避險) 4,858,793,774 5,360,634,883 -501,841,109
# 投信 842,037,060 1,269,238,502 -427,201,442
# 外資及陸資 12,382,715,256 15,582,342,791 -3,199,627,535
