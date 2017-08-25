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


# 外資及陸資買賣超彙總表
class WebScrapyCompanyForeignInvestorsNetBuyOrSellSummary(web_scrapy_base.WebScrapyBase):

    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
# CAUTION: This function MUST be called by the LEAF derived class
        url = self.URL_FORMAT.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        super(WebScrapyCompanyForeignInvestorsNetBuyOrSellSummary, self).__init__(
            "http://www.twse.com.tw/ch/trading/fund/TWT38U/TWT38U.php?download=&qdate={0}%2F{1}%2F{2}&sorting=by_stkno", 
            __file__, 
            'utf-8', 
            'table tr', 
            datetime_range_start, 
            datetime_range_end
        )


    def _scrape_web_data(self, timeslice, company_code_number):
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        data_list = []
        element_list = []
        for tr in web_data[2:]:
            td = tr.select('td')
            # for i in range(1, 3):
            element = str(td[1].text).strip(' ')
            element_list.append(element)
            for i in range(3, 6):
                element = str(td[i].text).replace(',', '')
                element_list.append(element)
            data_list.append(element_list)
        return data_list
# 證券代號
# 證券名稱
# 買進股數
# 賣出股數
# 買賣超股數


    def do_debug(self):
        # res = requests.get("http://www.twse.com.tw/ch/trading/fund/TWT38U/TWT38U.php?download=&qdate=105%2F03%2F25&sorting=by_stkno")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/fund/TWT38U/TWT38U.php?download=&qdate=105%2F03%2F25&sorting=by_stkno")
        #print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        for tr in g_data[2:]:
            td = tr.select('td')
            print td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
# ==== result: ====
# 0050   台灣50           597,000 315,000 282,000
# 0055   寶金融           18,000 0 18,000
# 0056   高股息           35,000 0 35,000
# 006205 FB上証           0 115,000 -115,000
# ......
