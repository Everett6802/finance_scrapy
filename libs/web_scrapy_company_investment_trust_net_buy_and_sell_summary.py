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


# 投信買賣超彙總表
class WebScrapyCompanyInvestmentTrustNetBuyOrSellSummary(web_scrapy_base.WebScrapyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        super(WebScrapyCompanyInvestmentTrustNetBuyOrSellSummary, self).__init__(
            "http://www.twse.com.tw/ch/trading/fund/TWT44U/TWT44U.php?download=&qdate={0}%2F{1}%2F{2}&sorting=by_stkno", 
            __file__, 
            'utf-8', 
            'table tr', 
            datetime_range_start, 
            datetime_range_end
        )


    def assemble_web_url(self, datetime_cfg):
        url = self.url_format.format(
            *(
                datetime_cfg.year - 1911, 
                "%02d" % datetime_cfg.month,
                "%02d" % datetime_cfg.day
            )
        )
        return url


    def parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        data_list = []
        for tr in web_data[2:]:
            td = tr.select('td')
            # for i in range(1, 3):
            data_list.append(str(td[1].text).strip(' '))
            for i in range(3, 6):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
        return data_list
# 證券代號
# 證券名稱
# 買進股數
# 賣出股數
# 買賣超股數


    def do_debug(self):
        res = requests.get("http://www.twse.com.tw/ch/trading/fund/TWT44U/TWT44U.php?download=&qdate=105%2F03%2F25&sorting=by_stkno")
        #print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        for tr in g_data[2:]:
            td = tr.select('td')
            print td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
# ==== result: ====
# 00632R T50反1           250,000 0 250,000
# 00633L 上証2X           31,000 0 31,000
# 00637L 滬深2X           25,000 0 25,000
# 00642U 元石油           0 300,000 -300,000
# ......
