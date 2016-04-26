# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


# 上市櫃公司代號
class WebScrapyCompanyStockMarketCodeNumber(web_scrapy_base.WebScrapyBase):

    def __init__(self, market_type):
        self.market_type = market_type
        self.str_mode = None
        url_format = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=%d"
        if self.market_type == CMN.MARKET_TYPE_STOCK_EXCHANGE:
            self.str_mode = 2
        elif self.market_type == CMN.MARKET_TYPE_OVER_THE_COUNTER:
            self.str_mode = 4
        else:
            raise ValueError("Unknown Market Type: %d", self.market_type)
        url = url_format % self.str_mode

        super(WebScrapyCompanyStockMarketCodeNumber, self).__init__(
            url, 
            __file__, 
            CMN_CLS.ParseURLDataByBS4('big5', 'table tr')
        )


    def assemble_web_url(self, datetime_cfg):
        raise RuntimeError("No need to run this function")


    def parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None

        data_list = []
        for tr in web_data[2:]:
            td = tr.select('td')
            if len(td) != 7:
                continue
            company_number_and_name = "%s" % str(td[0].text).strip(' ')
            [company_number, company_name] = re.split("[\s]+", company_number_and_name)
            if not re.match("^[\d][\d]{2}[\d]$", company_number):
                continue
            element_list.append(company_number)
            element_list.append(company_name)
            for i in range(1, 6): 
                element_list.append(td[i].text)

        return data_list


    def do_debug(self):
        # import pdb; pdb.set_trace()
        res = requests.get("http://isin.twse.com.tw/isin/C_public.jsp?strMode=%d" % self.str_mode)
        res.encoding = 'big5'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        # print g_data
        for tr in g_data[2:]:
            td = tr.select('td')
            if len(td) != 7:
                continue
            print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
# ==== result: ====
# 1 1-999 10,306 2,826,860 0.17
# 2 1,000-5,000 25,902 58,118,814 3.65
# 3 5,001-10,000 5,226 41,051,024 2.58
# 4 10,001-15,000 1,759 22,311,406 1.40
# 5 15,001-20,000 1,045 19,220,774 1.20
# 6 20,001-30,000 891 22,776,215 1.43
# 7 30,001-40,000 390 13,958,604 0.87
# 8 40,001-50,000 275 12,847,077 0.80
# 9 50,001-100,000 520 37,685,496 2.37
# 10 100,001-200,000 241 34,077,197 2.14
# 11 200,001-400,000 132 37,307,870 2.34
# 12 400,001-600,000 61 29,705,626 1.87
# 13 600,001-800,000 30 21,034,103 1.32
# 14 800,001-1,000,000 23 20,988,311 1.32
# 15 1,000,001以上 149 1,214,611,545 76.46
#  合　計 46,950 1,588,520,922 100.00
