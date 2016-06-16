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


# 融資融券餘額統計表
class WebScrapyStockMarginTradingAndShortSelling(web_scrapy_base.WebScrapyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        super(WebScrapyStockMarginTradingAndShortSelling, self).__init__(
            # "http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate={0}%2F{1}%2F{2}&selectType=MS", 
            __file__
            # CMN_CLS.ParseURLDataByBS4('utf-8', 'tr'),
            # datetime_range_start, 
            # datetime_range_end
        )


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
        for tr in web_data[2:]:
            td = tr.select('td')
            for i in range(1, 6):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
        return data_list
# 融資(交易單位)_買進
# 融資(交易單位)_賣出
# 融資(交易單位)_現金(券)償還
# 融資(交易單位)_前日餘額
# 融資(交易單位)_今日餘額
# 融券(交易單位)_買進
# 融券(交易單位)_賣出
# 融券(交易單位)_現金(券)償還
# 融券(交易單位)_前日餘額
# 融券(交易單位)_今日餘額
# 融資金額(仟元)_買進
# 融資金額(仟元)_賣出
# 融資金額(仟元)_現金(券)償還
# 融資金額(仟元)_前日餘額
# 融資金額(仟元)_今日餘額


    def do_debug(self):
        res = requests.get("http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate=104%2F11%2F19&selectType=MS")
        # print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('tr')
        # print g_data
        for tr in g_data[2:]:
            td = tr.select('td')
            print td[0].text, td[1].text, td[2].text, td[3].text , td[4].text, td[5].text

# ==== result: ====
# 融資(交易單位) 306,709 265,329 4,091 9,062,601 9,099,890
# 融券(交易單位) 26,636 41,757 904 555,470 569,687
# 融資金額(仟元) 6,553,208 6,071,954 119,416 145,001,426 145,363,264

