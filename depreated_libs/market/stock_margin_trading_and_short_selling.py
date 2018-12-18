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


# 融資融券餘額統計表
class StockMarginTradingAndShortSelling(ScrapyMarketBase.ScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        # url = cls.URL_FORMAT.format(
        #     *(
        #         timeslice.year - 1911, 
        #         "%02d" % timeslice.month,
        #         "%02d" % timeslice.day
        #     )
        # )
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, timeslice.day))
        return url


    def __init__(self, **kwargs):
        super(StockMarginTradingAndShortSelling, self).__init__(**kwargs)
        self.cur_date_str = None


    def _scrape_web_data(self, timeslice):
        # import pdb; pdb.set_trace()
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = [self.cur_date_str,]
        # for tr in web_data[2:]:
        #     td = tr.select('td')
        #     for i in range(1, 6):
        #         element = str(td[i].text).replace(',', '')
        #         data_list.append(element)
        # return data_list
        for entry in web_data:
            for i in range(1, 6):
                element = str(entry[i]).replace(',', '')
                data_list.append(element)
        return data_list
# 日期
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


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate=104%2F11%2F19&selectType=MS")
        # res = CMN.FUNC.request_from_url_and_check_return("http://app.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate=104%2F11%2F19&selectType=MS")
        # # print res.text
        # res.encoding = 'utf-8'
        # soup = BeautifulSoup(res.text)
        # g_data = soup.select('tr')
        # # print g_data
        # for tr in g_data[2:]:
        #     td = tr.select('td')
        #     if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text , td[4].text, td[5].text
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/exchangeReport/MI_MARGN?response=json&date=20180302&selectType=MS")
        res.encoding = 'utf-8'
        # import pdb; pdb.set_trace()
        g_data = json.loads(res.text)['creditList']
        if not silent_mode: 
            print g_data[0][1], g_data[0][2], g_data[0][3], g_data[0][4], g_data[0][5],g_data[1][1], g_data[1][2], g_data[1][3], g_data[1][4], g_data[1][5],g_data[2][1], g_data[2][2], g_data[2][3], g_data[2][4], g_data[2][5]


# ==== result: ====
# 項目  買進  賣出  現金(券)償還 前日餘額    今日餘額
# 融資(交易單位)    235,530 214,022 6,122   8,892,037   8,907,423
# 融券(交易單位)    33,968  28,856  12,497  562,538 544,929
# 融資金額(仟元)    5,004,467   4,422,992   163,095 165,713,658 166,132,038
