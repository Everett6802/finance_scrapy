# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 融資融券餘額統計表
class WebScrapyStockMarginTradingAndShortSelling(WebScrapyMarketBase.WebScrapyMarketBase):

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
        super(WebScrapyStockMarginTradingAndShortSelling, self).__init__(__file__, **kwargs)
        self.cur_date_str = None


    def prepare_for_scrapy(self, timeslice):
        # import pdb; pdb.set_trace()
        url = self.assemble_web_url(timeslice)
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        return url


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = [self.cur_date_str,]
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


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate=104%2F11%2F19&selectType=MS")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate=104%2F11%2F19&selectType=MS")
        # print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('tr')
        # print g_data
        for tr in g_data[2:]:
            td = tr.select('td')
            if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text , td[4].text, td[5].text

# ==== result: ====
# 融資(交易單位) 306,709 265,329 4,091 9,062,601 9,099,890
# 融券(交易單位) 26,636 41,757 904 555,470 569,687
# 融資金額(仟元) 6,553,208 6,071,954 119,416 145,001,426 145,363,264

