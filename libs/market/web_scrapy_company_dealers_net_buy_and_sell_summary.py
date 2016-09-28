# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 自營商買賣超彙總表
class WebScrapyCompanyDealersNetBuyOrSellSummary(WebScrapyMarketBase.WebScrapyMarketBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyCompanyDealersNetBuyOrSellSummary, self).__init__(
    #         "http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php?download=&qdate={0}%2F{1}%2F{2}&sorting=by_stkno", 
    #         __file__, 
    #         'utf-8', 
    #         'table tr', 
    #         datetime_range_start, 
    #         datetime_range_end
    #     )
    def __init__(self, **kwargs):
        super(WebScrapyCompanyDealersNetBuyOrSellSummary, self).__init__(__file__, **kwargs)


    def assemble_web_url(self, timeslice):
        url = self.url_format.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def _parse_web_data(self, web_data):
        import pdb; pdb.set_trace()
        if len(web_data) == 0:
            return None
        data_list = []
        for tr in web_data[2:]:
            td = tr.select('td')
            # for i in range(1, 3):
            data_list.append(str(td[0].text).strip(' '))
            for i in range(8, 11):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
        return data_list
# 證券代號
# 證券名稱
# 自營商買進股數
# 自營商賣出股數
# 自營商買賣超股數


    def do_debug(self):
        res = requests.get("http://www.twse.com.tw/ch/trading/fund/TWT43U/TWT43U.php?download=&qdate=105%2F03%2F25&sorting=by_stkno")
        #print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        #print g_data
        # data_len = len(g_data)
        # print "len: %d" % data_len
        for tr in g_data[3:]:
            td = tr.select('td')
        #     print td
            print td[0].text, td[1].text, td[8].text, td[9].text, td[10].text
        print "\n"
# ==== result: ====
# 0050   台灣50           2,002,000 415,000 1,587,000
# 0051   中100            0 15,000 -15,000
# 0052   FB科技           0 1,000 -1,000
# 0053   寶電子           23,000 0 23,000
# ......
