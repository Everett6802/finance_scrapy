# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


NEW_FORAMT_START_DATE_STR = "2014-12-01"
NEW_FORAMT_START_DATE_CFG = CMN.transform_string2datetime(NEW_FORAMT_START_DATE_STR)
# NEW_FORMAT_ENTRY_END_INDEX = 9
# NEW_FORMAT_ENTRY_END_INDEX = 11

# 三大法人上市個股買賣超日報
class WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary(web_scrapy_base.WebScrapyStockBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary, self).__init__(
    #         # "http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date={0}%2F{1}%2F{2}&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+", 
    #         __file__
    #         # CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    #         # datetime_range_start, 
    #         # datetime_range_end
    #     )
    #     self.new_format_table = False
    #     # self.entry_index_index = OLD_FORMAT_ENTRY_END_INDEX
    def __init__(self, **kwargs):
        super(WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary, self).__init__(__file__, **kwargs)


    def assemble_web_url(self, timeslice):
        url = self.url_format.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        if not self.new_format_table:
            if datetime_cfg >= NEW_FORAMT_START_DATE_CFG:
                self.new_format = True

        return url


    def parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        data_list = []
        if not self.new_format_table:
            for tr in web_data:
                element_list = []
                td = tr.select('td')
                # for i in range(1, 3):
                company_number = str(td[0].text).strip(' ')
# Filter the data which I am NOT Interested in
                # if len(company_number) != 4:
                if not re.match("^[\d][\d]{2}[\d]$", company_number):
                    continue
                element_list.append(company_number)
                for i in range(2, 9):
                    value = CMN.transform_share_number_string_to_board_lot(td[i].text)
                    element_list.append(str(value))
                data_list.append(element_list)
        else:
            for tr in web_data:
                element_list = []
                td = tr.select('td')
                # for i in range(1, 3):
                company_number = str(td[0].text).strip(' ')
# Filter the data which I am NOT Interested in
                # if len(company_number) != 4:
                if not re.match("^[\d][\d]{2}[\d]$", company_number):
                    continue
                element_list.append(company_number)
                for i in range(2, 7):
                    value = CMN.transform_share_number_string_to_board_lot(td[i].text)
                    element_list.append(str(value))
                for i in range(7, 9):
                    value1 = CMN.transform_share_number_string_to_board_lot(td[i].text)
                    value2 = CMN.transform_share_number_string_to_board_lot(td[i + 2].text)
                    element_list.append(str(value1 + value2))
                data_list.append(element_list)
        return data_list
# 證券代號
# 外資買進股數
# 外資賣出股數
# 投信買進股數
# 投信賣出股數
# 自營商買進股數(自行買賣) + 自營商買進股數(避險)
# 自營商賣出股數(自行買賣) + 自營商賣出股數(避險)
# 三大法人買賣超股數


    def do_debug(self):
        res = requests.get("http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date=105%2F03%2F23&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+")
        #print res.text
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tbody tr')
        # data_len = len(g_data)
        # print "len: %d" % data_len
        for tr in g_data:
        #     print tr.text
            td = tr.select('td')
            print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text, td[6].text, td[7].text, td[8].text, td[9].text, td[10].text
# 證券代號
# 證券名稱
# 外資買進股數
# 外資賣出股數
# 投信買進股數
# 投信賣出股數
# 自營商買進股數(自行買賣)
# 自營商賣出股數(自行買賣)
# 自營商買進股數(避險)
# 自營商賣出股數(避險)
# 三大法人買賣超股數

# ==== result: ====
# 0050 台灣50           9,093,000 480,000 0 0 3,000 572,000 950,000 545,000 8,449,000
# 0051 中100            0 0 0 0 0 0 4,000 6,000 -2,000
# 0053 寶電子           0 0 0 0 0 0 9,000 0 9,000
# 0054 台商50           0 0 0 0 0 0 0 1,000 -1,000
# ......
