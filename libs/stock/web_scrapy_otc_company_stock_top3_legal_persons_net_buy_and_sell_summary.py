# -*- coding: utf8 -*-

import re
import requests
import csv
import json
# from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


# 三大法人上櫃個股買賣超日報
class WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary(web_scrapy_base.WebScrapyStockBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary, self).__init__(
    #         # "http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d={0}/{1}/{2}&_=1460104675945", 
    #         __file__
    #         # CMN_CLS.ParseURLDataByJSON('aaData'),
    #         # datetime_range_start, 
    #         # datetime_range_end
    #     )
    def __init__(self, **kwargs):
        super(WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary, self).__init__(__file__, **kwargs)


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
        # import pdb; pdb.set_trace()
        if len(web_data) == 0:
            return None

        data_list = []
        for data in web_data:
            element_list = []
            company_number = "%s" % str(data[0]).strip(' ')
            if not re.match("^[\d][\d]{2}[\d]$", company_number):
                continue
            element_list.append(company_number)
# 外資
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[2])))
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[3])))
# 投信
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[5])))
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[6])))
# 自營商
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[9]) + CMN.transform_share_number_string_to_board_lot(data[12])))
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[10]) + CMN.transform_share_number_string_to_board_lot(data[13])))
# 三大法人
            element_list.append(str(CMN.transform_share_number_string_to_board_lot(data[15])))

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
        res = requests.get("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=105/04/01&_=1460104675945")
        json_res = json.loads(res.text)
        g_data = json_res['aaData']
        for data in g_data:
            entry = "%s" % str(data[0]).strip(' ')
            for i in range(2, 16):
                entry += ",%s" % str(data[i]).strip(' ')
            print entry
# 代號
# 名稱
# 外資及陸資買股數
# 外資及陸資賣股數
# 外資及陸資淨買股數
# 投信買股數
# 投信賣股數
# 投信淨買股數
# 自營商淨買股數
# 自營商(自行買賣)買股數
# 自營商(自行買賣)賣股數
# 自營商(自行買賣)淨買股數
# 自營商(避險)買股數
# 自營商(避險)賣股數
# 自營商(避險)淨買股數
# 三大法人買賣超股數

# ==== result: ====
# 006201,0,0,0,0,0,0,32,000,0,0,0,32,000,0,32,000,32,000
# 1258,5,000,0,5,000,0,0,0,0,0,0,0,0,0,0,5,000
# 1264,2,000,0,2,000,5,000,0,5,000,-10,000,0,10,000,-10,000,0,0,0,-3,000
# 1333,0,1,000,-1,000,0,0,0,0,0,0,0,0,0,0,-1,000
# ......
