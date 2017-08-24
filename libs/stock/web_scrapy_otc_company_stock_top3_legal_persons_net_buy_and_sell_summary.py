# -*- coding: utf8 -*-

import re
import requests
import json
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 上櫃三大法人個股買賣超日報
class WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary(WebScrapyStockBase.WebScrapyStockBase):

    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = self.URL_FORMAT.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, **kwargs):
        super(WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary, self).__init__(**kwargs)


    def _scrape_web_data(self, timeslice, company_code_number):
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = []
        for data_entry in web_data:
            element_list = []
            company_number = "%s" % str(data_entry[0]).strip(' ')
            if not re.match("^[\d][\d]{2}[\d]$", company_number):
                continue
# Company code number
            element_list.append(company_number)
# 外資
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[2])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[3])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[4])))
# 投信
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[5])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[6])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[7])))
# 自營商
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[9]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[12])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[10]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[13])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[11]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[14])))
# 三大法人
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[15])))
# Add the entry
            data_list.append(element_list)
        return data_list
# 證券代號
# 外資及陸資買進股數
# 外資及陸資賣出股數
# 外資及陸資淨買股數
# 投信買進股數
# 投信賣出股數
# 投信淨買股數
# 自營商買進股數(自行買賣) + 自營商買進股數(避險)
# 自營商賣出股數(自行買賣) + 自營商賣出股數(避險)
# 自營商(自行買賣)淨買股數 + 自營商(避險)淨買股數
# 三大法人買賣超股數


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=105/04/01&_=1460104675945")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=105/04/01&_=1460104675945")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=106/08/15")
        json_res = json.loads(res.text)
        g_data = json_res['aaData']
        if not silent_mode: 
            for entry in g_data:
                print entry[0], entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7], entry[8], entry[9], entry[10], entry[11], entry[12], entry[13], entry[14], entry[15]
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
