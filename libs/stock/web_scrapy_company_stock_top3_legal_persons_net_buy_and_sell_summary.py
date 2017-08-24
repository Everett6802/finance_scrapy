# -*- coding: utf8 -*-

import re
import requests
import json
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 上市三大法人個股買賣超日報
class WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary(WebScrapyStockBase.WebScrapyStockBase):

    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = self.URL_FORMAT.format(
            *(
                timeslice.year, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, **kwargs):
        super(WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary, self).__init__(**kwargs)


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
        # res = requests.get("http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date=105%2F03%2F23&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date=105%2F03%2F23&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/fund/T86?response=json&date=20170804&selectType=ALL")
        #print res.text
        res.encoding = 'utf-8'
        g_data = json.loads(res.text)['data']
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
# 0050 台灣50           9,093,000 480,000 0 0 3,000 572,000 950,000 545,000 8,449,000
# 0051 中100            0 0 0 0 0 0 4,000 6,000 -2,000
# 0053 寶電子           0 0 0 0 0 0 9,000 0 9,000
# 0054 台商50           0 0 0 0 0 0 0 1,000 -1,000
# ......
