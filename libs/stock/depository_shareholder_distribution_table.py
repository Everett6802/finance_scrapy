# -*- coding: utf8 -*-

import re
import requests
import csv
import threading
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import scrapy_stock_base as ScrapyStockBase
g_logger = CMN.LOG.get_logger()


# 集保戶股權分散表
class DepositoryShareholderDistributionTable(ScrapyStockBase.ScrapyStockBase):

    TABLE_SUM_FLAG = u'\u5408\u3000\u8a08' # "合　計"
    # TABLE_SUM_FLAG = u"合　計".encode(CMN.DEF.URL_ENCODING_BIG5)
    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
# CAUTION: This function MUST be called by the LEAF derived class
        url = cls.URL_FORMAT.format(
            *(
                timeslice.year, 
                timeslice.month,
                timeslice.day,
                company_code_number
            )
        )
        return url


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(DepositoryShareholderDistributionTable, self).__init__(**kwargs)
        self.date_cur_string = None
        self.last_finance_date = None
        self.__update_last_finance_date()


    def __update_last_finance_date(self):
        timeslice_generator = BASE.TSG.TimeSliceGenerator.Instance()
        last_friday_date_str_for_financial_statement = timeslice_generator.get_last_friday_date_str_for_financial_statement()
        last_finance_date_str = "%s-%s-%s" % (last_friday_date_str_for_financial_statement[0:4], last_friday_date_str_for_financial_statement[4:6], last_friday_date_str_for_financial_statement[6:8])
        self.last_finance_date = CMN.CLS.FinanceDate(last_finance_date_str) 


    def _scrape_web_data(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        self.date_cur_string = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = []
        data_list.append(self.date_cur_string)
        sum_found = False
        index_list = range(9, 24)
        index_last = index_list[-1]
# Scrape the data of each stock interval
        for index in index_list:
            tr = web_data[index]
            td = tr.select('td')
            data_list.append(str(CMN.FUNC.remove_comma_in_string(td[2].text)))
            data_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(td[3].text)))
            data_list.append(str(td[4].text))
            if index == index_last:
                if re.match(self.TABLE_SUM_FLAG, td[1].text, re.U):
                    sum_found = True
# Ignore the data which is NOT interesting... Scrape the data of sum if necessary
        # import pdb; pdb.set_trace()
        if not sum_found:
            for tr in web_data[24:]:
                td = tr.select('td')
                if not re.match(self.TABLE_SUM_FLAG, td[1].text, re.U):
                    continue
                data_list.append(str(CMN.FUNC.remove_comma_in_string(td[2].text)))
                data_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(td[3].text)))
                data_list.append(str(td[4].text))
                sum_found = True
                break
        if not sum_found:
            raise ValueError("Fail to find the sum flag in the table");
        return data_list
# 持股1-999人數
# 持股1-999股數
# 持股1-999佔集保庫存數比例
# 持股1,000-5,000人數
# 持股1,000-5,000股數
# 持股1,000-5,000佔集保庫存數比例
# 持股5,001-10,000人數
# 持股5,001-10,000股數
# 持股5,001-10,000佔集保庫存數比例
# 持股10,001-15,000人數
# 持股10,001-15,000股數
# 持股10,001-15,000佔集保庫存數比例
# 持股15,001-20,000人數
# 持股15,001-20,000股數
# 持股15,001-20,000佔集保庫存數比例
# 持股20,001-30,000人數
# 持股20,001-30,000股數
# 持股20,001-30,000佔集保庫存數比例
# 持股30,001-40,000人數
# 持股30,001-40,000股數
# 持股30,001-40,000佔集保庫存數比例
# 持股40,001-50,000人數
# 持股40,001-50,000股數
# 持股40,001-50,000佔集保庫存數比例
# 持股50,001-100,000人數
# 持股50,001-100,000股數
# 持股150,001-100,000佔集保庫存數比例
# 持股100,001-200,000人數
# 持股100,001-200,000股數
# 持股100,001-200,000佔集保庫存數比例
# 持股200,001-400,000人數
# 持股200,001-400,000股數
# 持股200,001-400,000佔集保庫存數比例
# 持股400,001-600,000人數
# 持股400,001-600,000股數
# 持股400,001-600,000佔集保庫存數比例
# 持股600,001-800,000人數
# 持股600,001-800,000股數
# 持股600,001-800,000佔集保庫存數比例
# 持股800,001-1,000,000人數
# 持股800,001-1,000,000股數
# 持股800,001-1,000,000佔集保庫存數比例
# 持股1,000,001以上人數
# 持股1,000,001以上股數
# 持股1,000,001以上佔集保庫存數比例
# 合計人數
# 合計股數
# 合計佔集保庫存數比例


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE=20160408&SqlMethod=StockNo&StockNo=2347&StockName=&sub=%ACd%B8%DF")
        res = CMN.FUNC.request_from_url_and_check_return("https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE=20170908&SqlMethod=StockNo&StockNo=2347&StockName=&sub=%ACd%B8%DF")
        res.encoding = 'big5'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tbody tr')
        # print g_data
        # print g_data[7].text
        for tr in g_data[9:25]:
        #     print tr.text
            td = tr.select('td')
            if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text
# 序 持股／單位數分級 人數 股數 佔集保庫存比例
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
