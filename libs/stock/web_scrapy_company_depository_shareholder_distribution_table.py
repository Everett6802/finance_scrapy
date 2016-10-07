# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_stock_base as WebScrapyStockBase
# import web_scrapy_timeslice_generator as TimeSliceGenerator
g_logger = CMN.WSL.get_web_scrapy_logger()


# 集保戶股權分散表
class WebScrapyDepositoryShareholderDistributionTable(WebScrapyStockBase.WebScrapyStockBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyDepositoryShareholderDistributionTable, self).__init__(
    #         "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE={0}{1}{2}&SqlMethod=StockNo&StockNo={3}&StockName=&sub=%ACd%B8%DF", 
    #         __file__, 
    #         # 'big5', 
    #         # 'table tbody tr', 
    #         CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    #         datetime_range_start, 
    #         datetime_range_end
    #     )
    #     self.generate_day_time_list_rule = self.__generate_day_time_list_rule_select_friday
    last_finance_date = None
    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyDepositoryShareholderDistributionTable, self).__init__(__file__, **kwargs)
        self.date_cur_string = None


    @classmethod
    def _get_time_last_start_and_end_time(cls, *args):
        # import pdb; pdb.set_trace()
        if cls.last_finance_date is None:
            timeslice_generator = BASE.TSG.WebScrapyTimeSliceGenerator.Instance()
            last_friday_date_str_for_financial_statement = timeslice_generator.get_last_friday_date_str_for_financial_statement()
            last_finance_date_str = "%s-%s-%s" % (last_friday_date_str_for_financial_statement[0:4], last_friday_date_str_for_financial_statement[4:6], last_friday_date_str_for_financial_statement[6:8])
            cls.last_finance_date = CMN.CLS.FinanceDate(last_finance_date_str) 
        return (cls.last_finance_date, cls.last_finance_date)


    def assemble_web_url(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        url = self.url_format.format(
            *(
                timeslice.year, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day,
                company_code_number
            )
        )
        self.date_cur_string = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        return url


    def _parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        # import pdb; pdb.set_trace()
        data_list = []
        data_list.append(self.date_cur_string)
        for tr in web_data[9:25]:
            td = tr.select('td')
            data_list.append(str(CMN.FUNC.remove_comma_in_string(td[2].text)))
            data_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(td[3].text)))
            data_list.append(td[4].text)

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


    # def __generate_day_time_list_rule_select_friday(self, datetime_cfg):
    #     day_of_week = datetime_cfg.weekday()
    #     return (True if day_of_week == 4 else False)


    def do_debug(self, silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE=20160408&SqlMethod=StockNo&StockNo=2347&StockName=&sub=%ACd%B8%DF")
        res = CMN.FUNC.request_from_url_and_check_return("https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE=20160408&SqlMethod=StockNo&StockNo=2347&StockName=&sub=%ACd%B8%DF")
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
