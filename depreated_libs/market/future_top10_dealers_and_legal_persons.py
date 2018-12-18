# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import scrapy_market_base as ScrapyMarketBase
g_logger = CMN.LOG.get_logger()


# 期貨大額交易人未沖銷部位結構表 : 臺股期貨
class FutureTop10DealersAndLegalPersons(ScrapyMarketBase.ScrapyMarketBase):

    OLD_FORMAT_ROW_START = 3
    OLD_FORMAT_ROW_END = 5
    NEW_FORMAT_ROW_START = 4
    NEW_FORMAT_ROW_END = 6
    DATE_OLD_FORMAT = CMN.CLS.FinanceDate(2013, 7, 26)


    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, timeslice.day))
        return url


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(FutureTop10DealersAndLegalPersons, self).__init__(**kwargs)
        self.need_check_everytime = False
        self.data_row_start_index = FutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_START
        self.data_row_end_index = FutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_END
        self.cur_date = None
        self.start_index_list = [1, 1]
        # datetime_real_start = super(FutureTop10DealersAndLegalPersons, self).get_real_datetime_start()
        # datetime_real_end = super(FutureTop10DealersAndLegalPersons, self).get_real_datetime_end()
        self.cur_date_str = None


    # def _adjust_time_range_from_web(self, *args):
    #     import pdb; pdb.set_trace()
    #     time_duration_after_lookup_time = super(FutureTop10DealersAndLegalPersons, self)._adjust_time_range_from_web(*args)
    #     assert isinstance(self.xcfg["time_duration_start"], CMN.CLS.FinanceDate), "The input start time duration time unit is %s, not FinanceDate" % type(self.xcfg["time_duration_start"])
    #     assert isinstance(self.xcfg["time_duration_end"], CMN.CLS.FinanceDate), "The input end time duration time unit is %s, not FinanceDate" % type(self.xcfg["time_duration_end"])
    #     if self.xcfg["time_duration_start"] <= FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT and self.xcfg["time_duration_end"] > FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
    #         self.need_check_everytime = True
    #         self.data_row_start_index = None
    #         self.data_row_end_index = None
    #         self.start_index_list = None
    #     elif self.xcfg["time_duration_end"] <= FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
    #         self.data_row_start_index = FutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_START
    #         self.data_row_end_index = FutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_END  
    #         self.start_index_list = [2, 1] 
    #     return time_duration_after_lookup_time


    def _adjust_config_before_scrapy(self, *args):
        # import pdb; pdb.set_trace()
# args[0]: time duration start
# args[1]: time duration end
        web2csv_time_duration_update = args[0]
        time_duration_start_after_adjustment = web2csv_time_duration_update.NewWebStart
        time_duration_end_after_adjustment = web2csv_time_duration_update.NewWebEnd
        if time_duration_start_after_adjustment <= FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT and time_duration_end_after_adjustment > FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
            self.need_check_everytime = True
            self.data_row_start_index = None
            self.data_row_end_index = None
            self.start_index_list = None
        elif time_duration_end_after_adjustment <= FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
            self.data_row_start_index = FutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_START
            self.data_row_end_index = FutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_END  
            self.start_index_list = [2, 1] 


    def _scrape_web_data(self, timeslice):
        # import pdb; pdb.set_trace()
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        if self.need_check_everytime:
            self.cur_date = timeslice
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = [self.cur_date_str,]
        if self.need_check_everytime:
            if self.cur_date <= FutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
                self.data_row_start_index = FutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_START
                self.data_row_end_index = FutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_END   
                self.start_index_list = [2, 1]
            else:
                self.data_row_start_index = FutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_START
                self.data_row_end_index = FutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_END
                self.start_index_list = [1, 1]

        column_num = 9
        row_index = 0
        # import pdb; pdb.set_trace()
        for tr in web_data[self.data_row_start_index:self.data_row_end_index]:
            start_index = self.start_index_list[row_index]
            td = tr.select('td')
            for i in range(start_index, start_index + column_num):
                element = str(re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)).strip(' ').replace(',', '')
                data_list.append(element)
            row_index += 1
        return data_list
# "日期",
# "臺股期貨_到期月份_買方_前五大交易人合計_部位數",
# "臺股期貨_到期月份_買方_前五大交易人合計_百分比",
# "臺股期貨_到期月份_買方_前十大交易人合計_部位數",
# "臺股期貨_到期月份_買方_前十大交易人合計_百分比",
# "臺股期貨_到期月份_賣方_前五大交易人合計_部位數",
# "臺股期貨_到期月份_賣方_前五大交易人合計_百分比",
# "臺股期貨_到期月份_賣方_前十大交易人合計_部位數",
# "臺股期貨_到期月份_賣方_前十大交易人合計_百分比",
# "臺股期貨_到期月份_全市場未沖銷部位數",
# "臺股期貨_所有契約_買方_前五大交易人合計_部位數",
# "臺股期貨_所有契約_買方_前五大交易人合計_百分比",
# "臺股期貨_所有契約_買方_前十大交易人合計_部位數",
# "臺股期貨_所有契約_買方_前十大交易人合計_百分比",
# "臺股期貨_所有契約_賣方_前五大交易人合計_部位數",
# "臺股期貨_所有契約_賣方_前五大交易人合計_百分比",
# "臺股期貨_所有契約_賣方_前五大交易人合計_部位數",
# "臺股期貨_所有契約_賣方_前十大交易人合計_百分比",
# "臺股期貨_所有契約_全市場未沖銷部位數",


    @staticmethod
    def do_debug(silent_mode=False):
        # res = requests.get("http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=ALL&goday=&choose_yy=2013&choose_mm=6&choose_dd=17&datestart=1979%2F9%2F4&choose_item=TX+++++")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=ALL&goday=&choose_yy=2013&choose_mm=6&choose_dd=17&datestart=1979%2F9%2F4&choose_item=TX+++++")
        res.encoding = 'utf-8'
        #print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_f tr')
        # import pdb; pdb.set_trace()
        for tr in g_data[4:6]:
            td = tr.select('td')
            for i in range(10):
                if not silent_mode: print re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)
# ==== result: ====
# 201509 (臺股期貨_到期月份)
# 20,642 (臺股期貨_到期月份_買方_前五大交易人合計_部位數)  
# 36 (臺股期貨_到期月份_買方_前五大交易人合計_百分比)                                       
# 27,681 (臺股期貨_到期月份_買方_前十大交易人合計_部位數)                                      
# 48.3 (臺股期貨_到期月份_買方_前十大交易人合計_百分比)                                       
# 16,918 (臺股期貨_到期月份_賣方_前五大交易人合計_部位數)                                       
# 29.5 (臺股期貨_到期月份_賣方_前五大交易人合計_百分比)                                       
# 25,055 (臺股期貨_到期月份_賣方_前十大交易人合計_部位數)                                      
# 43.7 (臺股期貨_到期月份_賣方_前十大交易人合計_百分比)   
# 57,365 (臺股期貨_到期月份_全市場未沖銷部位數)                                    
# 所有契約 (臺股期貨_所有契約)
# 25,538 (臺股期貨_所有契約_買方_前五大交易人合計_部位數)  
# 32.5  (臺股期貨_所有契約_買方_前五大交易人合計_百分比)                                      
# 37,821 (臺股期貨_所有契約_買方_前十大交易人合計_部位數)                                      
# 48.1 (臺股期貨_所有契約_買方_前十大交易人合計_百分比)                                       
# 18,363 (臺股期貨_所有契約_賣方_前五大交易人合計_部位數)                                       
# 23.4 (臺股期貨_所有契約_賣方_前五大交易人合計_百分比)                                       
# 28,093 (臺股期貨_所有契約_賣方_前十大交易人合計_部位數)                                      
# 35.7 (臺股期貨_所有契約_賣方_前十大交易人合計_百分比)                                       
# 78,637 (臺股期貨_所有契約_全市場未沖銷部位數)  
