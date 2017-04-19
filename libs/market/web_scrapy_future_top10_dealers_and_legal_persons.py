# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 期貨大額交易人未沖銷部位結構表 : 臺股期貨
class WebScrapyFutureTop10DealersAndLegalPersons(WebScrapyMarketBase.WebScrapyMarketBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     self.OLD_FORMAT_ROW_START = 3
    #     self.OLD_FORMAT_ROW_END = 5
    #     self.NEW_FORMAT_ROW_START = 4
    #     self.NEW_FORMAT_ROW_END = 6
    #     self.DATE_OLD_FORMAT = datetime(2013, 7, 26)

    #     super(WebScrapyFutureTop10DealersAndLegalPersons, self).__init__(
    #         # "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++", 
    #         __file__
    #         # CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),
    #         # datetime_range_start, 
    #         # datetime_range_end
    #     )

    #     self.need_check_everytime = False
    #     self.data_row_start_index = self.NEW_FORMAT_ROW_START
    #     self.data_row_end_index = self.NEW_FORMAT_ROW_END
    #     self.cur_date = None
    #     self.start_index_list = [1, 1]
    #     datetime_real_start = super(WebScrapyFutureTop10DealersAndLegalPersons, self).get_real_datetime_start()
    #     datetime_real_end = super(WebScrapyFutureTop10DealersAndLegalPersons, self).get_real_datetime_end()
    #     if datetime_real_start <= self.DATE_OLD_FORMAT and datetime_real_end > self.DATE_OLD_FORMAT:
    #         self.need_check_everytime = True
    #         self.data_row_start_index = None
    #         self.data_row_end_index = None
    #         self.start_index_list = None
    #     elif datetime_real_end <= self.DATE_OLD_FORMAT:
    #         self.data_row_start_index = self.OLD_FORMAT_ROW_START
    #         self.data_row_end_index = self.OLD_FORMAT_ROW_END  
    #         self.start_index_list = [2, 1] 
    OLD_FORMAT_ROW_START = 3
    OLD_FORMAT_ROW_END = 5
    NEW_FORMAT_ROW_START = 4
    NEW_FORMAT_ROW_END = 6
    DATE_OLD_FORMAT = CMN.CLS.FinanceDate(2013, 7, 26)
    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyFutureTop10DealersAndLegalPersons, self).__init__(__file__, **kwargs)
        self.need_check_everytime = False
        self.data_row_start_index = WebScrapyFutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_START
        self.data_row_end_index = WebScrapyFutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_END
        self.cur_date = None
        self.start_index_list = [1, 1]
        # datetime_real_start = super(WebScrapyFutureTop10DealersAndLegalPersons, self).get_real_datetime_start()
        # datetime_real_end = super(WebScrapyFutureTop10DealersAndLegalPersons, self).get_real_datetime_end()
        self.cur_date_str = None


    def _adjust_time_duration_from_lookup_table(self):
        super(WebScrapyFutureTop10DealersAndLegalPersons, self)._adjust_time_duration_from_lookup_table()
        if self.xcfg["time_duration_start"] <= WebScrapyFutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT and self.xcfg["time_duration_end"] > WebScrapyFutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
            self.need_check_everytime = True
            self.data_row_start_index = None
            self.data_row_end_index = None
            self.start_index_list = None
        elif self.xcfg["time_duration_end"] <= WebScrapyFutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
            self.data_row_start_index = WebScrapyFutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_START
            self.data_row_end_index = WebScrapyFutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_END  
            self.start_index_list = [2, 1] 


    def assemble_web_url(self, timeslice):
        # import pdb; pdb.set_trace()
        url = self.url_format.format(*(timeslice.year, timeslice.month, timeslice.day))
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        if self.need_check_everytime:
            self.cur_date = timeslice
        return url


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        if len(web_data) == 0:
            return None
        data_list = [self.cur_date_str,]
        if self.need_check_everytime:
            if self.cur_date <= WebScrapyFutureTop10DealersAndLegalPersons.DATE_OLD_FORMAT:
                self.data_row_start_index = WebScrapyFutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_START
                self.data_row_end_index = WebScrapyFutureTop10DealersAndLegalPersons.OLD_FORMAT_ROW_END   
                self.start_index_list = [2, 1]
            else:
                self.data_row_start_index = WebScrapyFutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_START
                self.data_row_end_index = WebScrapyFutureTop10DealersAndLegalPersons.NEW_FORMAT_ROW_END
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
