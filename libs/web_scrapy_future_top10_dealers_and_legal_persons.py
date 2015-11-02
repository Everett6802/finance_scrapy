# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


# 期貨大額交易人未沖銷部位結構表 : 臺股期貨
class WebSracpyFutureTop10DealersAndLegalPersons(web_scrapy_base.WebSracpyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        data_source_index = 2
        url_format = "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++"
        csv_filename_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%s.csv"

        super(WebSracpyFutureTop10DealersAndLegalPersons, self).__init__(
            url_format, 
            csv_filename_format, 
            'utf-8', 
            '.table_f tr', 
            data_source_index,
            datetime_range_start, 
            datetime_range_end
        )
        

    def assemble_web_url(self, datetime_cfg):
        url = self.url_format.format(*(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
        return url


    def parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        data_list = []
        # import pdb; pdb.set_trace()
        for tr in web_data[4:6]:
            td = tr.select('td')
            for i in range(1, 10):
                element = str(re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)).strip(' ').replace(',', '')
                data_list.append(element)
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


    def debug_only(self):
        res = requests.get("http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=2015&mmtemp=9&ddtemp=10&chooseitemtemp=ALL&goday=&choose_yy=2015&choose_mm=9&choose_dd=10&datestart=2015%2F9%2F10&choose_item=TX+++++")
        res.encoding = 'utf-8'
        #print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_f tr')
        for tr in g_data[4:6]:
            td = tr.select('td')
            for i in range(10):
                print re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)
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
