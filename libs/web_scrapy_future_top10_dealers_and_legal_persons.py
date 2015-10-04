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
        data_source_index = 0
        url_format = "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++"
        csv_filename_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%s.csv"

        super(WebSracpyFutureTop10DealersAndLegalPersons, self).__init__(
            url_format, 
            csv_filename_format, 
            'utf-8', 
            '.table_f tr', 
            CMN.DEF_DATA_SOURCE_INDEX_MAPPING[data_source_index],
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
            for i in range(1, 9):
                element = str(re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)).strip(' ')
                data_list.append(element)
        return data_list


    def debug_only(self):
        res = requests.get("http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=2015&mmtemp=9&ddtemp=10&chooseitemtemp=ALL&goday=&choose_yy=2015&choose_mm=9&choose_dd=10&datestart=2015%2F9%2F10&choose_item=TX+++++")
        res.encoding = 'utf-8'
        #print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_f tr')
        for tr in g_data[4:6]:
            td = tr.select('td')
            for i in range(9):
                print re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)
# ==== result: ====
# 201509
# 20,642  
# 36                                       
# 27,681                                      
# 48.3                                       
# 16,918                                       
# 29.5                                       
# 25,055                                      
# 43.7                                       
# 所有契約
# 25,538  
# 32.5                                       
# 37,821                                      
# 48.1                                       
# 18,363                                       
# 23.4                                       
# 28,093                                      
# 35.7                                       

