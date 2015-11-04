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


# 選擇權買賣權未平倉口數與契約金額
class WebSracpyOptionTop3LegalPersonsBuyAndSellOptionOpenInterest(web_scrapy_base.WebSracpyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        data_source_index = 1
        url_format = "http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F09%2F04"
        csv_filename_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%s.csv"
        
        super(WebSracpyFutureTop3LegalPersonsOpenInterest, self).__init__(
            url_format, 
            csv_filename_format, 
            'utf-8', 
            '.table_c tr', 
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
        for tr in web_data[3:6]:
            td = tr.select('td')
            for i in range(6):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
        return data_list
# "自營商_多方_口數",
# "自營商_多方_契約金額",
# "自營商_空方_口數",
# "自營商_空方_契約金額",
# "自營商_多空淨額_口數",
# "自營商_多空淨額_契約金額",
# "投信_多方_口數",
# "投信_多方_契約金額",
# "投信_空方_口數",
# "投信_空方_契約金額",
# "投信_多空淨額_口數",
# "投信_多空淨額_契約金額",
# "外資_多方_口數",
# "外資_多方_契約金額",
# "外資_空方_口數",
# "外資_空方_契約金額",
# "外資_多空淨額_口數",
# "外資_多空淨額_契約金額",


    def debug_only(self):
        res = requests.get("http://www.taifex.com.tw/chinese/3/7_12_5.asp?goday=&DATA_DATE_Y=2015&DATA_DATE_M=11&DATA_DATE_D=3&syear=2015&smonth=11&sday=3&datestart=2015%2F11%2F3&COMMODITY_ID=TXO")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_f tr')
        # print g_data
        for tr in g_data[3:9]:
        th = tr.select('th')
        td = tr.select('td')
        print td[4].text, td[5].text, td[6].text, td[7].text, td[8].text, td[9].text, td[10].text, td[11].text
