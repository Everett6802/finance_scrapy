# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import scrapy_market_base as ScrapyMarketBase
g_logger = CMN.LOG.get_logger()


# 期貨和選擇權未平倉口數與契約金額
class FutureAndOptionTop3LegalPersonsOpenInterest(ScrapyMarketBase.ScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, timeslice.day))
        return url


    def __init__(self, **kwargs):
        super(FutureAndOptionTop3LegalPersonsOpenInterest, self).__init__(**kwargs)
        self.cur_date_str = None


    def _scrape_web_data(self, timeslice):
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        data_list = [self.cur_date_str,]
        for tr in web_data[3:6]:
            td = tr.select('td')
            for i in range(6):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
        return data_list
# "日期",
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


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear=2015&smonth=10&sday=1&datestart=1979%2F09%2F04")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear=2015&smonth=10&sday=1&datestart=1979%2F09%2F04")
        res.encoding = 'utf-8'
        #print res.text
        soup = BeautifulSoup(res.text)
        #g_data = soup.find_all("table", {"class": "table_f"}).select('tr')
# 交易口數與契約金額
        g_data = soup.select('.table_f tr')
        #print "len: %d" % len(g_data)
        for tr in g_data[3:6]:
            th = tr.select('th')
            td = tr.select('td')
            if not silent_mode: print th[0].text, td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
# ==== result: ====
# 自營商 404,438 53,429 396,842 50,928 7,596 2,501
# 投信 1,150 1,298 566 721 584 577
# 外資 186,318 98,294 189,514 97,276 -3,196 1,018
        if not silent_mode: print "\n"
# 未平倉口數與契約金額
        # g_data = soup.find("table", {"class": "table_c"}).select('tr')
        g_data = soup.select('.table_c tr')
        #print "len: %d" % len(g_data)
        for tr in g_data[3:6]:
            th = tr.select('th')
            td = tr.select('td')
            if not silent_mode: print th[0].text, td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
# ==== result: ====
# 自營商 324,371 24,235 312,783 32,145 11,588 -7,910
# 投信 9,323 8,006 9,403 13,131 -80 -5,125
# 外資 230,957 69,097 215,187 29,563 15,770 39,534
