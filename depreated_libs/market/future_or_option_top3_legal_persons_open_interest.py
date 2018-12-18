# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import scrapy_market_base as ScrapyMarketBase
g_logger = CMN.LOG.get_logger()


# 期貨或選擇權未平倉口數與契約金額
class FutureOrOptionTop3LegalPersonsOpenInterest(ScrapyMarketBase.ScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month, timeslice.day))
        return url


    def __init__(self, **kwargs):
        super(FutureOrOptionTop3LegalPersonsOpenInterest, self).__init__(**kwargs)
        self.cur_date_str = None


    def _scrape_web_data(self, timeslice):
        self.cur_date_str = CMN.FUNC.transform_date_str(timeslice.year, timeslice.month, timeslice.day)
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        data_list = [self.cur_date_str,]
        for tr in web_data[12:15]:
            td = tr.select('td')
            for i in range(12):
                element = str(td[i].text).replace(',', '').strip('\r\n ')
                data_list.append(element)
        return data_list
# "日期",
# "自營商_多方_口數_期貨",
# "自營商_多方_口數_選擇權",
# "自營商_多方_契約金額_期貨",
# "自營商_多方_契約金額_選擇權",
# "自營商_空方_口數_期貨",
# "自營商_空方_口數_選擇權",
# "自營商_空方_契約金額_期貨",
# "自營商_空方_契約金額_選擇權",
# "自營商_多空淨額_口數_期貨",
# "自營商_多空淨額_口數_選擇權",
# "自營商_多空淨額_契約金額_期貨",
# "自營商_多空淨額_契約金額_選擇權",
# "投信_多方_口數_期貨",
# "投信_多方_口數_選擇權",
# "投信_多方_契約金額_期貨",
# "投信_多方_契約金額_選擇權",
# "投信_空方_口數_期貨",
# "投信_空方_口數_選擇權",
# "投信_空方_契約金額_期貨",
# "投信_空方_契約金額_選擇權",
# "投信_多空淨額_口數_期貨",
# "投信_多空淨額_口數_選擇權",
# "投信_多空淨額_契約金額_期貨",
# "投信_多空淨額_契約金額_選擇權",
# "外資_多方_口數_期貨",
# "外資_多方_口數_選擇權",
# "外資_多方_契約金額_期貨",
# "外資_多方_契約金額_選擇權",
# "外資_空方_口數_期貨",
# "外資_空方_口數_選擇權",
# "外資_空方_契約金額_期貨",
# "外資_空方_契約金額_選擇權",
# "外資_多空淨額_口數_期貨",
# "外資_多空淨額_口數_選擇權",
# "外資_多空淨額_契約金額_期貨",
# "外資_多空淨額_契約金額_選擇權",


    @staticmethod
    def do_debug(silent_mode=False):
        # res = requests.get("http://www.taifex.com.tw/chinese/3/7_12_2.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear=2015&smonth=11&sday=9&datestart=1979%2F09%2F04")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.taifex.com.tw/chinese/3/7_12_2.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear=2015&smonth=11&sday=9&datestart=1979%2F09%2F04")
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_f tr')
        # print "len: %d" % len(g_data)
# 交易口數與契約金額        
        for tr in g_data[4:7]:
            td = tr.select('td')
            for i in range(12):
                if not silent_mode: print str(td[i].text).replace(',', '').strip('\r\n ')
            # print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text, td[6].text, td[7].text, td[8].text, td[9].text, td[10].text, td[11].text
# ==== result: ====
# 66615
# 554493
# 56656223
# 1052092
# 66438
# 570616
# 56042023
# 1102547
# 177
# -16123
# 614200
# -50455
# 516
# 36
# 572065
# 117
# 753
# 24
# 1125574
# 62
# -237
# 12
# -553509
# 55
# 94137
# 117747
# 104342301
# 374417
# 93568
# 115544
# 103225236
# 348968
# 569
# 2203
# 1117065
# 25449
        if not silent_mode: print "\n"
# 未平倉餘額
        for tr in g_data[12:15]:
            td = tr.select('td')
            for i in range(12):
                if not silent_mode: print str(td[i].text).replace(',', '').strip('\r\n ')
# ==== result: ====
# 58455
# 266547
# 25984551
# 963371
# 103974
# 298940
# 31375074
# 813614
# -45519
# -32393
# -5390523
# 149757
# 6708
# 382
# 5240396
# 2441
# 11663
# 362
# 18449355
# 1230
# -4955
# 20
# -13208959
# 1211
# 49323
# 202452
# 68759729
# 788712
# 31844
# 202466
# 41260715
# 970382
# 17479
# -14
# 27499014
# -181670
