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


# 臺指選擇權賣權買權比
class WebScrapyOptionPullCallRatio(web_scrapy_base.WebScrapyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        super(WebScrapyOptionTop3LegalPersonsBuyAndSellOptionOpenInterest, self).__init__(
            "http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart={0}%2F{1}%2F{2}&dateend={0}%2F{1}%2F{3}", 
            __file__, 
            'utf-8', 
            '.table_a tr', 
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
        # start_index_list = [4, 1, 1, 2, 1, 1]
        # column_num = 12
        start_index_list = [10, 7, 7, 8, 7, 7]
        column_num = 6
        row_index = 0
        for tr in web_data[3:9]:
            start_index = start_index_list[row_index]
            td = tr.select('td')
            for i in range(start_index, start_index + column_num):
                element = str(td[i].text).replace(',', '')
                data_list.append(element)
            row_index += 1
        return data_list
# "買權_自營商_買方_口數",
# "買權_自營商_買方_契約金額",
# "買權_自營商_賣方_口數",
# "買權_自營商_賣方_契約金額",
# "買權_自營商_買賣差額_口數",
# "買權_自營商_買賣差額_契約金額",
# "買權_投信_買方_口數",
# "買權_投信_買方_契約金額",
# "買權_投信_賣方_口數",
# "買權_投信_賣方_契約金額",
# "買權_投信_買賣差額_口數",
# "買權_投信_買賣差額_契約金額",
# "買權_外資_買方_口數",
# "買權_外資_買方_契約金額",
# "買權_外資_賣方_口數",
# "買權_外資_賣方_契約金額",
# "買權_外資_買賣差額_口數",
# "買權_外資_買賣差額_契約金額",
# "賣權_自營商_買方_口數",
# "賣權_自營商_買方_契約金額",
# "賣權_自營商_賣方_口數",
# "賣權_自營商_賣方_契約金額",
# "賣權_自營商_買賣差額_口數",
# "賣權_自營商_買賣差額_契約金額",
# "賣權_投信_買方_口數",
# "賣權_投信_買方_契約金額",
# "賣權_投信_賣方_口數",
# "賣權_投信_賣方_契約金額",
# "賣權_投信_買賣差額_口數",
# "賣權_投信_買賣差額_契約金額",
# "賣權_外資_買方_口數",
# "賣權_外資_買方_契約金額",
# "賣權_外資_賣方_口數",
# "賣權_外資_賣方_契約金額",
# "賣權_外資_買賣差額_口數",
# "賣權_外資_買賣差額_契約金額",


    def do_debug(self):
        res = requests.get("http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart=2015%2F10%2F1&dateend=2015%2F10%2F31")
        # print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_a tr')
        data_len = len(g_data)
        # print "len: %d" % data_len
# 交易口數與契約金額
        for tr in g_data[data_len - 1 : 0 : -1]:
            td = tr.select('td')
            print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text, td[6].text
        print "\n"
# 2015/10/1 469,154 363,160 129.19 528,800 444,977 118.84
# 2015/10/2 227,935 188,407 120.98 566,471 460,938 122.90
# 2015/10/5 313,428 294,016 106.60 604,928 497,533 121.59
# 2015/10/6 428,186 398,654 107.41 651,043 529,468 122.96
# 2015/10/7 500,065 490,839 101.88 524,338 427,387 122.68
# 2015/10/8 350,069 359,173 97.47 583,602 479,646 121.67
# 2015/10/12 425,550 346,892 122.68 657,646 502,986 130.75
# 2015/10/13 304,045 250,105 121.57 690,070 527,494 130.82
# 2015/10/14 426,506 372,268 114.57 574,323 438,918 130.85
# 2015/10/15 401,647 352,800 113.85 646,034 458,179 141.00
# 2015/10/16 237,588 223,340 106.38 668,791 481,344 138.94
# 2015/10/19 278,442 260,546 106.87 707,554 512,545 138.05
# 2015/10/20 245,587 206,721 118.80 739,575 517,067 143.03
# 2015/10/21 470,134 447,010 105.17 252,130 249,172 101.19
# 2015/10/22 181,449 173,657 104.49 313,715 296,261 105.89
# 2015/10/23 300,366 297,664 100.91 367,083 342,264 107.25
# 2015/10/26 211,699 202,286 104.65 416,828 377,964 110.28
# 2015/10/27 281,119 286,301 98.19 459,396 433,331 106.02
# 2015/10/28 493,177 456,812 107.96 342,651 341,580 100.31
# 2015/10/29 330,574 362,899 91.09 401,071 431,222 93.01
# 2015/10/30 381,443 388,214 98.26 435,379 470,926 92.45
