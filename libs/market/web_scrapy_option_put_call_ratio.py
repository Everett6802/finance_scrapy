# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 臺指選擇權賣權買權比
class WebScrapyOptionPutCallRatio(WebScrapyMarketBase.WebScrapyMarketBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyOptionPutCallRatio, self).__init__(
    #         # "http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart={0}%2F{1}%2F{2}&dateend={3}%2F{4}%2F{5}", 
    #         __file__
    #         # CMN_CLS.ParseURLDataByBS4('utf-8', '.table_a tr'),
    #         # datetime_range_start, 
    #         # datetime_range_end,
    #         # enable_time_range_mode = True,
    #     )
    def __init__(self, **kwargs):
        super(WebScrapyOptionPutCallRatio, self).__init__(__file__, **kwargs)
        self.whole_month_data = True
        self.data_not_whole_month_list = []


    def _adjust_time_duration_from_lookup_table(self):
        super(WebScrapyOptionPutCallRatio, self)._adjust_time_duration_from_lookup_table()
        if CMN.CLS.FinanceDate.is_same_month(self.xcfg["time_duration_start"], self.xcfg["time_duration_end"]):
            if self.xcfg["time_duration_start"].day > 1 or self.xcfg["time_duration_end"].day < CMN.FUNC.get_month_last_day(self.xcfg["time_duration_end"].year, self.xcfg["time_duration_end"].month):
                self.data_not_whole_month_list.append(CMN.CLS.FinanceMonth(self.xcfg["time_duration_end"].year, self.xcfg["time_duration_end"].month))
        else:
            if self.xcfg["time_duration_start"].day > 1:
                self.data_not_whole_month_list.append(CMN.CLS.FinanceMonth(self.xcfg["time_duration_start"].year, self.xcfg["time_duration_start"].month))
            if self.xcfg["time_duration_end"].day < CMN.FUNC.get_month_last_day(self.xcfg["time_duration_end"].year, self.xcfg["time_duration_end"].month):
                self.data_not_whole_month_list.append(CMN.CLS.FinanceMonth(self.xcfg["time_duration_end"].year, self.xcfg["time_duration_end"].month))


    def assemble_web_url(self, timeslice):
        # if timeslice is None:
        #     datetime_start_cfg = self.get_datetime_startday()
        #     datetime_end_cfg= self.get_datetime_endday()
        #     url = self.url_format.format(*(datetime_start_cfg.year, datetime_start_cfg.month, datetime_start_cfg.day, datetime_end_cfg.year, datetime_end_cfg.month, datetime_end_cfg.day))
        # else:
        #     url = self.url_format.format(*(timeslice.year, timeslice.month, timeslice.day))
        # import pdb; pdb.set_trace()
# Check if it's no need to acquire the whole month data in this month
        try:
            index = self.data_not_whole_month_list.index(timeslice)
            if len(self.data_not_whole_month_list) == 1:
                url = self.url_format.format(*(timeslice.year, timeslice.month, self.xcfg["time_duration_start"].day, self.xcfg["time_duration_end"].day))
            else:
                if index == 0:
                    end_day_in_month = CMN.FUNC.get_month_last_day(timeslice.year, timeslice.month)
                    url = self.url_format.format(*(timeslice.year, timeslice.month, self.xcfg["time_duration_start"].day, end_day_in_month))
                else:
                    url = self.url_format.format(*(timeslice.year, timeslice.month, 1, self.xcfg["time_duration_start"].day))
            self.whole_month_data = False
        except ValueError:
            end_day_in_month = CMN.FUNC.get_month_last_day(timeslice.year, timeslice.month)
            url = self.url_format.format(*(timeslice.year, timeslice.month, 1, end_day_in_month))
            self.whole_month_data = True
        return url


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        web_data_len = len(web_data)
        if web_data_len == 0:
            return None
        data_list = []
        
        # print "len: %d" % data_len
        for tr in web_data[web_data_len - 1 : 0 : -1]:
            td = tr.select('td')
            entry = [str(td[0].text.replace("/", "-")),]
            for index in range(1, 7):
                entry.append(str(td[index].text).replace(',', ''))
            data_list.append(entry)
        return data_list
# "日期"
# "賣權成交量",
# "買權成交量",
# "買賣權成交量比率%",
# "賣權未平倉量",
# "買權未平倉量",
# "買賣權未平倉量比率%",


    def do_debug(self, silent_mode=False):
        # res = requests.get("http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart=2015%2F10%2F1&dateend=2015%2F10%2F31")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart=2015%2F10%2F1&dateend=2015%2F10%2F31")
        # print res.text
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('.table_a tr')
        data_len = len(g_data)
        # print "len: %d" % data_len
        for tr in g_data[data_len - 1 : 0 : -1]:
            td = tr.select('td')
            if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text, td[6].text
        if not silent_mode: print "\n"
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
