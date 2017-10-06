# -*- coding: utf8 -*-

import re
import requests
# import csv
# from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 臺股指數及成交量
class WebScrapyStockExchangeAndVolume(WebScrapyMarketBase.WebScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month))
        return url


    @classmethod
    def _transform_data_for_writing_to_csv(cls, csv_data_list):
        return cls._transform_multi_data_in_one_page(csv_data_list)


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyStockExchangeAndVolume, self).__init__(**kwargs)
        self.whole_month_data = True
        self.time_duration_start_after_adjustment = self.xcfg["time_duration_start"]
        self.time_duration_end_after_adjustment = self.xcfg["time_duration_end"]
        self.data_not_whole_month_list = None


#     def _adjust_time_range_from_web(self, *args):
#         # import pdb; pdb.set_trace()
#         time_duration_after_lookup_time = super(WebScrapyStockExchangeAndVolume, self)._adjust_time_range_from_web(*args)
# # Find the month which data does NOT contain the whole month
#         self.data_not_whole_month_list = CMN.FUNC.get_data_not_whole_month_list(time_duration_after_lookup_time.time_duration_start, time_duration_after_lookup_time.time_duration_end)


    def _adjust_config_before_scrapy(self, *args):
        # import pdb; pdb.set_trace()
# args[0]: time duration start
# args[1]: time duration end
        web2csv_time_duration_update = args[0]
        self.time_duration_start_after_adjustment = web2csv_time_duration_update.NewWebStart
        self.time_duration_end_after_adjustment = web2csv_time_duration_update.NewWebEnd
        self.data_not_whole_month_list = CMN.FUNC.get_data_not_whole_month_list(self.time_duration_start_after_adjustment, self.time_duration_end_after_adjustment)


    def _scrape_web_data(self, timeslice):
        # import pdb; pdb.set_trace()
        assert isinstance(timeslice, CMN.CLS.FinanceMonth), "The input time duration time unit is %s, not FinanceMonth" % type(timeslice)
# Check if it's no need to acquire the whole month data in this month
        try:
            index = self.data_not_whole_month_list.index(timeslice)
            self.whole_month_data = False
        except ValueError:
            self.whole_month_data = True
        url = self.assemble_web_url(timeslice)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = []
        # print "len: %d" % data_len
        # for tr in web_data[2:]:
        #     td = tr.select('td')
        #     date_list = td[0].text.split('/')
        #     if len(date_list) != 3:
        #         raise RuntimeError("The date format is NOT as expected: %s", date_list)
        #     entry = [CMN.FUNC.transform_date_str(int(date_list[0]) + CMN.DEF.REPUBLIC_ERA_YEAR_OFFSET, int(date_list[1]), int(date_list[2])),]
        for data_entry in web_data:
            date_list = str(data_entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            entry = [CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2])),]
            if not self.whole_month_data:
                date_cur = CMN.CLS.FinanceDate.from_string(entry[0])
                if date_cur < self.time_duration_start_after_adjustment:
                    continue
                elif date_cur > self.time_duration_end_after_adjustment:
                    break
            for index in range(1, 6):
                entry.append(str(data_entry[index]).replace(',', ''))
            data_list.append(entry)
        return data_list
# "日期",
# "成交股數",
# "成交金額",
# "成交筆數",
# "發行量加權股價指數",
# "漲跌點數",


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.twse.com.tw/ch/trading/exchange/FMTQIK/genpage/Report201511/201511_F3_1_2.php?STK_NO=&myear=2015&mmon=11")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/exchange/FMTQIK/genpage/Report201511/201511_F3_1_2.php?STK_NO=&myear=2015&mmon=11")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/exchange/FMTQIK/FMTQIK.php?download=&query_year=2017&query_month=3")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/indices/MI_5MINS_HIST/MI_5MINS_HIST.php?myear=104&mmon=10")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date=20160301")
        # print res.text
        res.encoding = 'utf-8'
        # soup = BeautifulSoup(res.text)
        # # print soup
        # # g_data = soup.select('.board_trad tr')
        # g_data = soup.findAll('table')[7].findAll('tr')
        # print g_data
        # for tr in g_data[2:]:
        #     td = tr.select('td')
        #     date_list = td[0].text.split('/')
            #     raise RuntimeError("The date format is NOT as expected: %s", date_list)
            # date_str = CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))
            # if not silent_mode: print date_str, td[1].text, td[2].text, td[3].text , td[4].text#, td[5].text
        g_data = json.loads(res.text)['data']
        for entry in g_data:
            date_list = str(entry[0]).split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            date_str = CMN.FUNC.transform_date_str(int(date_list[0]), int(date_list[1]), int(date_list[2]))
            if not silent_mode: print date_str, entry[1], entry[2], entry[3], entry[4], entry[5]
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
