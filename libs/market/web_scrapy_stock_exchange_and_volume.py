# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_market_base as WebScrapyMarketBase
# import web_scrapy_market_url_date_range as WebScrapyMarketURLTimeRange
# from libs import web_scrapy_logging as WSL
g_logger = CMN.WSL.get_web_scrapy_logger()


# 臺股指數及成交量
class WebScrapyStockExchangeAndVolume(WebScrapyMarketBase.WebScrapyMarketBase):

    # def __init__(self, datetime_range_start=None, datetime_range_end=None):
    #     super(WebScrapyStockExchangeAndVolume, self).__init__(
    #         # "http://www.twse.com.tw/ch/trading/exchange/FMTQIK/genpage/Report{0}{1:02d}/{0}{1:02d}_F3_1_2.php?STK_NO=&myear={0}&mmon={1:02d}",
    #         __file__
    #         # CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    #         # datetime_range_start, 
    #         # datetime_range_end,
    #         # enable_time_range_mode = True,
    #     )
    #     # import pdb; pdb.set_trace()
    #     datetime_start_cfg = self.get_datetime_startday()
    #     datetime_end_cfg = self.get_datetime_endday()
    #     # assert (datetime_start_cfg.year == datetime_end_cfg.year), "Start Year[%d] is NOT equal to End Year[%d]" % (datetime_start_cfg.year, datetime_end_cfg.year)
    #     # assert (datetime_start_cfg.month == datetime_end_cfg.month), "Start Month[%d] is NOT equal to End Month[%d]" % (datetime_start_cfg.month, datetime_end_cfg.month)
    #     self.whole_month_data = True
    #     if  datetime_start_cfg.day > 1 or datetime_end_cfg.day < CMN.get_cfg_month_last_day(datetime_end_cfg):
    #         self.whole_month_data = False
    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyStockExchangeAndVolume, self).__init__(__file__, **kwargs)
        self.whole_month_data = True
        self.data_not_whole_month_list = []
        if self.xcfg["time_start"].day > 1:
            self.data_not_whole_month_list.append(CMN.CLS.FinanceMonth(self.xcfg["time_start"].year, self.xcfg["time_start"].month))
        if not CMN.CLS.FinanceDate.is_same_month(self.xcfg["time_start"], self.xcfg["time_end"]):
            if self.xcfg["time_end"].day < CMN.FUNC.get_month_last_day(self.xcfg["time_end"].year, self.xcfg["time_end"].month):
                self.data_not_whole_month_list.append(CMN.CLS.FinanceMonth(self.xcfg["time_end"].year, self.xcfg["time_end"].month))


    def assemble_web_url(self, timeslice):
        # assert (timeslice is None), "timeslice is NOT None"
        # import pdb; pdb.set_trace()
        # datetime_month_cfg = self.get_datetime_startday()
        # url = self.url_format.format(*(datetime_month_cfg.year, datetime_month_cfg.month))
        url = self.url_format.format(*(timeslice.year, timeslice.month))
# Check if it's no need to acquire the whole month data in this month
        try:
            index = self.data_not_whole_month_list.index(timeslice)
            self.whole_month_data = False
        except ValueError:
            self.whole_month_data = True
        return url


    def parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        if len(web_data) == 0:
            return None
        data_list = []

        # print "len: %d" % data_len
        for tr in web_data[2:]:
            td = tr.select('td')
            date_list = td[0].text.split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            # entry = [CMN.transform_datetime2string(date_list[0], date_list[1], date_list[2], True),]
            entry = [CMN.FUNC.transform_date_str(int(date_list[0]) + CMN.DEF.DEF_REPUBLIC_ERA_YEAR_OFFSET, int(date_list[1]), int(date_list[2])),]

            if not self.whole_month_data:
                # datetime_cur = CMN.transform_string2datetime(entry[0])
                # if datetime_cur < self.get_datetime_startday() or datetime_cur > self.get_datetime_endday():
                date_cur = CMN.CLS.FinanceDate.from_string(entry[0])
                if date_cur < self.xcfg["time_start"] or date_cur > self.xcfg["time_end"]:
                    continue
            for index in range(1, 6):
                entry.append(str(td[index].text).replace(',', ''))
            data_list.append(entry)
        return data_list
# "日期",
# "成交股數",
# "成交金額",
# "成交筆數",
# "發行量加權股價指數",
# "漲跌點數",


    def do_debug(self):
        res = requests.get("http://www.twse.com.tw/ch/trading/exchange/FMTQIK/genpage/Report201511/201511_F3_1_2.php?STK_NO=&myear=2015&mmon=11")
        # print res.text
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        # print soup
        g_data = soup.select('.board_trad tr')
        # print g_data
        for tr in g_data[2:]:
            td = tr.select('td')
            date_list = td[0].text.split('/')
            if len(date_list) != 3:
                raise RuntimeError("The date format is NOT as expected: %s", date_list)
            date_str = CMN.transform_datetime2string(date_list[0], date_list[1], date_list[2], True)
            print date_str, td[1].text, td[2].text, td[3].text , td[4].text, td[5].text

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
