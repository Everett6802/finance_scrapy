# -*- coding: utf8 -*-

import re
import requests
# import csv
# from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import libs.common as CMN
import scrapy_market_base as ScrapyMarketBase
g_logger = CMN.LOG.get_logger()


# 臺股指數及成交量
class StockExchangeAndVolume(ScrapyMarketBase.ScrapyMarketBase):

    @classmethod
    def assemble_web_url(cls, timeslice, *args):
        url = cls.URL_FORMAT.format(*(timeslice.year, timeslice.month))
        return url


    @classmethod
    def _transform_data_for_writing_to_csv(cls, csv_data_list):
        return cls._transform_multi_data_in_one_page(csv_data_list)


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(StockExchangeAndVolume, self).__init__(**kwargs)
        self.whole_month_data = True
        self.time_duration_start_after_adjustment = self.xcfg["time_duration_start"]
        self.time_duration_end_after_adjustment = self.xcfg["time_duration_end"]
        self.data_not_whole_month_list = None


#     def _adjust_time_range_from_web(self, *args):
#         # import pdb; pdb.set_trace()
#         time_duration_after_lookup_time = super(StockExchangeAndVolume, self)._adjust_time_range_from_web(*args)
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
            entry = [CMN.FUNC.transform_date_str(int(date_list[0]) + 1911, int(date_list[1]), int(date_list[2])),]
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
# 105-03-01 4,344,172,230 93,076,019,508 914,162 8,485.69 74.53
# 105-03-02 5,383,933,146 101,503,867,237 1,022,407 8,544.05 58.36
# 105-03-03 5,585,659,656 101,457,411,076 1,058,220 8,611.79 67.74
# 105-03-04 4,963,184,767 95,177,431,173 1,002,782 8,643.55 31.76
# 105-03-07 4,985,069,930 97,317,441,753 1,044,138 8,659.55 16.00
# 105-03-08 5,752,490,488 107,338,379,721 1,118,305 8,664.31 4.76
# 105-03-09 4,873,667,413 87,722,074,364 924,681 8,634.11 -30.20
# 105-03-10 4,847,234,890 86,979,509,559 912,645 8,660.70 26.59
# 105-03-11 4,696,453,516 89,618,730,648 932,516 8,706.14 45.44
# 105-03-14 4,958,111,936 100,745,050,581 1,001,609 8,747.90 41.76
# 105-03-15 5,749,788,495 116,596,382,075 1,152,022 8,611.18 -136.72
# 105-03-16 4,940,468,348 95,303,426,426 931,278 8,699.14 87.96
# 105-03-17 5,640,225,790 110,428,671,229 1,069,422 8,734.54 35.40
# 105-03-18 5,058,056,953 118,470,664,453 980,140 8,810.71 76.17
# 105-03-21 4,189,171,268 83,602,377,454 828,164 8,812.70 1.99
# 105-03-22 4,537,521,519 87,909,107,741 896,141 8,785.68 -27.02
# 105-03-23 4,146,764,067 83,001,864,735 841,520 8,766.09 -19.59
# 105-03-24 4,332,890,552 81,028,032,418 828,433 8,743.38 -22.71
# 105-03-25 3,480,063,136 66,971,897,265 668,084 8,704.97 -38.41
# 105-03-28 3,632,554,168 69,405,830,918 707,968 8,690.45 -14.52
# 105-03-29 4,336,733,399 80,354,222,855 863,621 8,617.35 -73.10
# 105-03-30 4,449,258,360 89,188,136,871 850,273 8,737.04 119.69
# 105-03-31 4,483,580,039 96,143,315,403 841,211 8,744.83 7.79

