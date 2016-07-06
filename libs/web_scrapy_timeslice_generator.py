# -*- coding: utf8 -*-

import os
import sys
import re
import requests
from bs4 import BeautifulSoup
# from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
from libs import web_scrapy_url_date_range as URLTimeRange
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


@CMN_CLS.Singleton
class WebScrapyTimeSliceGenerator(object):

    def __init__(self):
        # self.FINANCIAL_STATEMENT_DATE_LIST = [[3, 31], [5, 15], [8, 14], [11, 14],]
        # self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST = [[-1, 3], [-1, 4], [0, 1], [0, 2], [0, 3]]
        self.REVENUE_DAY = 10
        self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_HEAD_FORMAT = "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SqlMethod=StockNo&StockNo=%s"
        self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_TAIL = "&StockName=&sub=%ACd%B8%DF"

        self.workday_canlendar = None
        self.generate_time_slice_func_ptr = [
            self.__generate_time_slice_by_workday,
            self.__generate_time_slice_by_company_foreign_investors_shareholder,
            self.__generate_time_slice_by_month,
            self.__generate_time_slice_by_revenue,
            self.__generate_time_slice_by_financial_statement_season,
        ]
        # self.company_foreign_investors_shareholder_date_list = None
        # self.financial_statement_last_season = None
        self.date_today = None
        self.month_today = None
        self.url_date_range = None


    def initialize(self):
        pass
        # import pdb; pdb.set_trace()


    # def __get_financial_statement_season(self, date_cur=None):
    #     date_now = self.date_today if date_cur is None else date_cur
    #     cur_year = date_now.year
    #     financial_statement_date_list = []
        
    #     financial_statement_season = None
    #     financial_statement_date_list.append(CMN_CLS.FinanceDate(cur_year, self.FINANCIAL_STATEMENT_DATE_LIST[0][0], self.FINANCIAL_STATEMENT_DATE_LIST[0][1]))
    #     if date_now < financial_statement_date_list[0]:
    #         financial_statement_season = {"year": cur_year + self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[0][0], "season": self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[0][1]}
    #     else:
    #         season_index = 1
    #         while season_index < 4:
    #             financial_statement_date_list.append(datetime(cur_year, self.FINANCIAL_STATEMENT_DATE_LIST[season_index][0], self.FINANCIAL_STATEMENT_DATE_LIST[season_index][1]))
    #             if datetime_now >= financial_statement_date_list[season_index - 1] and datetime_now < financial_statement_date_list[season_index]:
    #                 financial_statement_season_cfg = {"year": cur_year + self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[season_index][0], "season": self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[season_index][1]}
    #                 break
    #             season_index += 1
    #         if financial_statement_season_cfg is None:
    #             if datetime_now >= financial_statement_date_list[-1]:
    #                 financial_statement_season_cfg = {"year": cur_year + self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[4][0], "season": self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[4][1]}
    #     if financial_statement_season_cfg is None:
    #         raise ValueError("Fail to find last seaon of financial statement from the date: %s" % datetime_now)
    #     g_logger.debug("The financial statement season of the date[%s]: year: %d, season: %d" % (CMN.to_date_only_str(datetime_now), financial_statement_season_cfg['year'], financial_statement_season_cfg['season']))
    #     return financial_statement_season_cfg


    def __get_company_foreign_investors_shareholder_date_list(self, company_code_number):
        # import pdb; pdb.set_trace()
        url = self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_HEAD_FORMAT % company_code_number + self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_TAIL
        res = requests.get(url)
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tbody tr option')
        company_foreign_investors_shareholder_date_list = []
        for index in range(len(g_data) - 1, -1, -1):
            time_str = g_data[index].text
            year = int(time_str[0:4])
            month = int(time_str[4:6])
            day = int(time_str[6:8])
            company_foreign_investors_shareholder_date_list.append(CMN_CLS.FinanceDate(year, month, day))
        return company_foreign_investors_shareholder_date_list


    def __generate_time_slice_by_workday(self, date_start, date_end, **kwargs):
# The data type in the list is datetime
        if self.workday_canlendar is None:
            self.workday_canlendar = WorkdayCanlendar.WebScrapyWorkdayCanlendar.Instance()
# Check time range
        if date_start < self.workday_canlendar.get_first_workday():
            g_logger.warn("The start workday [%s] is earlier than the first day[%s]" % (date_start, self.workday_canlendar.get_first_workday()))
            date_start = self.workday_canlendar.get_first_workday()
        if date_end > self.workday_canlendar.get_last_workday():
            g_logger.warn("The end workday [%s] is later than the last day[%s]" % (date_end, self.workday_canlendar.get_last_workday()))
            date_end = self.workday_canlendar.get_last_workday()
# Return the iterable
        return WorkdayCanlendar.WorkdayNearestIterator(date_start, date_end)


    def __generate_time_slice_by_company_foreign_investors_shareholder(self, date_start, date_end, **kwargs):
# The data type in the list is datetime
        # if time_slice_cfg is None:
        #     raise ValueError("The config should NOT be NULL") 
        company_code_number = kwargs.pop("company_code_number", None)
        if company_code_number is None:
            raise ValueError("Fail to find the 'company_code_number' in the config") 
        company_foreign_investors_shareholder_date_list = self.__get_company_foreign_investors_shareholder_date_list(company_code_number)
        if company_foreign_investors_shareholder_date_list is None:
            raise ValueError("Fail to find the company[%s] foreign investors sharehold date list" % company_code_number)
# Check time range
        if date_end < company_foreign_investors_shareholder_date_list[0]:
            raise ValueError("The end day [%s] is earlier than the first one[%s]" % (date_end, company_foreign_investors_shareholder_date_list[0]))
        if date_start > company_foreign_investors_shareholder_date_list[-1]:
            raise ValueError("The start day [%s] is later than the last one[%s]" % (date_start, company_foreign_investors_shareholder_date_list[-1]))

        if date_start < company_foreign_investors_shareholder_date_list[0]:
            g_logger.warn("The start day [%s] is earlier than the first one[%s]" % (date_start, company_foreign_investors_shareholder_date_list[0]))
            date_start = company_foreign_investors_shareholder_date_list[0]
        if date_end > company_foreign_investors_shareholder_date_list[-1]:
            g_logger.warn("The end day [%s] is later than the last one[%s]" % (date_end, company_foreign_investors_shareholder_date_list[-1]))
            date_end = company_foreign_investors_shareholder_date_list[-1]
# Define the iterator
        class TimeSliceIterator(object):
            def __init__(self, date_start, date_end, date_list):
                self.time_to_stop = False
                # import pdb; pdb.set_trace()
                self.start_index = next((index for index, date_cur in enumerate(date_list) if date_cur >= date_start), -1)
                if self.start_index == -1:
                    raise ValueError("Fail to find the start index of the start date: %s" % date_start)
                self.end_index = next((index for index, date_cur in reversed(list(enumerate(date_list))) if date_cur <= date_end), -1)
                if self.end_index == -1:
                    raise ValueError("Fail to find the end index of the end date: %s" % date_end)
                self.cur_index = self.start_index
                self.date_list = date_list

            def __iter__(self):
                return self

            def next(self):
                # import pdb; pdb.set_trace()
                if self.time_to_stop:
                    raise StopIteration
                cur_index = self.cur_index
                if self.cur_index == self.end_index:
                    self.time_to_stop = True
                if not self.time_to_stop:
                    self.cur_index += 1
                return self.date_list[cur_index]
        return TimeSliceIterator(date_start, date_end, company_foreign_investors_shareholder_date_list)


    def __generate_time_slice_by_revenue(self, month_start, month_end, **kwargs):
# Check time range
        if month_end == self.month_today:
            if datetime_end.day < self.REVENUE_DAY:
                g_logger.warn("The revenue of this month is NOT released on the date [%s] " % self.date_today)
                month_end = month_end - 1

        return self.__generate_time_slice_by_month(month_start, month_end, **kwargs)


    def __generate_time_slice_by_month(self, month_start, month_end, **kwargs):
# The data type in the list is datetime
# Define the iterator
        class TimeSliceIterator(object):
            def __init__(self, month_start, month_end):
                self.time_to_stop = False
                # import pdb; pdb.set_trace()
                self.month_cur = month_start
                self.month_end = month_end

            def __iter__(self):
                return self

            def next(self):
                # import pdb; pdb.set_trace()
                if self.time_to_stop:
                    raise StopIteration
                month_cur = self.month_cur
                if self.month_cur == self.month_end:
                    self.time_to_stop = True
                if not self.time_to_stop:
                    self.month_cur = month_cur + 1
                return month_cur
# Check time range
        return TimeSliceIterator(month_start, month_end)


    def __generate_time_slice_by_financial_statement_season(self, quarter_start, quarter_end, **kwargs):
# Define the iterator
        class TimeSliceIterator(object):
            def __init__(self, quarter_start, quarter_end):
                self.time_to_stop = False
                self.quarter_cur = quarter_start
                self.quarter_end = quarter_end

            def __iter__(self):
                return self

            def next(self):
                # import pdb; pdb.set_trace()
                if self.time_to_stop:
                    raise StopIteration
                quarter_cur = self.quarter_cur
                if self.quarter_cur == self.quarter_end:
                    self.time_to_stop = True
                if not self.time_to_stop:
                    self.quarter_cur = quarter_cur + 1
                return quarter_cur
# Return the iterable
        return TimeSliceIterator(quarter_start, quarter_end)


    def __restrict_date_range(self, date_start, date_end, data_source_id):
# Caution: For Market mode, the data_source_id is date source type.
# Caution: For Stock Mode, the data_source_id is company code number.
        if date_start > date_end:
            raise RuntimeError("The Start Time[%s] should NOT be greater than the End Time[%s]" % (date_start, date_end))
        if self.url_date_range is None:
            self.url_date_range = URLTimeRange.WebScrapyMarketURLTimeRange.Instance() if CMN.IS_FINANCE_MARKET_MODE else URLTimeRange.WebScrapyStockURLTimeRange.Instance()
        if date_start is None or date_start < self.url_date_range.get_date_range_start(data_source_id):
            date_start = self.url_date_range.get_date_range_start(data_source_id)
        if date_end is None or date_end > self.url_date_range.get_date_range_end(data_source_id):
            date_end = self.url_date_range.get_date_range_end(data_source_id)
        g_logger.debug("The URL[ID: %d] restricted date range: %s %s" % (data_source_id, date_start, date_end))
        return (date_start, date_end)


    def generate_time_slice(self, time_slice_type, **kwargs):
        if self.date_today is None:
           self.date_today = CMN_CLS.FinanceDate(datetime.today())
        if self.month_today is None:
           self.month_today = CMN_CLS.FinanceMonth(datetime.today())
# Check input argument
        data_source_index = kwargs.get("data_source_index", None)
        if data_source_index is None:
            raise TypeError("The data_source_index field is NOT found in kwargs")
        date_start = kwargs.get("date_start", None)
        date_end = kwargs.get("date_end", None)
        data_source_id = kwargs.get("data_source_id", None)
        company_code_number = kwargs.get("company_code_number", None)
        if CMN.IS_FINANCE_STOCK_MODE and company_code_number is None:
            raise TypeError("The company_code_number field is NOT found in kwargs")
        # if kwargs:
        #     raise TypeError("Unexpected **kwargs: %s" % kwargs)
# Restrict the max time range
        (restricted_date_start, restricted_date_end) = self.__restrict_date_range(
            date_start, 
            date_end, 
            data_source_index if CMN.IS_FINANCE_MARKET_MODE else company_code_number
        )
        time_slice_kwargs = {}
        if CMN.IS_FINANCE_STOCK_MODE:
            time_slice_kwargs["company_code_number"] = company_code_number
        return (self.generate_time_slice_func_ptr[time_slice_type])(restricted_date_start, restricted_date_end, **time_slice_kwargs)
