# -*- coding: utf8 -*-

import os
import sys
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


@CMN_CLS.Singleton
class WebScrapyTimeSliceGenerator(object):

    def __init__(self):
        self.FINANCIAL_STATEMENT_DATE_LIST = [[3, 31], [5, 15], [8, 14], [11, 14],]
        self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST = [[-1, 3], [-1, 4], [0, 1], [0, 2], [0, 3]]
        self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_HEAD_FORMAT = "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SqlMethod=StockNo&StockNo=%s"
        self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_TAIL = "&StockName=&sub=%ACd%B8%DF"

        self.workday_canlendar = None
        self.generate_time_slice_func_ptr = [
            self.__generate_time_slice_by_workday,
            self.__generate_time_slice_by_company_foreign_investors_shareholder,
            self.__generate_time_slice_by_month,
            self.__generate_time_slice_by_financial_statement_season,
        ]
        # self.company_foreign_investors_shareholder_timeslice_list = None
        self.financial_statement_last_season_cfg = None
        self.datetime_today = None


    def initialize(self):
        pass
        # import pdb; pdb.set_trace()


    def __get_financial_statement_season(self, datetime_cur=None):
        datetime_now = datetime.today() if datetime_cur is None else datetime_cur
        cur_year = datetime_now.year
        financial_statement_date_list = []
        
        financial_statement_season_cfg = None
        financial_statement_date_list.append(datetime(cur_year, self.FINANCIAL_STATEMENT_DATE_LIST[0][0], self.FINANCIAL_STATEMENT_DATE_LIST[0][1]))
        if datetime_now < financial_statement_date_list[0]:
            financial_statement_season_cfg = {"year": cur_year + self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[0][0], "season": self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[0][1]}
        else:
            season_index = 1
            while season_index < 4:
                financial_statement_date_list.append(datetime(cur_year, self.FINANCIAL_STATEMENT_DATE_LIST[season_index][0], self.FINANCIAL_STATEMENT_DATE_LIST[season_index][1]))
                if datetime_now >= financial_statement_date_list[season_index - 1] and datetime_now < financial_statement_date_list[season_index]:
                    financial_statement_season_cfg = {"year": cur_year + self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[season_index][0], "season": self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[season_index][1]}
                    break
                season_index += 1
            if financial_statement_season_cfg is None:
                if datetime_now >= financial_statement_date_list[-1]:
                    financial_statement_season_cfg = {"year": cur_year + self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[4][0], "season": self.FINANCIAL_STATEMENT_SEASON_OFFSET_LIST[4][1]}
        if financial_statement_season_cfg is None:
            raise ValueError("Fail to find last seaon of financial statement from the date: %s" % datetime_now)
        g_logger.debug("The financial statement season of the date[%s]: year: %d, season: %d" % (CMN.to_date_only_str(datetime_now), financial_statement_season_cfg['year'], financial_statement_season_cfg['season']))
        return financial_statement_season_cfg


    def __get_company_foreign_investors_shareholder_time_list(self, company_code_number):
        # import pdb; pdb.set_trace()
        url = self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_HEAD_FORMAT % company_code_number + self.COMPANY_FOREIGN_INVESTORS_SHAREHOLDER_URL_PAIR_TAIL
        res = requests.get(url)
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tbody tr option')
        company_foreign_investors_shareholder_timeslice_list = []
        for index in range(len(g_data) - 1, -1, -1):
            time_str = g_data[index].text
            year = int(time_str[0:4])
            month = int(time_str[4:6])
            day = int(time_str[6:8])
            company_foreign_investors_shareholder_timeslice_list.append(datetime(year, month, day))
        return company_foreign_investors_shareholder_timeslice_list


    def __generate_time_slice_by_workday(self, datetime_start, datetime_end, time_slice_cfg=None):
# The data type in the list is datetime
        if self.workday_canlendar is None:
            self.workday_canlendar = WorkdayCanlendar.WebScrapyWorkdayCanlendar.Instance()
# Check time range
        if datetime_start < self.workday_canlendar.get_first_workday():
            g_logger.warn("The start workday [%s] is earlier than the first day[%s]" % (CMN.to_date_only_str(datetime_start), CMN.to_date_only_str(self.workday_canlendar.get_first_workday())))
            datetime_start = self.workday_canlendar.get_first_workday()
        if datetime_end > self.workday_canlendar.get_latest_workday():
            g_logger.warn("The end workday [%s] is later than the last day[%s]" % (CMN.to_date_only_str(datetime_end), CMN.to_date_only_str(self.workday_canlendar.get_latest_workday())))
            datetime_end = self.workday_canlendar.get_latest_workday()
# Return the iterable
        return WorkdayCanlendar.WebScrapyWorkdayCanlendarNearestIterator(datetime_start, datetime_end)
        # for timeslice in datetime_iterator:
        #     timeslice_list.append(timeslice)
        # return timeslice_list


    def __generate_time_slice_by_company_foreign_investors_shareholder(self, datetime_start, datetime_end, time_slice_cfg):
# The data type in the list is datetime
        if time_slice_cfg is None:
            raise ValueError("The config should NOT be NULL") 
        company_code_number = time_slice_cfg.get("company_code_number", None)
        if company_code_number is None:
            raise ValueError("Fail to find the 'company_code_number' in the config") 
        company_foreign_investors_shareholder_timeslice_list = self.__get_company_foreign_investors_shareholder_time_list(company_code_number)
        if company_foreign_investors_shareholder_timeslice_list is None:
            raise ValueError("Fail to find the company[%s] foreign investors sharehold time slice list" % company_code_number)
# Check time range
        if datetime_end < company_foreign_investors_shareholder_timeslice_list[0]:
            raise ValueError("The end day [%s] is earlier than the first one[%s]" % (CMN.to_date_only_str(datetime_end), CMN.to_date_only_str(company_foreign_investors_shareholder_timeslice_list[0])))
        if datetime_start > company_foreign_investors_shareholder_timeslice_list[-1]:
            raise ValueError("The start day [%s] is later than the last one[%s]" % (CMN.to_date_only_str(datetime_start), CMN.to_date_only_str(company_foreign_investors_shareholder_timeslice_list[-1])))

        if datetime_start < company_foreign_investors_shareholder_timeslice_list[0]:
            g_logger.warn("The start day [%s] is earlier than the first one[%s]" % (CMN.to_date_only_str(datetime_start), CMN.to_date_only_str(company_foreign_investors_shareholder_timeslice_list[0])))
            datetime_start = company_foreign_investors_shareholder_timeslice_list[0]
        if datetime_end > company_foreign_investors_shareholder_timeslice_list[-1]:
            g_logger.warn("The end day [%s] is later than the last one[%s]" % (CMN.to_date_only_str(datetime_end), CMN.to_date_only_str(company_foreign_investors_shareholder_timeslice_list[-1])))
            datetime_end = company_foreign_investors_shareholder_timeslice_list[-1]
# Define the iterator
        class TimeSliceIterator(object):
            def __init__(self, datetime_start, datetime_end, timeslice_list):
                self.time_to_stop = False
                # import pdb; pdb.set_trace()
                self.start_index = next((index for index, datetime_cur in enumerate(timeslice_list) if datetime_cur >= datetime_start), -1)
                if self.start_index == -1:
                    raise ValueError("Fail to find the start index of the start date: %s" % CMN.to_date_only_str(datetime_start))
                self.end_index = next((index for index, datetime_cur in reversed(list(enumerate(timeslice_list))) if datetime_cur <= datetime_end), -1)
                if self.end_index == -1:
                    raise ValueError("Fail to find the end index of the end date: %s" % CMN.to_date_only_str(datetime_end))
                self.cur_index = self.start_index
                self.timeslice_list = timeslice_list

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
                return self.timeslice_list[cur_index]
        return TimeSliceIterator(datetime_start, datetime_end, company_foreign_investors_shareholder_timeslice_list)


    def __generate_time_slice_by_month(self, datetime_start, datetime_end, time_slice_cfg=None):
# The data type in the list is datetime
# Define the iterator
        class TimeSliceIterator(object):
            def __init__(self, datetime_start, datetime_end):
                self.time_to_stop = False
                # import pdb; pdb.set_trace()
                self.datetime_cur = datetime(datetime_start.year, datetime_start.month, 1)
                self.datetime_end = datetime_end

            def __iter__(self):
                return self

            def next(self):
                # import pdb; pdb.set_trace()
                if self.time_to_stop:
                    raise StopIteration
                datetime_cur = self.datetime_cur
                if self.datetime_cur.year == self.datetime_end.year and self.datetime_cur.month == self.datetime_end.month:
                    self.time_to_stop = True
                if not self.time_to_stop:
                    last_day = CMN.get_cfg_month_last_day(self.datetime_cur)
                    self.datetime_cur += timedelta(days = last_day)
                return datetime_cur
# Check time range
        return TimeSliceIterator(datetime_start, datetime_end)


    def __generate_time_slice_by_financial_statement_season(self, datetime_start, datetime_end, time_slice_cfg=None):
# The data type in the list is a dictionay with the fields: year, season
        def calculate_season_number(financial_statement_season):
            return financial_statement_season['year'] * 10 + financial_statement_season['season']

        if self.financial_statement_last_season_cfg is None:
            self.financial_statement_last_season_cfg  = self.__get_financial_statement_season()
# Check time range
        financial_statement_start_season_cfg = self.__get_financial_statement_season(datetime_start)
        financial_statement_end_season_cfg = self.__get_financial_statement_season(datetime_end)
        if calculate_season_number(financial_statement_end_season_cfg) > calculate_season_number(self.financial_statement_last_season_cfg):
            g_logger.warn("The financial statement end season [%d%d] is later than the last one[%d%d]" % (financial_statement_end_season_cfg['year'], financial_statement_end_season_cfg['season'], self.financial_statement_last_season_cfg['year'], self.financial_statement_last_season_cfg['season']))
            financial_statement_end_season_cfg = self.financial_statement_last_season_cfg
# Define the iterator
        class TimeSliceIterator(object):
            def __init__(self, financial_statement_start_season_cfg, financial_statement_end_season_cfg):
                self.time_to_stop = False
                self.cur_year = financial_statement_start_season_cfg['year']
                self.cur_season_index = financial_statement_start_season_cfg['season']
                self.end_year = financial_statement_end_season_cfg['year']
                self.end_season_index = financial_statement_end_season_cfg['season']

            def __iter__(self):
                return self

            def next(self):
                # import pdb; pdb.set_trace()
                if self.time_to_stop:
                    raise StopIteration
                financial_statement_cur_season = {"year": self.cur_year, "season": self.cur_season_index + 1}
                if self.cur_year == self.end_year and self.cur_season_index == self.end_season_index:
                    self.time_to_stop = True
                if not self.time_to_stop:
                    if self.cur_season_index == 3:
                        self.cur_year += 1
                        self.cur_season_index = 0
                    else:
                        self.cur_season_index += 1
                return financial_statement_cur_season
# Return the iterable
        return TimeSliceIterator(financial_statement_start_season_cfg, financial_statement_end_season_cfg)


    def generate_time_slice(self, time_slice_type, datetime_start, datetime_end, time_slice_cfg=None):
        if datetime_start > datetime_end:
            raise RuntimeError("The Start Time[%s] should NOT be greater than the End Time[%s]" % (datetime_start, datetime_end))

        if self.datetime_today is None:
           self.datetime_today  = datetime.today()
        if datetime_end > self.datetime_today:
            g_logger.warn("The end day [%s] is later than the today[%s]" % (CMN.to_date_only_str(datetime_end), CMN.to_date_only_str(self.datetime_today)))
            datetime_end = self.datetime_today

        return (self.generate_time_slice_func_ptr[time_slice_type])(datetime_start, datetime_end, time_slice_cfg)
