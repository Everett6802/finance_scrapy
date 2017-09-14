# -*- coding: utf8 -*-

import re
import requests
import json
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 三大法人個股買賣超日報
class WebScrapyTop3LegalPersonsStockNetBuyOrSellSummaryBase(WebScrapyStockBase.WebScrapyStockBase):

    __metaclass__ = ABCMeta
    CSV_DATA_BUF_SIZE = 20
    WEB_DATA_ENTRY_COMPANY_CODE_NUMBER = 0

    # @classmethod
    # def assemble_web_url(cls, timeslice, company_code_number, *args):
    #     url = self.URL_FORMAT.format(
    #         *(
    #             timeslice.year, 
    #             "%02d" % timeslice.month,
    #             "%02d" % timeslice.day
    #         )
    #     )
    #     return url


    def __init__(self, **kwargs):
        super(WebScrapyTop3LegalPersonsStockNetBuyOrSellSummaryBase, self).__init__(**kwargs)
        self.company_number_csv_time_range_dict = None
        self.company_group_out_of_csv_time_range_set = None
        self.time_duration_after_lookup_time = None
        self.company_number_csv_data_dict = None


    def _scrape_web_data(self, timeslice, company_code_number):
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        data_list = []
        for data_entry in web_data:
            element_list = []
            company_number = "%s" % str(data_entry[0]).strip(' ')
            if not re.match("^[\d][\d]{2}[\d]$", company_number):
                continue
# Company code number
            element_list.append(company_number)
# 外資
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[2])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[3])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[4])))
# 投信
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[5])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[6])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[7])))
# 自營商
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[9]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[12])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[10]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[13])))
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[11]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[14])))
# 三大法人
            element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[15])))
# Add the entry
            data_list.append(element_list)
        return data_list
# 證券代號
# 外資及陸資買進股數
# 外資及陸資賣出股數
# 外資及陸資淨買股數
# 投信買進股數
# 投信賣出股數
# 投信淨買股數
# 自營商買進股數(自行買賣) + 自營商買進股數(避險)
# 自營商賣出股數(自行買賣) + 自營商賣出股數(避險)
# 自營商(自行買賣)淨買股數 + 自營商(避險)淨買股數
# 三大法人買賣超股數


    def __write_each_company_to_csv(self, company_code_number, csv_data_list, web2csv_time_duration_update, need_append_old_csv=True):
# Find the file path for writing data into csv
        csv_filepath = self.assemble_csv_filepath(self.SOURCE_TYPE_INDEX, company_code_number)
        scrapy_msg = "[%s:%s] %s %d => %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SOURCE_TYPE_INDEX], company_code_number, CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], len(csv_data_list), csv_filepath)
        g_logger.debug(scrapy_msg)
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
        web2csv_time_duration_update.backup_old_csv_if_necessary(csv_filepath, True)
# Create a folder for a specific company
        csv_company_folderpath = self.assemble_csv_company_folderpath(company_code_number)
        CMN.FUNC.create_folder_if_not_exist(csv_company_folderpath)
# Write data to csv
        self._write_to_csv(csv_filepath, csv_data_list)
# Merge the CSV files
        if need_append_old_csv:
            web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath, True)


    def __update_each_company_csv_data(self, finance_date, all_company_csv_data_list):
        for all_company_csv_data_entry in all_company_csv_data_list:
            company_code_number = all_company_csv_data_entry[self.WEB_DATA_ENTRY_COMPANY_CODE_NUMBER]
        for all_company_csv_data_entry in all_company_csv_data_list:
            company_code_number = all_company_csv_data_entry[self.WEB_DATA_ENTRY_COMPANY_CODE_NUMBER]
# Find the csv time range in the initail update
# If time range is out of the csv data range....
            if self.company_group_out_of_csv_time_range_set.is_current_company_exist(company_code_number):
                continue
            web2csv_time_duration_update_list = None
# Limit the searching time range from the local CSV data
            if not self.company_number_csv_time_range_dict.has_key(company_code_number):
                web2csv_time_duration_update_tuple = self._adjust_time_range_from_csv(self.time_duration_after_lookup_time, company_code_number)
                if web2csv_time_duration_update_tuple is None:
                    self.csv_file_no_scrapy_record.add_csv_file_already_exist_record(self.SOURCE_TYPE_INDEX, company_code_number)
                    g_logger.debug("[%s:%s] %s %s:%s => The CSV data already cover this time range !!!" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SOURCE_TYPE_INDEX], company_code_number, CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], self.xcfg["csv_time_duration_table"][company_code_number][self.SOURCE_TYPE_INDEX].time_duration_start, self.xcfg["csv_time_duration_table"][company_code_number][self.SOURCE_TYPE_INDEX].time_duration_end))
                    self.company_group_out_of_csv_time_range_set.add_company(company_code_number)
                    continue
                self.company_number_csv_time_range_dict[company_code_number] = []
                for web2csv_time_duration_update in web2csv_time_duration_update_tuple:
                    self.company_number_csv_time_range_dict[company_code_number].append(web2csv_time_duration_update)
            web2csv_time_duration_update_list = self.company_number_csv_time_range_dict[company_code_number]
            web2csv_time_duration_update = web2csv_time_duration_update_list[0]
            need_write_csv = False
            need_append_old_csv = False
# Update the web data
            if not CMN.FUNC.is_time_in_range(web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, finance_date):
                if web2csv_time_duration_update.AppendDirection == WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_BEFORE:
                    need_append_old_csv = True
                    web2csv_time_duration_update_list.pop(0)
                    if len(web2csv_time_duration_update_list) == 0:
# All data are scraped
                        self.company_number_csv_time_range_dict.pop(company_code_number)
                        self.company_group_out_of_csv_time_range_set.add_company(company_code_number)
                    need_write_csv = True
            else:
                if not self.company_number_csv_data_dict.has_key(company_code_number):
                    self.company_number_csv_data_dict[company_code_number] = []
                self.company_number_csv_data_dict[company_code_number].append(all_company_csv_data_entry[1:])
                if len(self.company_number_csv_data_dict[company_code_number]) == self.CSV_DATA_BUF_SIZE:
                    need_write_csv = True
            if need_write_csv:
                csv_data_list = self.company_number_csv_data_dict[company_code_number]
                self.__write_each_company_to_csv(company_code_number, csv_data_list, web2csv_time_duration_update, need_append_old_csv)
                self.company_number_csv_data_dict[company_code_number] = []


    def __update_each_company_last_csv_data(self):
# Check the last data and write into CSV if necessary
        for company_code_number, csv_data_list in self.company_number_csv_data_dict.items():
# Append the old CSV data after the new web data if necessary
            assert self.company_number_csv_time_range_dict.has_key(company_code_number), "company_number_csv_time_range_dict does NOT costain the key: %s" % company_code_number
            web2csv_time_duration_update_list = self.company_number_csv_time_range_dict[company_code_number]
            assert len(web2csv_time_duration_update_list) == 1, "The length of web2csv_time_duration_update_list should be 1, not %d" % len(web2csv_time_duration_update_list)
# Write the last data into CSV
            self.__write_each_company_to_csv(company_code_number, csv_data_list, web2csv_time_duration_update_list[0])


    def _calculate_progress_amount(self, **kwargs):
        self.progress_amount = self._get_timeslice_iterable_len(**kwargs)


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
# Create the time slice iterator due to correct time range
# Limit the searching time range from the web site
        self.time_duration_after_lookup_time = self._adjust_time_range_from_web(self.SOURCE_TYPE_INDEX, None)
        if self.time_duration_after_lookup_time is None:
            # self.csv_file_no_scrapy_record.add_time_range_not_overlap_record(self.SOURCE_TYPE_INDEX, company_code_number)
            g_logger.debug("[%s:%s] %s => The search time range is NOT in the time range of web data !!!" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SOURCE_TYPE_INDEX], company_code_number, CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]]))
            return
        self.company_number_csv_time_range_dict = {}
        self.company_number_csv_data_dict = {}
        self.company_group_out_of_csv_time_range_set = WebScrapyCompanyGroupSet.CompanyGroupSet()
        timeslice_generator_cfg = {"time_duration_start": time_duration_after_lookup_time.time_duration_end, "time_duration_end": time_duration_after_lookup_time.time_duration_end,}
        timeslice_iterable = self._get_timeslice_iterable(**timeslice_generator_cfg)
        self._calculate_progress_amount(**timeslice_generator_cfg)
        for timeslice in timeslice_iterable:
# Scrape the web data
            web_data = self._scrape_web_data(timeslice, None)
            if len(web_data) == 0:
# Keep track of the time range in which the web data is empty
                self.csv_file_no_scrapy_record.add_web_data_not_found_record(timeslice, self.SOURCE_TYPE_INDEX, CMN.DEF.TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_DUMMY_COMPANY_CODE_NUMBER)
            else:
                csv_data_list = self._parse_web_data(web_data)
                if len(csv_data_list) == 0:
                    raise CMN.EXCEPTION.WebScrapyNotFoundException("No entry in the web data from URL: %s" % url)
                self.__update_each_company_csv_data(timeslice, csv_data_list)
# Flush the last data into the list if required
            self.csv_file_no_scrapy_record.add_web_data_not_found_record(None, self.SOURCE_TYPE_INDEX, TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_DUMMY_COMPANY_CODE_NUMBER)
# Write the last data in the buf
            self.__update_each_company_last_csv_data()
# Increase the progress count
            self.progress_count += 1
# Parse csv file status
        self._parse_csv_file_status_to_string_list()

#####################################################################

# 上市三大法人個股買賣超日報
class WebScrapyTop3LegalPersonsStockNetBuyOrSellSummary(WebScrapyTop3LegalPersonsStockNetBuyOrSellSummaryBase):

    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = self.URL_FORMAT.format(
            *(
                timeslice.year, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, **kwargs):
        super(WebScrapyTop3LegalPersonsStockNetBuyOrSellSummary, self).__init__(**kwargs)


    # def _scrape_web_data(self, timeslice, company_code_number):
    #     url = self.assemble_web_url(timeslice, company_code_number)
    #     web_data = self.try_get_web_data(url)
    #     return web_data


#     def _parse_web_data(self, web_data):
#         # import pdb; pdb.set_trace()
#         data_list = []
#         for data_entry in web_data:
#             element_list = []
#             company_number = "%s" % str(data_entry[0]).strip(' ')
#             if not re.match("^[\d][\d]{2}[\d]$", company_number):
#                 continue
# # Company code number
#             element_list.append(company_number)
# # 外資
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[2])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[3])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[4])))
# # 投信
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[5])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[6])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[7])))
# # 自營商
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[9]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[12])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[10]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[13])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[11]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[14])))
# # 三大法人
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[15])))
# # Add the entry
#             data_list.append(element_list)
#         return data_list
# # 證券代號
# # 外資及陸資買進股數
# # 外資及陸資賣出股數
# # 外資及陸資淨買股數
# # 投信買進股數
# # 投信賣出股數
# # 投信淨買股數
# # 自營商買進股數(自行買賣) + 自營商買進股數(避險)
# # 自營商賣出股數(自行買賣) + 自營商賣出股數(避險)
# # 自營商(自行買賣)淨買股數 + 自營商(避險)淨買股數
# # 三大法人買賣超股數


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date=105%2F03%2F23&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date=105%2F03%2F23&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/fund/T86?response=json&date=20170804&selectType=ALL")
        #print res.text
        res.encoding = 'utf-8'
        g_data = json.loads(res.text)['data']
        if not silent_mode: 
            for entry in g_data:
                print entry[0], entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7], entry[8], entry[9], entry[10], entry[11], entry[12], entry[13], entry[14], entry[15]
# 代號
# 名稱
# 外資及陸資買股數
# 外資及陸資賣股數
# 外資及陸資淨買股數
# 投信買股數
# 投信賣股數
# 投信淨買股數
# 自營商淨買股數
# 自營商(自行買賣)買股數
# 自營商(自行買賣)賣股數
# 自營商(自行買賣)淨買股數
# 自營商(避險)買股數
# 自營商(避險)賣股數
# 自營商(避險)淨買股數
# 三大法人買賣超股數

# ==== result: ====
# 0050 台灣50           9,093,000 480,000 0 0 3,000 572,000 950,000 545,000 8,449,000
# 0051 中100            0 0 0 0 0 0 4,000 6,000 -2,000
# 0053 寶電子           0 0 0 0 0 0 9,000 0 9,000
# 0054 台商50           0 0 0 0 0 0 0 1,000 -1,000
# ......

###########################################################################

# 上櫃三大法人個股買賣超日報
class WebScrapyOTCTop3LegalPersonsStockNetBuyOrSellSummary(WebScrapyTop3LegalPersonsStockNetBuyOrSellSummaryBase):

    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = self.URL_FORMAT.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, **kwargs):
        super(WebScrapyOTCTop3LegalPersonsStockNetBuyOrSellSummary, self).__init__(**kwargs)


#     def _scrape_web_data(self, timeslice, company_code_number):
#         url = self.assemble_web_url(timeslice, company_code_number)
#         web_data = self.try_get_web_data(url)
#         return web_data


#     def _parse_web_data(self, web_data):
#         # import pdb; pdb.set_trace()
#         data_list = []
#         for data_entry in web_data:
#             element_list = []
#             company_number = "%s" % str(data_entry[0]).strip(' ')
#             if not re.match("^[\d][\d]{2}[\d]$", company_number):
#                 continue
# # Company code number
#             element_list.append(company_number)
# # 外資
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[2])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[3])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[4])))
# # 投信
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[5])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[6])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[7])))
# # 自營商
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[9]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[12])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[10]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[13])))
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[11]) + CMN.FUNC.transform_share_number_string_to_board_lot(data[14])))
# # 三大法人
#             element_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(data[15])))
# # Add the entry
#             data_list.append(element_list)
#         return data_list
# # 證券代號
# # 外資及陸資買進股數
# # 外資及陸資賣出股數
# # 外資及陸資淨買股數
# # 投信買進股數
# # 投信賣出股數
# # 投信淨買股數
# # 自營商買進股數(自行買賣) + 自營商買進股數(避險)
# # 自營商賣出股數(自行買賣) + 自營商賣出股數(避險)
# # 自營商(自行買賣)淨買股數 + 自營商(避險)淨買股數
# # 三大法人買賣超股數


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=105/04/01&_=1460104675945")
        # res = CMN.FUNC.request_from_url_and_check_return("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=105/04/01&_=1460104675945")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d=106/08/15")
        json_res = json.loads(res.text)
        g_data = json_res['aaData']
        if not silent_mode: 
            for entry in g_data:
                print entry[0], entry[1], entry[2], entry[3], entry[4], entry[5], entry[6], entry[7], entry[8], entry[9], entry[10], entry[11], entry[12], entry[13], entry[14], entry[15]
# 代號
# 名稱
# 外資及陸資買股數
# 外資及陸資賣股數
# 外資及陸資淨買股數
# 投信買股數
# 投信賣股數
# 投信淨買股數
# 自營商淨買股數
# 自營商(自行買賣)買股數
# 自營商(自行買賣)賣股數
# 自營商(自行買賣)淨買股數
# 自營商(避險)買股數
# 自營商(避險)賣股數
# 自營商(避險)淨買股數
# 三大法人買賣超股數

# ==== result: ====
# 006201,0,0,0,0,0,0,32,000,0,0,0,32,000,0,32,000,32,000
# 1258,5,000,0,5,000,0,0,0,0,0,0,0,0,0,0,5,000
# 1264,2,000,0,2,000,5,000,0,5,000,-10,000,0,10,000,-10,000,0,0,0,-3,000
# 1333,0,1,000,-1,000,0,0,0,0,0,0,0,0,0,0,-1,000
# ......
