# -*- coding: utf8 -*-

import os
import re
import json
import requests
import collections
import csv
import time
from abc import ABCMeta, abstractmethod
from random import randint
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_company_group_set as CompanyGroupSet
import web_scrapy_company_profile as CompanyProfile
import web_scrapy_url_time_range as URLTimeRange
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyStockBase(BASE.BASE.WebScrapyBase):

    # url_time_range = None
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyStockBase, self).__init__(cur_file_path, **kwargs)
        self.company_group_set = None
        if kwargs.get("company_group_set", None) is None:
            self.company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_group_set()
        else:
            self.company_group_set = kwargs["company_group_set"]
        # self.time_slice_kwargs["company_code_number"] = None
        self.new_csv_time_duration_dict = None
        self.scrapy_company_progress_count = 0
        self.company_profile = None
        self.cur_company_code_number = None # Caution: This value is updated every time when assemble_web_url() is called


    def __get_company_profile(self):
        if self.company_profile is None:
            self.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return self.company_profile


    def _get_url_time_range(self):
        # import pdb; pdb.set_trace()
        if self.url_time_range is None:
            self.url_time_range = URLTimeRange.WebScrapyURLTimeRange.Instance()
        return self.url_time_range


    def assemble_csv_company_folderpath(self, company_code_number, company_group_number=-1):
        if company_group_number == -1:
            company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
        csv_company_folderpath = "%s/%s%02d/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_STOCK_FOLDERNAME, int(company_group_number), company_code_number) 
        return csv_company_folderpath


    def assemble_csv_filepath(self, source_type_index, company_code_number, company_group_number=-1):
        if company_group_number == -1:
            company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
        # csv_filepath = "%s/%s%02d/%s/%s.csv" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_STOCK_FOLDERNAME, int(company_group_number), company_code_number, CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[source_type_index]) 
        # return csv_filepath
        return CMN.FUNC.assemble_stock_csv_filepath(self.xcfg["finance_root_folderpath"], source_type_index, company_code_number, company_group_number)


    def _check_old_csv_time_duration_exist(self, *args):
        if self.xcfg["csv_time_duration_table"] is None:
            return False 
        company_csv_time_duration_table = self.xcfg["csv_time_duration_table"].get(args[0], None)
        if company_csv_time_duration_table is None:
            return False
        if company_csv_time_duration_table.get(self.source_type_index, None) is None:
            return False
        return True


    def _adjust_csv_time_duration(self, company_code_number):
        # import pdb; pdb.set_trace()
# Limit the time range from the web site
        time_duration_after_lookup_time = (self._adjust_time_duration_start_and_end_time_func_ptr(self.xcfg["time_duration_type"]))(self.source_type_index, company_code_number)
# Determine the CSV/Web time duration
        web2csv_time_duration_update = None
        if self._check_old_csv_time_duration_exist(company_code_number):
            web2csv_time_duration_update = self._get_overlapped_web2csv_time_duration_update_cfg(
                self.xcfg["csv_time_duration_table"][company_code_number][self.source_type_index], 
                time_duration_after_lookup_time.time_duration_start, 
                time_duration_after_lookup_time.time_duration_end
            )
        else:
            web2csv_time_duration_update = self._get_non_overlapped_web2csv_time_duration_update_cfg(
                time_duration_after_lookup_time.time_duration_start, 
                time_duration_after_lookup_time.time_duration_end
            )
        if web2csv_time_duration_update.NeedUpdate:
            self.new_csv_time_duration_dict[company_code_number] = CMN.CLS.TimeDurationTuple(web2csv_time_duration_update.NewCSVStart, web2csv_time_duration_update.NewCSVEnd)
        return web2csv_time_duration_update


    def scrap_web_to_csv(self):
        # import pdb; pdb.set_trace()
        self.new_csv_time_duration_dict = {}
        self.scrapy_company_progress_count = 0
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
# Create a folder for a specific company
                csv_company_folderpath = self.assemble_csv_company_folderpath(company_code_number, company_group_number)
                CMN.FUNC.create_folder_if_not_exist(csv_company_folderpath)
                # import pdb; pdb.set_trace()
# Find the file path for writing data into csv
                csv_filepath = self.assemble_csv_filepath(self.source_type_index, company_code_number, company_group_number)
# Determine the actual time range
                web2csv_time_duration_update = self._adjust_csv_time_duration(company_code_number)
                if not web2csv_time_duration_update.NeedUpdate:
                    g_logger.debug("[%s:%s] %s %s:%s => The CSV data already cover this time range !!!" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], company_code_number, CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewCSVStart, web2csv_time_duration_update.NewCSVEnd))
                    continue
                scrapy_msg = "[%s:%s] %s %s:%s => %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], company_code_number, CMN.DEF.DEF_TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
                g_logger.debug(scrapy_msg)
# Check if only dry-run
                if self.xcfg["dry_run_only"]:
                    print scrapy_msg
                    continue
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
                if web2csv_time_duration_update.AppendDirection == BASE.BASE.WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
                    g_logger.debug("Need add the new data in front of the old CSV data, rename the file: %s" % (csv_filepath + ".old"))
                    CMN.FUNC.rename_file_if_exist(csv_filepath, csv_filepath + ".old") 
# Create the time slice iterator due to correct time range
                # import pdb; pdb.set_trace()
# Update the time range of time slice
                time_slice_generator_cfg = {"company_code_number": company_code_number, "time_duration_start": web2csv_time_duration_update.NewWebStart, "time_duration_end": web2csv_time_duration_update.NewWebEnd,}
                # import pdb; pdb.set_trace()
# Generate the time slice
                timeslice_iterable = self._get_timeslice_iterable(**time_slice_generator_cfg)
                csv_data_list_each_year = []
                cur_year = None
                # import pdb; pdb.set_trace()
# Generate the time slice list                
                for timeslice in timeslice_iterable:
# Write the data into csv year by year
                    if timeslice.year != cur_year:
                        if len(csv_data_list_each_year) > 0:
                            # import pdb; pdb.set_trace()
                            self._write_to_csv(csv_filepath, csv_data_list_each_year, self.source_url_parsing_cfg["url_multi_data_one_page"])
                            csv_data_list_each_year = []
                        cur_year = timeslice.year
                    url = self.assemble_web_url(timeslice, company_code_number)
                    g_logger.debug("Get the data from URL: %s" % url)
                    try:
# Grab the data from website and assemble the data to the entry of CSV
                        csv_data_list = self._parse_web_data(self._get_web_data(url))
                        if csv_data_list is None:
                            raise RuntimeError(url)
                        csv_data_list_each_year.append(csv_data_list)
                    except CMN.EXCEPTION.WebScrapyNotFoundException as e:
                        # import pdb;pdb.set_trace()
                        if isinstance(e.message, str):
                            g_logger.error("Fail to scrap URL[%s], due to: %s" % (url, e.message))
                        else:
                            g_logger.error(u"Fail to scrap URL[%s], due to: %s" % (url, e.message))
                        raise e
                    except Exception as e:
                        import pdb;pdb.set_trace()
                        if isinstance(e.message, str):
                            g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, e.message))
                        else:
                            g_logger.warn(u"Fail to scrap URL[%s], due to: %s" % (url, e.message))
# Write the data of last year into csv
                if len(csv_data_list_each_year) > 0:
                    self._write_to_csv(csv_filepath, csv_data_list_each_year, self.source_url_parsing_cfg["url_multi_data_one_page"])
# Append the old CSV data after the new web data if necessary
                if web2csv_time_duration_update.AppendDirection == BASE.BASE.WebScrapyBase.Web2CSVTimeRangeUpdate.WEB2CSV_APPEND_FRONT:
                    g_logger.debug("Append the old CSV data to the file: %s" % csv_filepath)
                    CMN.FUNC.append_data_into_file(csv_filepath + ".old", csv_filepath)
                    CMN.FUNC.remove_file_if_exist(csv_filepath + ".old") 
                self.scrapy_company_progress_count += 1


    def get_new_csv_time_duration_dict(self):
# No matter the csv time range would be updated, the new time duration is required to re-write into the config file
        assert self.new_csv_time_duration_dict is not None, "self.new_csv_time_duration_dict should NOT be None"
        return self.new_csv_time_duration_dict


    def assemble_web_url(self, timeslice, company_code_number):
        # raise NotImplementedError
        self.cur_company_code_number = company_code_number


    @property
    def CompanyProgressCount(self):
        return self.scrapy_company_progress_count

##########################################################################

class WebScrapyStockStatementBase(WebScrapyStockBase):

    __metaclass__ = ABCMeta
    def __init__(self, cur_file_path, **kwargs):
        super(WebScrapyStockStatementBase, self).__init__(cur_file_path, **kwargs)
        if not kwargs.get("renew_statement_field", False):
            if self.TABLE_FIELD_INTEREST_TITLE_LIST is None:
                raise ValueError("TABLE_FIELD_INTEREST_TITLE_LIST is None")


    @classmethod
    def _init_statement_field_class_variables(cls, conf_filename):
        if not CMN.FUNC.check_config_file_exist(conf_filename):
            raise CMN.EXCEPTION.WebScrapyNotFoundException("The %s file does NOT exist" % conf_filename)
        table_field_title_list = CMN.FUNC.read_config_file_lines_ex(conf_filename, "rb")
        cls.TABLE_FIELD_INTEREST_TITLE_LIST = [title for title in table_field_title_list if title not in cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST]
        cls.TABLE_FIELD_INTEREST_TITLE_INDEX_DICT = {title: title_index for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST)}
        cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST)
        cls.TABLE_FIELD_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_FIELD_INTEREST_TITLE_LIST)
        cls.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = collections.defaultdict(lambda: cls.TABLE_FIELD_INTEREST_DEFAULT_ENTRY_LEN)


    def _modify_time_for_timeslice_generator(self, finance_time_start, finance_time_end):
        assert finance_time_start.get_time_unit_type() == CMN.DEF.DATA_TIME_UNIT_DAY, "The input start time unit type should be %d, not %d" % (CMN.DEF.DATA_TIME_UNIT_DAY, finance_time_start.get_time_unit_type())
        assert finance_time_end.get_time_unit_type() == CMN.DEF.DATA_TIME_UNIT_DAY, "The input end time unit type should be %d, not %d" % (CMN.DEF.DATA_TIME_UNIT_DAY, finance_time_end.get_time_unit_type())
        finance_quarter_start = CMN.CLS.FinanceQuarter.get_start_finance_quarter_from_date(finance_time_start)
        finance_quarter_end = CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(finance_time_end)
        return (finance_quarter_start, finance_quarter_end)


    def _insert_not_exist_statement_element(self, dst_statement_field_list, src_statement_list):
        dst_index = 0
        src_index = 0
        dst_statement_field_list_len = len(dst_statement_field_list)
        # import pdb; pdb.set_trace()
        for src_data in src_statement_list:
            data_found = False
            try:
                cur_offset = (dst_statement_field_list[dst_index:]).index(src_data)
                dst_index = dst_index + cur_offset + 1
                # data_found = True
            except ValueError:
    # New element, insert the data into the list
                if dst_index == dst_statement_field_list_len:
                    dst_statement_field_list.append(src_data)
                else:
                    dst_statement_field_list.insert(dst_index, src_data)
                dst_index += 1
                dst_statement_field_list_len += 1
            # print "FUNC: %s" % dst_statement_field_list


    def update_statement_field_to_file(self, dst_statement_field_list):
        # import pdb; pdb.set_trace()
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
# Create the time slice iterator due to correct time range
                # import pdb; pdb.set_trace()
# Update the time range of time slice
# Limit the time range from the web site
                time_duration_after_lookup_time = (self._adjust_time_duration_start_and_end_time_func_ptr(self.xcfg["time_duration_type"]))(self.source_type_index, company_code_number)
                g_logger.debug("Update statement field => [%s:%s] %s:%s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[self.source_type_index], company_code_number, time_duration_after_lookup_time.time_duration_start, time_duration_after_lookup_time.time_duration_end))
                time_slice_generator_cfg = {"company_code_number": company_code_number, "time_duration_start": time_duration_after_lookup_time.time_duration_start, "time_duration_end": time_duration_after_lookup_time.time_duration_end,}
# Generate the time slice
                timeslice_iterable = self._get_timeslice_iterable(**time_slice_generator_cfg)
                # import pdb; pdb.set_trace()
# Generate the time slice list                
                for timeslice in timeslice_iterable:
                    url = self.assemble_web_url(timeslice, company_code_number)
                    g_logger.debug("Get the statement data from URL: %s" % url)
# Grab the data from website and assemble the data to the entry of CSV
                    company_statement_field_list = self._parse_web_statement_field_data(self._get_web_data(url))
                    if company_statement_field_list is None:
                        raise RuntimeError(url)
                    self._insert_not_exist_statement_element(dst_statement_field_list, company_statement_field_list)


    def assemble_web_url(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        super(WebScrapyStockStatementBase, self).assemble_web_url(timeslice, company_code_number)
        url = self.url_format.format(
            *(
                company_code_number,
                timeslice.year - 1911, 
                "%02d" % timeslice.quarter,
            )
        )
        return url


    def _parse_web_statement_field_data_internal(self, web_data, web_data_start_index, web_data_end_index=None):
        web_data_len = len(web_data)
        if web_data_len == 0:
            return None
        if web_data_end_index is None:
            web_data_end_index = web_data_len
        data_list = []
# Filter the data which is NOT interested in
        for tr in web_data[web_data_start_index:web_data_end_index]:
        #     print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            # data_list.append(td[0].text.encode(CMN.DEF.URL_ENCODING_UTF8))
            data_list.append(td[0].text)
        return data_list


    def _parse_web_data_internal(self, web_data, web_data_start_index, web_data_end_index=None):
        web_data_len = len(web_data)
        if web_data_len == 0:
            return None
        if web_data_end_index is None:
            web_data_end_index = web_data_len
        # import pdb; pdb.set_trace()
        data_list = []
        table_field_list = [None] * self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN
        interest_index = 0
        not_interest_index = 0
# Filter the data which is NOT interested in
        for index, tr in enumerate(web_data[web_data_start_index:web_data_end_index]):
        #     print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            data_found = False
            data_can_ignore = False
            data_index = None
            title = td[0].text.encode(CMN.DEF.URL_ENCODING_UTF8)
            # import pdb; pdb.set_trace()
            if interest_index < self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN:
                try:
                    # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
                    data_index = cur_interest_index = (self.TABLE_FIELD_INTEREST_TITLE_LIST[interest_index:]).index(title) + interest_index
                    interest_index = cur_interest_index + 1
                    data_found = True
                except ValueError:
                    pass
            if not data_found:
                if not_interest_index < self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN:
                    try:
                        cur_not_interest_index = (self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST[not_interest_index:]).index(title) + not_interest_index
                        not_interest_index = cur_not_interest_index + 1
                        data_can_ignore = True
                    except ValueError:
                        pass                
# Check if the entry is NOT in the title list of interest
            if (not data_found) and (not data_can_ignore):
                # import pdb; pdb.set_trace()
                raise CMN.EXCEPTION.WebScrapyNotFoundException(u"The title[%s] in company[%s] does NOT exist in the title list of interest" % (title, self.cur_company_code_number))
            if data_can_ignore:
                continue
# Parse the content of this entry, and the interested field into data structure
            entry_list_entry = self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT[title]
            # print "data_index: %d, title: [%s]" % (data_index, title)
            # import pdb;pdb.set_trace()
            field_index_list = None
            if isinstance(entry_list_entry, list):
                field_index_list = entry_list_entry
            else:
                field_index_list = range(1, entry_list_entry + 1)
            table_field_list[data_index] = []
            for field_index in field_index_list:
                # import pdb; pdb.set_trace()
                # print "data_index: %d, field_index: %d, data: [%s]" % (data_index, field_index, td[field_index].text)
                field_value = str(td[field_index].text).strip(" ").replace(",", "")
                if field_value.find('.') == -1: # Integer
                    table_field_list[data_index].append(int(field_value))
                else: # Floating point
                    table_field_list[data_index].append(float(field_value))
# Transforms the table into the 1-Dimension list
        padding_entry = "0" 
        for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
            if table_field_list[index] is None:
# Padding
                entry_list_len = entry_list_entry = self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT[self.TABLE_FIELD_INTEREST_TITLE_LIST[index]]
                # print "data_index: %d, title: [%s]" % (data_index, title)
                if isinstance(entry_list_entry, list):
                    entry_list_len = len(entry_list_entry)
                data_list.extend([padding_entry] * entry_list_len)
            else:
                data_list.extend(table_field_list[index])
        return data_list


    @abstractmethod
    def _parse_web_statement_field_data(self, web_data):
        pass


    @abstractmethod
    def _parse_web_data(self, web_data):
        pass
