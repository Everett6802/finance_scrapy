# -*- coding: utf8 -*-

import os
import re
import json
import requests
import csv
import time
import copy
import collections
from abc import ABCMeta, abstractmethod
# from random import randint
# from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import scrapy_stock_base as ScrapyStockBase
# import web_scrapy_company_group_set as CompanyGroupSet
# import web_scrapy_company_profile as CompanyProfile
# import stock_url_time_range as URLTimeRange
g_logger = CMN.LOG.get_logger()


class ScrapyStatementBase(ScrapyStockBase.ScrapyStockBase):

    __metaclass__ = ABCMeta
    TABLE_COLUMN_FIELD_EXIST = False
    ALIAS_DATA_SPLIT = ":ALIAS:"


    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
# CAUTION: This function MUST be called by the LEAF derived class
        url = cls.URL_FORMAT.format(
            *(
                company_code_number,
                timeslice.year - 1911, 
                "%02d" % timeslice.quarter,
            )
        )
        return url

    # @classmethod
    # def _init_statement_field_class_variables(cls, conf_filename):
    #     # import pdb; pdb.set_trace()
    #     if not CMN.FUNC.check_config_file_exist(conf_filename):
    #         raise CMN.EXCEPTION.WebScrapyNotFoundException("The %s file does NOT exist" % conf_filename)
    #     table_field_title_list = None
    #     table_column_field_title_list = None
    #     if cls.TABLE_COLUMN_FIELD_EXIST:
    #         total_table_field_title_list = CMN.FUNC.read_config_file_lines_ex(conf_filename, "rb")
    #         try:
    #             column_field_start_flag_index = total_table_field_title_list.index(CMN.DEF.COLUMN_FIELD_START_FLAG_IN_CONFIG)
    #             table_field_title_list = copy.deepcopy(total_table_field_title_list[0:column_field_start_flag_index])
    #             table_column_field_title_list = copy.deepcopy(total_table_field_title_list[column_field_start_flag_index + 1:])
    #         except ValueError:
    #             raise CMN.EXCEPTION.WebScrapyIncorrectFormatException("The column field flag is NOT found in the config file" % conf_filename)        
    #     else:
    #         table_field_title_list = CMN.FUNC.read_config_file_lines_ex(conf_filename, "rb")
    #     cls.TABLE_FIELD_INTEREST_TITLE_LIST = [title for title in table_field_title_list if title not in cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST]
    #     # for title in table_field_title_list:
    #     #     if title not in cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST:
    #     #         print "Interested title: %s" % title
    #     #     else:
    #     #         print "Non-Interested title: %s" % title
    #     # import pdb; pdb.set_trace()
    #     # cls.TABLE_FIELD_INTEREST_TITLE_INDEX_DICT = {title: title_index for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST)}
    #     cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_FIELD_NOT_INTEREST_TITLE_LIST)
    #     cls.TABLE_FIELD_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_FIELD_INTEREST_TITLE_LIST)
    #     if cls.TABLE_COLUMN_FIELD_EXIST:
    #         cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST = [title for title in table_column_field_title_list if title not in cls.TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST]           
    #         cls.TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST)
    #         cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST)
    #     else:
    #         cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT = collections.defaultdict(lambda: cls.TABLE_FIELD_INTEREST_ENTRY_LEN)
    @classmethod
    def _init_statement_field_class_variables(cls, conf_filename):
        # import pdb; pdb.set_trace()
        if not CMN.FUNC.check_config_file_exist(conf_filename):
            raise CMN.EXCEPTION.WebScrapyNotFoundException("The %s file does NOT exist" % conf_filename)
# Read the data from the config file
        table_field_data_list = None
        table_column_field_data_list = None
        if cls.TABLE_COLUMN_FIELD_EXIST:
            total_table_field_data_list = CMN.FUNC.read_config_file_lines_ex(conf_filename, "rb")
            try:
                column_field_start_flag_index = total_table_field_data_list.index(CMN.DEF.COLUMN_FIELD_START_FLAG_IN_CONFIG)
                table_field_data_list = copy.deepcopy(total_table_field_data_list[0:column_field_start_flag_index])
                table_column_field_data_list = copy.deepcopy(total_table_field_data_list[column_field_start_flag_index + 1:])
            except ValueError:
                raise CMN.EXCEPTION.WebScrapyIncorrectFormatException("The column field flag is NOT found in the config file" % conf_filename)        
        else:
            table_field_data_list = CMN.FUNC.read_config_file_lines_ex(conf_filename, "rb")
# Parse the config content
        cls.TABLE_FIELD_INTEREST_TITLE_LIST = []
        cls.TABLE_FIELD_INTEREST_ALIAS_TITLE_DICT = {}
        cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT = collections.defaultdict(lambda: cls.TABLE_FIELD_INTEREST_ENTRY_LEN)
        for table_field_data in table_field_data_list:
            if table_field_data.find(cls.ALIAS_DATA_SPLIT) == -1:
# Normal type title
                data_array = table_field_data.split(CMN.DEF.COLON_DATA_SPLIT)
                field_title = data_array[0]
                cls.TABLE_FIELD_INTEREST_TITLE_LIST.append(field_title)
                if len(data_array) == 2:
                    field_interest_entry = [int(field_index) for field_index in data_array[1].split(CMN.DEF.COMMA_DATA_SPLIT)]
                    cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[field_title] = field_interest_entry
                elif len(data_array) > 2:
                    raise CMN.EXCEPTION.WebScrapyIncorrectFormatException("Incorrect field config format: %s" % table_field_data)
            else:
# Alias type title
                data_array = table_field_data.split(cls.ALIAS_DATA_SPLIT)
                if len(data_array) != 2:
                    raise CMN.EXCEPTION.WebScrapyIncorrectFormatException("Incorrect field config format: %s" % table_field_data)
                cls.TABLE_FIELD_INTEREST_ALIAS_TITLE_DICT[data_array[0]] = data_array[1]
        cls.TABLE_FIELD_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_FIELD_INTEREST_TITLE_LIST)
# Initialize the column field title if required
        if cls.TABLE_COLUMN_FIELD_EXIST:
            cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST = []
            for table_column_field_data in table_column_field_data_list:
                cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST.append(table_column_field_data)
            cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST_LEN = len(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST)


    @classmethod
    def post_check_web_data(cls, web_data):
        for entry in web_data:
            if re.search(r"查詢過於頻繁,請稍後再試", entry.text.encode(CMN.DEF.URL_ENCODING_UTF8), re.U):
                raise CMN.EXCEPTION.WebScrapyServerBusyException("Server is busy... try again later on !!!")


    @classmethod
    def _show_statement_field_dimension_internal(cls, interest_conf_filename, auto_gen_sql_element):
        # import pdb; pdb.set_trace()
        field_count = len(cls.TABLE_FIELD_INTEREST_TITLE_LIST)
        field_element_count = 0
        table_field_interest_description_list = []
        for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
            entry_list_entry = cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
            field_entry_index_list = None
            if isinstance(entry_list_entry, list):
                # print u"title: %s, entry_list_entry: %s" % (title, entry_list_entry)
                field_entry_index_list = entry_list_entry
                field_element_count += len(entry_list_entry)
            else:
                # print "title: %s, entry_list_entry: %d" % (title, entry_list_entry)
                table_field_interest_entry_end_index = cls.TABLE_FIELD_INTEREST_ENTRY_START_INDEX + cls.TABLE_FIELD_INTEREST_ENTRY_LEN
                field_entry_index_list = range(cls.TABLE_FIELD_INTEREST_ENTRY_START_INDEX, table_field_interest_entry_end_index)
                field_element_count += entry_list_entry
# Remove the whitespace and Add the field index list
            field_string_entry_index_list = [str(field_entry_index) for field_entry_index in field_entry_index_list]
            title_without_whitesapce = re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE) + u"[%d]: %s" % (title_index, ",".join(field_string_entry_index_list))
            table_field_interest_description_list.append(title_without_whitesapce.encode(CMN.DEF.URL_ENCODING_UTF8))
        field_count_msg = "Field Count: %d, Field Element Count: %d" % (field_count, field_element_count)
        CMN.FUNC.try_print(field_count_msg)
        table_field_interest_description_list.insert(0, field_count_msg)

        # import pdb; pdb.set_trace()
##############################################################################
# Generate the Table Field Definition/Table Field Type Definition list element
# for the finance_recorder_java project
##############################################################################
        if auto_gen_sql_element:
            table_field_interest_description_list.append(u"\n\n".encode(CMN.DEF.URL_ENCODING_UTF8))
# Table Field Definition
            table_field_interest_description_list.append(u"##### TABLE_FIELD_DEFINITION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"\"date\", // 日期".encode(CMN.DEF.URL_ENCODING_UTF8))
            field_count = 0
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                entry_list_entry = cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
                field_index_list_len = len(entry_list_entry) if isinstance(entry_list_entry, list) else entry_list_entry
                for entry_index in range(field_index_list_len):
                    field_count += 1
                    table_field_element_definition = u"\"value%d\", // %s" % (field_count, re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE))
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
# Table Field Type Definition
            table_field_interest_description_list.append(u"##### TABLE_FIELD_TYPE_DEFINITION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"\"DATE NOT NULL PRIMARY KEY\", // 日期".encode(CMN.DEF.URL_ENCODING_UTF8))
            # field_type_count = 0
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                entry_list_entry = cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
                field_index_list_len = len(entry_list_entry) if isinstance(entry_list_entry, list) else entry_list_entry
                for entry_index in range(field_index_list_len):
                    # field_type_count += 1
                    table_field_element_definition = u"\"BIGINT\", // %s" % re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE)
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
##############################################################################
# Generate the Field Type Definition/Field Description list element
# for the finance_analyzer project
##############################################################################
        if auto_gen_sql_element:
            table_field_interest_description_list.append(u"\n\n".encode(CMN.DEF.URL_ENCODING_UTF8))
# Field Type Definition
            table_field_interest_description_list.append(u"##### FIELD_TYPE_DEFINITION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"FinanceField_DATE, // 日期".encode(CMN.DEF.URL_ENCODING_UTF8))
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                entry_list_entry = cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
                field_index_list_len = len(entry_list_entry) if isinstance(entry_list_entry, list) else entry_list_entry
                for entry_index in range(field_index_list_len):
                    table_field_element_definition = u"FinanceField_LONG, // %s" % re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE)
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
# Field Description
            table_field_interest_description_list.append(u"##### FIELD_DESCRIPTION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"\"日期\", // FinanceField_DATE".encode(CMN.DEF.URL_ENCODING_UTF8))
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                entry_list_entry = cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
                field_index_list_len = len(entry_list_entry) if isinstance(entry_list_entry, list) else entry_list_entry
                for entry_index in range(field_index_list_len):
                    table_field_element_definition = u"\"%s\", // FinanceField_LONG" % re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE)
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
        CMN.FUNC.write_config_file_lines_ex(table_field_interest_description_list, interest_conf_filename, "wb")


    @classmethod
    def _show_statement_column_field_dimension_internal(cls, interest_conf_filename, auto_gen_sql_element):
        # field_count = len(cls.TABLE_FIELD_INTEREST_TITLE_LIST)
        # field_element_count = 0
        table_field_interest_description_list = []
        for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
            title_without_whitesapce = re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE) + u"[%d]" % title_index
            table_field_interest_description_list.append(title_without_whitesapce.encode(CMN.DEF.URL_ENCODING_UTF8))
        table_field_interest_description_list_len = len(table_field_interest_description_list)
# Column
        table_column_field_interest_description_list = []
        for column_title_index, column_title in enumerate(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST):
            column_title_without_whitesapce = re.sub(r"\s+", "", column_title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE) + u"[%d]" % column_title_index
            table_column_field_interest_description_list.append(column_title_without_whitesapce.encode(CMN.DEF.URL_ENCODING_UTF8))
        table_column_field_interest_description_list_len = len(table_column_field_interest_description_list)
        field_count_msg = "Field Count: %d, Column Field Count: %d, Field Element Count: %d" % (table_field_interest_description_list_len, table_column_field_interest_description_list_len, table_field_interest_description_list_len * table_column_field_interest_description_list_len)
        CMN.FUNC.try_print(field_count_msg)
        table_field_interest_description_list.insert(0, field_count_msg)
        table_field_interest_description_list.append(u"##### Column Field #####".encode(CMN.DEF.URL_ENCODING_UTF8))
        table_field_interest_description_list.extend(table_column_field_interest_description_list)

        # import pdb; pdb.set_trace()
##############################################################################
# Generate the Table Field Definition/Table Field Type Definition list element
# for the finance_recorder_java project
##############################################################################
        if auto_gen_sql_element:
            table_field_interest_description_list.append(u"\n\n".encode(CMN.DEF.URL_ENCODING_UTF8))
# Table Field Definition
            table_field_interest_description_list.append(u"##### TABLE_FIELD_DEFINITION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"\"date\", // 日期".encode(CMN.DEF.URL_ENCODING_UTF8))
            field_count = 0
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                for column_title_index, column_title in enumerate(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST):
                    field_count += 1
                    table_field_element_definition = u"\"value%d\", // %s:%s" % (field_count, re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE), re.sub(r"\s+", "", column_title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE))
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
# Table Field Type Definition
            table_field_interest_description_list.append(u"##### TABLE_FIELD_TYPE_DEFINITION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"\"DATE NOT NULL PRIMARY KEY\", // 日期".encode(CMN.DEF.URL_ENCODING_UTF8))
            # field_type_count = 0
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                for column_title_index, column_title in enumerate(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST):
                    # field_type_count += 1
                    table_field_element_definition = u"\"BIGINT\", // %s:%s" % (re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE), re.sub(r"\s+", "", column_title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE))
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
##############################################################################
# Generate the Field Type Definition/Field Description list element
# for the finance_analyzer project
##############################################################################
        if auto_gen_sql_element:
            table_field_interest_description_list.append(u"\n\n".encode(CMN.DEF.URL_ENCODING_UTF8))
# Field Type Definition
            table_field_interest_description_list.append(u"##### FIELD_TYPE_DEFINITION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"FinanceField_DATE, // 日期".encode(CMN.DEF.URL_ENCODING_UTF8))
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                for column_title_index, column_title in enumerate(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST):
                    table_field_element_definition = u"FinanceField_LONG, // %s:%s" % (re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE), re.sub(r"\s+", "", column_title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE))
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
# Field Description
            table_field_interest_description_list.append(u"##### FIELD_DESCRIPTION #####".encode(CMN.DEF.URL_ENCODING_UTF8))
            table_field_interest_description_list.append(u"\"日期\", // FinanceField_DATE".encode(CMN.DEF.URL_ENCODING_UTF8))
            for title_index, title in enumerate(cls.TABLE_FIELD_INTEREST_TITLE_LIST):
                for column_title_index, column_title in enumerate(cls.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST):
                    table_field_element_definition = u"\"%s:%s\", // FinanceField_LONG" % (re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE), re.sub(r"\s+", "", column_title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE))
                    table_field_interest_description_list.append(table_field_element_definition.encode(CMN.DEF.URL_ENCODING_UTF8))
        CMN.FUNC.write_config_file_lines_ex(table_field_interest_description_list, interest_conf_filename, "wb")


    def __init__(self, **kwargs):
        # import pdb;pdb.set_trace()
        super(ScrapyStatementBase, self).__init__(**kwargs)
        if not kwargs.get("renew_statement_field", False):
            if self.TABLE_FIELD_INTEREST_TITLE_LIST is None:
                raise ValueError("TABLE_FIELD_INTEREST_TITLE_LIST is None")
        self.cur_quarter_str = None


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
            try:
                # g_logger.debug(u"Check the statement field[%s] exist" % src_data)
                # cur_offset = (dst_statement_field_list[dst_index:]).index(src_data)
                # dst_index = dst_index + cur_offset + 1
                dst_index = dst_statement_field_list.index(src_data)
                dst_index += 1
            except ValueError:
# New element, insert the data into the list
                if dst_index == dst_statement_field_list_len:
                    dst_statement_field_list.append(src_data)
                else:
                    dst_statement_field_list.insert(dst_index, src_data)
                dst_index += 1
                dst_statement_field_list_len += 1
            # print "FUNC: %s" % dst_statement_field_list


    def update_statement_field(self, dst_statement_field_list, dst_statement_column_field_list=None):
        # import pdb; pdb.set_trace()
# Check if _parse_web_statement_column_field_data() is implemented before it's invoked
        if dst_statement_column_field_list is not None:
            if not hasattr(self, "_parse_web_statement_column_field_data"):
                raise AttributeError("_parse_web_statement_column_field_data() is NOT implemented")
# Update statement field from each company
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
# Limit the time range from the web site
                time_duration_after_lookup_time = self._adjust_time_range_from_web(self.SCRAPY_CLASS_INDEX, company_code_number)
                if time_duration_after_lookup_time is None:
                    self.csv_file_no_scrapy_record.add_time_range_not_overlap_record(self.SCRAPY_CLASS_INDEX, company_code_number)
                    continue
                g_logger.debug("Update statement field => [%s:%s] %s:%s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], company_code_number, time_duration_after_lookup_time.time_duration_start, time_duration_after_lookup_time.time_duration_end))
# Create the time slice iterator due to correct time range
                # import pdb; pdb.set_trace()
                company_statement_field_list = None
                company_statement_column_field_list = None
                timeslice_generator_cfg = {"company_code_number": company_code_number, "time_duration_start": time_duration_after_lookup_time.time_duration_start, "time_duration_end": time_duration_after_lookup_time.time_duration_end,}
                timeslice_iterable = self._get_timeslice_iterable(**timeslice_generator_cfg)      
                for timeslice in timeslice_iterable:
                    web_data = self._scrape_web_data(timeslice, company_code_number)
# Find the statement field
                    company_statement_field_list = self._parse_web_statement_field_data(web_data)
                    assert (company_statement_field_list is not None), "Fail to get company statement field from: %s" % url
# Find the statement column field
                    if dst_statement_column_field_list is not None:
                        company_statement_column_field_list = self._parse_web_statement_column_field_data(web_data)
                        assert (company_statement_column_field_list is not None), "Fail to get company statement column field from: %s" % url
# Update the new statement field
                    self._insert_not_exist_statement_element(dst_statement_field_list, company_statement_field_list)
                    if dst_statement_column_field_list is not None:
# Update the new statement column field
                        self._insert_not_exist_statement_element(dst_statement_column_field_list, company_statement_column_field_list)


    def _need_filter_scrape_company(self, company_code_number):
        return self._get_company_profile().is_company_etf(company_code_number)


    def _scrape_web_data_internal(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        self.cur_company_code_number = company_code_number
        self.cur_quarter_str = CMN.FUNC.transform_quarter_str(timeslice.year, timeslice.quarter)
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_statement_field_data_internal(self, web_data, web_data_start_index, web_data_end_index=None):
        # import pdb; pdb.set_trace()
        if web_data_end_index is None:
            web_data_end_index = len(web_data)
        data_list = []
# Filter the data which is NOT interested in
        for tr in web_data[web_data_start_index:web_data_end_index]:
        #     print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            # data_list.append(td[0].text.encode(CMN.DEF.URL_ENCODING_UTF8))
            data_list.append(td[0].text)
        if len(data_list) == 0:
            # import pdb;pdb.set_trace()
            raise CMN.EXCEPTION.WebScrapyServerBusyException(u"The field data[%s:%s] is EMPTY" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], self.cur_company_code_number))
        return data_list


    def _parse_web_statement_column_field_data_internal(self, web_data, web_column_data_start_index):
        data_list = []
# Filter the data which is NOT interested in
        td = web_data[web_column_data_start_index].select('td')
        td_len = len(td)
        for i in range(0, td_len):
            # print "%d: %s" % (i, td[i].text)
            data_list.append(td[i].text)
        if len(data_list) == 0:
            # import pdb;pdb.set_trace()
            raise CMN.EXCEPTION.WebScrapyServerBusyException(u"The column field data[%s:%s] is EMPTY" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], self.cur_company_code_number))
        return data_list


#     def _parse_web_data_internal(self, web_data, web_data_start_index, web_data_end_index=None):
#         web_data_len = len(web_data)
#         if web_data_len == 0:
#             return None
#         if web_data_end_index is None:
#             web_data_end_index = web_data_len
#         # import pdb; pdb.set_trace()
#         # data_list = []
#         table_column_field_index_list = None
#         table_column_field_index_mapping_list = None
#         if self.TABLE_COLUMN_FIELD_EXIST:
#             table_column_field_index_list = []
#             table_column_field_index_mapping_list = []
#             td = web_data[web_data_start_index].select('td')
#             td_len = len(td)
#             interest_index = 0
#             not_interest_index = 0
#             for i in range(0, td_len):
#                 data_found = False
#                 data_can_ignore = False
#                 data_index = None
#                 title = td[i].text.encode(CMN.DEF.URL_ENCODING_UTF8)
#                 # if interest_index < self.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST_LEN:
#                 #     try:
#                 #         # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
#                 #         data_index = cur_interest_index = (self.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST[interest_index:]).index(title) + interest_index
#                 #         interest_index = cur_interest_index + 1
#                 #         data_found = True
#                 #     except ValueError:
#                 #         pass
#                 try:
#                     # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
#                     data_index = self.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST.index(title)
#                     data_found = True
#                 except ValueError:
#                     pass
#                 if not data_found:
#                     # if not_interest_index < self.TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST_LEN:
#                     #     try:
#                     #         cur_not_interest_index = (self.TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST[not_interest_index:]).index(title) + not_interest_index
#                     #         not_interest_index = cur_not_interest_index + 1
#                     #         data_can_ignore = True
#                     #     except ValueError:
#                     #         pass
#                     try:
#                         self.TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST.index(title)
#                         data_can_ignore = True
#                     except ValueError:
#                         pass           
# # Check if the entry is NOT in the title list of interest
#                 if (not data_found) and (not data_can_ignore):
#                     # import pdb; pdb.set_trace()
#                     raise CMN.EXCEPTION.WebScrapyNotFoundException(u"The column title[%s] in company[%s] does NOT exist in the title list of interest" % (title.decode(CMN.DEF.URL_ENCODING_UTF8), self.cur_company_code_number))
#                 if data_can_ignore:
#                     continue
# # Parse the content of this entry, and the interested field into data structure
#                 table_column_field_index_list.append(i + 1)
#                 table_column_field_index_mapping_list.append(data_index)
#             self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT = collections.defaultdict(list) # Caution: Add the value in every row
# # Caution: Point to the first web data
#             web_data_start_index += 1
#         # import pdb; pdb.set_trace()
#         data_list = [self.cur_quarter_str,]
#         table_field_list = [None] * self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN
#         # interest_index = 0
#         # not_interest_index = 0
# # Filter the data which is NOT interested in
#         for index, tr in enumerate(web_data[web_data_start_index:web_data_end_index]):
#             # print "%d: %s" % (index, tr.text)
#             td = tr.select('td')
#             data_found = False
#             data_can_ignore = False
#             data_index = None
#             title = td[0].text.encode(CMN.DEF.URL_ENCODING_UTF8)
#             # import pdb; pdb.set_trace()
#             # if interest_index < self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN:
#             #     try:
#             #         # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
#             #         data_index = cur_interest_index = (self.TABLE_FIELD_INTEREST_TITLE_LIST[interest_index:]).index(title) + interest_index
#             #         interest_index = cur_interest_index + 1
#             #         data_found = True
#             #     except ValueError:
#             #         pass
#             try:
#                 # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
#                 data_index = self.TABLE_FIELD_INTEREST_TITLE_LIST.index(title)
#                 data_found = True
#             except ValueError:
#                 pass
#             if not data_found:
#                 # if not_interest_index < self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN:
#                 #     try:
#                 #         cur_not_interest_index = (self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST[not_interest_index:]).index(title) + not_interest_index
#                 #         not_interest_index = cur_not_interest_index + 1
#                 #         data_can_ignore = True
#                 #     except ValueError:
#                 #         pass
#                 try:
#                     self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST.index(title)
#                     data_can_ignore = True
#                 except ValueError:
#                     pass            
# # Check if the entry is NOT in the title list of interest
#             if (not data_found) and (not data_can_ignore):
#                 # import pdb; pdb.set_trace()
#                 raise CMN.EXCEPTION.WebScrapyNotFoundException(u"The title[%s] in company[%s] does NOT exist in the title list of interest" % (title.decode(CMN.DEF.URL_ENCODING_UTF8), self.cur_company_code_number))
#             if data_can_ignore:
#                 continue
# # Parse the content of this entry, and the interested field into data structure
#             entry_list_entry = table_column_field_index_list if self.TABLE_COLUMN_FIELD_EXIST else self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
#             # print "data_index: %d, data_title: [%s], table_title: [%s]" % (data_index, title, self.TABLE_FIELD_INTEREST_TITLE_LIST[data_index])
#             # import pdb;pdb.set_trace()
#             field_index_list = None
#             if isinstance(entry_list_entry, list):
#                 field_index_list = entry_list_entry
#             else:
#                 field_index_list = range(self.TABLE_FIELD_INTEREST_ENTRY_START_INDEX, self.TABLE_FIELD_INTEREST_ENTRY_START_INDEX + entry_list_entry)
#             table_field_list[data_index] = []
#             for field_index in field_index_list:
#                 # import pdb; pdb.set_trace()
#                 # print "data_index: %d, field_index: %d, data: [%s]" % (data_index, field_index, td[field_index].text)
#                 # field_value = str(td[field_index].text).strip(" ").replace(",", "")
#                 field_value = CMN.FUNC.remove_comma_in_string(str(td[field_index].text).strip(" "))
#                 if field_value.find('.') == -1: # Integer
#                     table_field_list[data_index].append(int(field_value))
#                 else: # Floating point
#                     table_field_list[data_index].append(float(field_value))
#         data_is_empty = True
#         for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
#             if table_field_list[index] is not None:
#                 data_is_empty = False
#                 break
#         if data_is_empty:
#             # import pdb;pdb.set_trace()
#             raise CMN.EXCEPTION.WebScrapyServerBusyException(u"The data[%s:%s] is EMPTY" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], self.cur_company_code_number))
# # Transforms the table into the 1-Dimension list
#         # import pdb;pdb.set_trace()
#         padding_entry = "0" 
#         if self.TABLE_COLUMN_FIELD_EXIST:
#             for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
#                 # entry_list_len = entry_list_entry = self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[self.TABLE_FIELD_INTEREST_TITLE_LIST[index]]
# # Padding in column
#                 data_entry_list = [padding_entry] * self.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST_LEN
#                 for column_field_key, column_field_value in enumerate(table_column_field_index_mapping_list):
#                     data_entry_list[column_field_value] = table_field_list[index][column_field_key]
#                 data_list.extend(data_entry_list)
#                 # print "index: %d, len: [%s]" % (index, len(data_list))
#         else:
#             for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
#                 if table_field_list[index] is None:
# # Padding
#                     entry_list_len = entry_list_entry = self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[self.TABLE_FIELD_INTEREST_TITLE_LIST[index]]
#                     # print "data_index: %d, title: [%s]" % (data_index, title)
#                     if isinstance(entry_list_entry, list):
#                         entry_list_len = len(entry_list_entry)
#                     data_list.extend([padding_entry] * entry_list_len)
#                 else:
#                     data_list.extend(table_field_list[index])
#                 # print "index: %d, len: [%s]" % (index, len(data_list))
#         # import pdb;pdb.set_trace()
#         return data_list
    def _parse_web_data_internal(self, web_data, web_data_start_index, web_data_end_index=None):
        if web_data_end_index is None:
            web_data_end_index = len(web_data)
        # import pdb; pdb.set_trace()
        table_column_field_index_list = None
        table_column_field_index_mapping_list = None
        if self.TABLE_COLUMN_FIELD_EXIST:
            table_column_field_index_list = []
            table_column_field_index_mapping_list = []
            td = web_data[web_data_start_index].select('td')
            td_len = len(td)
            interest_index = 0
            for i in range(0, td_len):
                data_index = None
                # title = td[i].text.encode(CMN.DEF.URL_ENCODING_UTF8)
                # title_without_whitesapce = re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE)
                title = re.sub(r"\s+", "", td[i].text, flags=re.UNICODE).encode(CMN.DEF.URL_ENCODING_UTF8)
                try:
                    # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
                    data_index = self.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST.index(title)
                except ValueError:
                    continue
# Parse the content of this entry, and the interested field into data structure
                table_column_field_index_list.append(i + 1)
                table_column_field_index_mapping_list.append(data_index)
            self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT = collections.defaultdict(list) # Caution: Add the value in every row
# Caution: Point to the first web data
            web_data_start_index += 1
        # import pdb; pdb.set_trace()
        data_list = [self.cur_quarter_str,]
        table_field_list = [None] * self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN
# Filter the data which is NOT interested in
        for index, tr in enumerate(web_data[web_data_start_index:web_data_end_index]):
            td = tr.select('td')
            data_index = None
            # title = td[0].text.encode(CMN.DEF.URL_ENCODING_UTF8)
            # title_without_whitesapce = re.sub(r"\s+", "", title.decode(CMN.DEF.URL_ENCODING_UTF8), flags=re.UNICODE)
            title = re.sub(r"\s+", "", td[0].text, flags=re.UNICODE).encode(CMN.DEF.URL_ENCODING_UTF8)
            # print "%d: %s" % (index, title)
            try:
                # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
                data_index = self.TABLE_FIELD_INTEREST_TITLE_LIST.index(title)
            except ValueError:
# Check if this title is alias
                if self.TABLE_FIELD_INTEREST_ALIAS_TITLE_DICT.has_key(title):
                    title_alias = self.TABLE_FIELD_INTEREST_ALIAS_TITLE_DICT[title]
                    try:
                        # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
                        data_index = self.TABLE_FIELD_INTEREST_TITLE_LIST.index(title_alias)
                    except ValueError:
                        raise CMN.EXCEPTION.WebScrapyNotFoundException(u"The title[%s, Alias: %s] is NOT found in the interest title list" % (title_alias, title))                  
                else:
                    continue
# Parse the content of this entry, and the interested field into data structure
            entry_list_entry = table_column_field_index_list if self.TABLE_COLUMN_FIELD_EXIST else self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[title]
            # print "data_index: %d, data_title: [%s], table_title: [%s]" % (data_index, title, self.TABLE_FIELD_INTEREST_TITLE_LIST[data_index])
            # import pdb;pdb.set_trace()
            field_index_list = None
            if isinstance(entry_list_entry, list):
                field_index_list = entry_list_entry
            else:
                field_index_list = range(self.TABLE_FIELD_INTEREST_ENTRY_START_INDEX, self.TABLE_FIELD_INTEREST_ENTRY_START_INDEX + entry_list_entry)
            table_field_list[data_index] = []
            for field_index in field_index_list:
                # import pdb; pdb.set_trace()
                # print "data_index: %d, field_index: %d, data: [%s]" % (data_index, field_index, td[field_index].text)
                # field_value = str(td[field_index].text).strip(" ").replace(",", "")
                field_value = CMN.FUNC.remove_comma_in_string(str(td[field_index].text).strip(" "))
                try:
                    if field_value.find('.') == -1: # Integer
                        table_field_list[data_index].append(int(field_value))
                    else: # Floating point
                        table_field_list[data_index].append(float(field_value))
                except:
                    raise CMN.EXCEPTION.WebScrapyIncorrectFormatException("Unknown data type: %s" % field_value)
# Check if there is not any statement data.......
        data_is_empty = True
        for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
            if table_field_list[index] is not None:
                data_is_empty = False
                break
        if data_is_empty:
            # import pdb;pdb.set_trace()
            raise CMN.EXCEPTION.WebScrapyServerBusyException(u"The data[%s:%s] is EMPTY" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[self.SCRAPY_CLASS_INDEX], self.cur_company_code_number))
# Transforms the table into the 1-Dimension list
        # import pdb;pdb.set_trace()
        padding_entry = "0" 
        if self.TABLE_COLUMN_FIELD_EXIST:
            for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
                # entry_list_len = entry_list_entry = self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[self.TABLE_FIELD_INTEREST_TITLE_LIST[index]]
# Padding in column
                data_entry_list = [padding_entry] * self.TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST_LEN
                for column_field_key, column_field_value in enumerate(table_column_field_index_mapping_list):
                    data_entry_list[column_field_value] = table_field_list[index][column_field_key]
                data_list.extend(data_entry_list)
                # print "index: %d, len: [%s]" % (index, len(data_list))
        else:
            for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
                if table_field_list[index] is None:
# No data found. Padding
                    entry_list_len = entry_list_entry = self.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[self.TABLE_FIELD_INTEREST_TITLE_LIST[index]]
                    # print "data_index: %d, title: [%s]" % (data_index, title)
                    if isinstance(entry_list_entry, list):
                        entry_list_len = len(entry_list_entry)
                    data_list.extend([padding_entry] * entry_list_len)
                else:
# Update data.
                    data_list.extend(table_field_list[index])
                # print "index: %d, len: [%s]" % (index, len(data_list))
        return data_list


    @abstractmethod
    def _parse_web_statement_field_data(self, web_data):
        pass


    @abstractmethod
    def _parse_web_data(self, web_data):
        pass
