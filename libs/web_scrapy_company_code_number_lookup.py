# -*- coding: utf8 -*-

import os
import sys
import re
import requests
import csv
import shutil
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


COMPANY_INFO_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER = 0
COMPANY_INFO_ENTRY_FIELD_INDEX_COMPANY_NAME = 1
COMPANY_INFO_ENTRY_FIELD_INDEX_INDUSTRY = 5

COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT = u"00"
COMPANY_GROUP_TDR_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT = u"91"
COMPANY_GROUP_EXCEPTION_DICT = {
    COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT: "ETF",
    COMPANY_GROUP_TDR_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT: "TDR",
}

COMPANY_GROUP_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT = 0
COMPANY_GROUP_INDUSTRY = 1
COMPANY_GROUP_METHOD_DESCRIPTION_LIST = [
    "Group by company code number first 2 digits",
    "Group by industry",
]

@CMN_CLS.Singleton
class WebScrapyCompanyCodeNumberLookup(object):

    def __init__(self):
        self.COMPANY_CODE_NUMBER_INFO_ELEMENT_LEN = 7
        self.UNICODE_ENCODING_IN_FILE = "utf-8"
        self.url_format = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=%d"
        self.encoding = "big5"
        self.select_flag = "table tr"

        self.company_code_number_info_list = []
        self.company_code_number_info_dict = {}

# A lookup table used when failing to parse company number and name
        self.failed_company_name_lookup = {
            "8349": u"恒耀",
        }
        self.group_company_func_ptr = [
            self.__group_company_by_company_code_number_first_2_digit,
            self.__group_company_by_industry,
        ]


    def initialize(self):
        # import pdb; pdb.set_trace()
        self.update_company_code_number_info(False)


    def update_company_code_number_info(self, need_update_from_web=True):
# Update data from the file
        if not need_update_from_web:
            need_update_from_web = self.__update_company_code_number_info_from_file()
# It's required to update the new data
        if need_update_from_web:
# Update data from the web
            self.__update_company_code_number_info_from_web()
# Write the result into the config file
            self.__write_company_code_number_info_to_file()
# Copy the config file to the finance_analyzer/finance_recorder_java project
            self.__copy_company_code_number_info_config_file()


    def __copy_company_code_number_info_config_file(self):
        current_path = os.path.dirname(os.path.realpath(__file__))
        [working_folder, project_name, lib_folder] = current_path.rsplit('/', 2)
        dst_folderpath_list = [
            "%s/%s/%s" % (working_folder, CMN.DEF_COPY_CONF_FILE_DST_PROJECT_NAME1, CMN.DEF_CONF_FOLDER),
            "%s/%s/%s" % (working_folder, CMN.DEF_COPY_CONF_FILE_DST_PROJECT_NAME2, CMN.DEF_CONF_FOLDER),
        ]
        src_filepath = "%s/%s/%s/%s" % (working_folder, project_name, CMN.DEF_CONF_FOLDER, CMN.DEF_WORKDAY_CANLENDAR_CONF_FILENAME)
        for dst_folderpath in dst_folderpath_list:
            if os.path.exists(dst_folderpath):
                g_logger.debug("Copy the file[%s] to %s" % (CMN.DEF_COMPANY_CODE_NUMBER_CONF_FILENAME, dst_folderpath))
                shutil.copy2(src_filepath, dst_folderpath)


    def __update_company_code_number_info_from_file(self):
        # import pdb; pdb.set_trace()
        need_update_from_web = False
        current_path = os.path.dirname(os.path.realpath(__file__))
        [project_folder, lib_folder] = current_path.rsplit('/', 1)
        conf_filepath = "%s/%s/%s" % (project_folder, CMN.DEF_CONF_FOLDER, CMN.DEF_COMPANY_CODE_NUMBER_CONF_FILENAME)
        g_logger.debug("Try to Acquire the Company Code Number data from the file: %s......" % conf_filepath)
        if not os.path.exists(conf_filepath):
            g_logger.warn("The Company Code Number config file does NOT exist")
            need_update_from_web = True
        else:
            try:
                date_range_str = None
                with open(conf_filepath, 'rb') as fp:
                    for line in fp:
                        line_unicode = line.rstrip("\n").decode(self.UNICODE_ENCODING_IN_FILE)
                        element_list = re.split(r",", line_unicode, re.U)
                        if len(element_list) != self.COMPANY_CODE_NUMBER_INFO_ELEMENT_LEN:
                            raise ValueError("The Company Code Number length[%d] should be %d", len(element_list), self.COMPANY_CODE_NUMBER_INFO_ELEMENT_LEN)
                        self.company_code_number_info_list.append(element_list)
                        self.company_code_number_info_dict[element_list[0]] = element_list
            except Exception as e:
                g_logger.error("Error occur while parsing Company Code Number info, due to %s" % str(e))
                raise e

        return need_update_from_web


    def __update_company_code_number_info_from_web(self):
        # import pdb; pdb.set_trace()
        g_logger.debug("Try to Acquire the Company Code Number info from the web......")
        g_logger.debug("###### Get the Code Number of the Stock Exchange Company ######")
        self.__scrap_company_code_number_from_web(CMN.MARKET_TYPE_STOCK_EXCHANGE)
        g_logger.debug("###### Get the Code Number of the Over-the-Counter Company ######")
        self.__scrap_company_code_number_from_web(CMN.MARKET_TYPE_OVER_THE_COUNTER)


    def __scrap_company_code_number_info_from_web(self, market_type):
        # import pdb; pdb.set_trace()
        str_mode = None
        if market_type == CMN.MARKET_TYPE_STOCK_EXCHANGE:
            str_mode = 2
        elif market_type == CMN.MARKET_TYPE_OVER_THE_COUNTER:
            str_mode = 4
        else:
            raise ValueError("Unknown Market Type: %d", self.market_type)
# Assemble the URL
        url = self.url_format % str_mode

# Scrap the web data
        try:
            # g_logger.debug("Try to Scrap data [%s]" % url)
            res = requests.get(url, timeout=CMN.DEF_SCRAPY_WAIT_TIMEOUT)
        except requests.exceptions.Timeout as e:
            # g_logger.debug("Try to Scrap data [%s]... Timeout" % url)
            fail_to_scrap = False
            for index in range(self.SCRAPY_RETRY_TIMES):
                time.sleep(randint(1,3))
                try:
                    res = requests.get(url, timeout=CMN.DEF_SCRAPY_WAIT_TIMEOUT)
                except requests.exceptions.Timeout as ex:
                    fail_to_scrap = True
                if not fail_to_scrap:
                    break
            if fail_to_scrap:
                g_logger.error("Fail to scrap company code number info even retry for %d times !!!!!!" % self.SCRAPY_RETRY_TIMES)
                raise e
# Select the section we are interested in
        res.encoding = self.encoding
        # print res.text
        soup = BeautifulSoup(res.text)
        web_data = soup.select(self.select_flag)
        if len(web_data) == 0:
            raise RuntimeError("Fail to find the compay code number info")

# Caution: handle the data based on Unicode
        for tr in web_data[2:]:
            td = tr.select('td')
            if len(td) != self.COMPANY_CODE_NUMBER_INFO_ELEMENT_LEN:
                continue
# The Regular Expression Template ([\w-]+) is used for the F-XX company name
            mobj = re.match(r"(\w+)\s+([\w-]+)", td[0].text, re.U)
            failed_case = False
            if mobj is None:
                # import pdb; pdb.set_trace()
                g_logger.warn(u"Error! Fail to parse: %s, try another way......" % td[0].text)
                mobj = re.match(r"([\d]{4,})", td[0].text, re.U)
                if mobj is None:
                    raise ValueError(u"Unknown data format: %s" % td[0].text)
                failed_case = True
                # res_list = re.split(r"\s+", td[0].text, re.U)

# Filter the data which are NOT interested in
            # company_number = str(mobj.group(1))
            # if not re.match("^[\d][\d]{2}[\d]$", company_number):
            company_number = mobj.group(1)
            if not re.match(r"^[\d][\d]{2}[\d]$", company_number, re.U):
                continue
            if failed_case:
                company_name = self.failed_company_name_lookup.get(company_number, None)
                if company_name is None:
                    raise RuntimeError("Fail to find the company name of company number: %s", company_number)
            else:
                company_name = mobj.group(2)
            # print "number: %s, name: %s" % (company_number, company_name)
            element_list = []
            element_list.append(company_number)
            element_list.append(company_name)
            for i in range(1, 6): 
                try:
                    # new_element = CMN.to_str(td[i].text, self.encoding)
                    # element_list.append(new_element)
                    element_list.append(td[i].text)
                except Exception as e:
                    g_logger.error("Fail to transform unicode[%s] to str: %s, due to: %s" % (self.encoding, td[i].text, str(e)))
                    raise e
            self.company_code_number_info_list.append(element_list)
# 有價證券代號及名稱
# 國際證券辨識號碼(ISIN Code) 
# 上市日 
# 市場別 
# 產業別 
# CFICode 


    def __write_company_code_number_info_to_file(self):
        # import pdb; pdb.set_trace()
        current_path = os.path.dirname(os.path.realpath(__file__))
        [project_folder, lib_folder] = current_path.rsplit('/', 1)
        conf_filepath = "%s/%s/%s" % (project_folder, CMN.DEF_CONF_FOLDER, CMN.DEF_COMPANY_CODE_NUMBER_CONF_FILENAME)
        g_logger.debug("Write the Company Code Number info to the file: %s......" % conf_filepath)
        with open(conf_filepath, 'wb') as fp:
            try:
                for company_code_number_info in self.company_code_number_info_list:
                    self.company_code_number_info_dict[company_code_number_info[0]] = company_code_number
                    # if company_code_number[0] == u"0050":
                    #     import pdb; pdb.set_trace()
                    company_code_number_info_unicode = u",".join(company_code_number_info)
                    # g_logger.debug(u"Company Code Number Data: %s", company_code_number_unicode)
# Can be readable for the CSV reader by encoding utf-8 unicode
                    fp.write(company_code_number_info_unicode.encode(self.UNICODE_ENCODING_IN_FILE) + "\n") 
 
            except Exception as e:
                g_logger.error(u"Error occur while writing Company Code Number[%s] info into config file, due to %s" % (company_code_number_unicode, str(e)))
                raise e


    def lookup_company_info(self, company_number):
        company_number_unicode = CMN.to_unicode(company_number, self.UNICODE_ENCODING_IN_FILE)
        company_info = self.company_code_number_info_dict.get(company_number_unicode, None)
        if company_info is None:
            raise ValueError("Fail to find the company info of company number: %s", company_number)
        return company_info


    def __get_exceptional_company_industry_by_company_code_number_first_2_digit(self, company_code_number):
        # import pdb; pdb.set_trace()
        for key, value in COMPANY_GROUP_EXCEPTION_DICT.items():
            pattern = r"%s" % str(key)
            if re.match(pattern, company_code_number, re.U):
                return value
        raise ValueError(u"Unknown exceptional company group: %s" % company_code_number)


    def __group_company_by_company_code_number_first_2_digit(self):
        group_dict = {}
        for company_code_number_info in self.company_code_number_info_list:
            company_code_number = str(company_code_number_info[0])
            company_code_number_first_2_digit = company_code_number[0:2]
            if group_dict.get(company_code_number_first_2_digit, None) is None:
                group_dict[company_code_number_first_2_digit] = []
            group_dict[company_code_number_first_2_digit].append(company_code_number)
        # for group_key, group_value in group_dict.items():
        #     print "Group: %s, Len: %d; %s" % (group_key, len(group_value), ",".join(group_value))
        for group_key in sorted(group_dict):
            print "Group: %s, Len: %d; %s" % (group_key, len(group_dict[group_key]), ",".join(group_dict[group_key]))
        print "There are totally %d groups" % len(group_dict.keys())

    def __group_company_by_industry(self):
        group_dict = {}
        for company_code_number_info in self.company_code_number_info_list:
            company_industry = None
            if company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_INDUSTRY] == u"":
                company_industry = self.__get_exceptional_company_industry_by_company_code_number_first_2_digit(company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
            else:
                company_industry = company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_INDUSTRY]
            if group_dict.get(company_industry, None) is None:
                group_dict[company_industry] = []

            if company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_INDUSTRY] == u"":
                # print company_code_number_info
                # import pdb; pdb.set_trace()
                company_industry = self.__get_exceptional_company_industry_by_company_code_number_first_2_digit(company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
                # print company_industry
            else:
                company_industry = company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_INDUSTRY]       
            group_dict[company_industry].append(company_code_number_info[COMPANY_INFO_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
        # for group_key, group_value in group_dict.items():
        #     print "Group: %s, Len: %d; %s" % (group_key, len(group_value), ",".join(group_value))
        for group_key in sorted(group_dict):
            print "Group: %s, Len: %d; %s" % (group_key, len(group_dict[group_key]), ",".join(group_dict[group_key]))
        print "There are totally %d groups" % len(group_dict.keys())

    def group_company(self, method_number):
        (self.group_company_func_ptr[method_number])()