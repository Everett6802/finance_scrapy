# -*- coding: utf8 -*-

import os
import sys
import re
import requests
import csv
import shutil
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import timeslice_generator as TimesliceGenerator
import scrapy.common as CMN
from scrapy.common.common_variable import GlobalVar as GV
# import libs.base as BASE
g_logger = CMN.LOG.get_logger()


COMPANY_MARKET_TYPE_STOCK_EXCHANGE = u"上市"
COMPANY_MARKET_TYPE_OVER_THE_COUNTER = u"上櫃"
COMPANY_MARKET_TYPE_DICT = {
    COMPANY_MARKET_TYPE_STOCK_EXCHANGE: CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE,
    COMPANY_MARKET_TYPE_OVER_THE_COUNTER: CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER,
}

COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER = 0
COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_NAME = 1
COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE = 3
COMPANY_PROFILE_ENTRY_FIELD_INDEX_MARKET_TYPE = 4
COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY = 5
COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NAME = 7
COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NUMBER = 8

COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT = u"00"
COMPANY_GROUP_TDR_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT = u"91"
COMPANY_GROUP_EXCEPTION_DICT = {
    COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT: "ETF",
    COMPANY_GROUP_TDR_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT: "TDR",
}
ETF_COMPANY_CODE_NUMBER_PATTERN = r"%s[\d]{2}" % COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT
TDR_COMPANY_CODE_NUMBER_PATTERN = r"%s[\d]{2}" % COMPANY_GROUP_TDR_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT
# FILTERED_COMPANY_CODE_NUMBER_PATTERN = "%s|%s" % (ETF_COMPANY_CODE_NUMBER_PATTERN, TDR_COMPANY_CODE_NUMBER_PATTERN)
FILTERED_COMPANY_CODE_NUMBER_PATTERN = "|".join([TDR_COMPANY_CODE_NUMBER_PATTERN,])

COMPANY_GROUP_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT = 0
COMPANY_GROUP_INDUSTRY = 1
COMPANY_GROUP_INDUSTRY_AND_MARKET = 2
COMPANY_GROUP_METHOD_DESCRIPTION_LIST = [
    "Group by company code number first 2 digits",
    "Group by industry",
    "Group by industry and market",
]
LARGE_INDUSTRY_COMPANY_GROUP_LIST = [u"光電業", u"半導體業", u"電子零組件業", u"電腦及週邊設備業",]


@CMN.CLS.Singleton
class CompanyProfile(object):

    def __init__(self):
        # import pdb; pdb.set_trace()
        self.COMPANY_PROFILE_ELEMENT_LEN = 7
        self.COMPANY_PROFILE_ELEMENT_EX_LEN = self.COMPANY_PROFILE_ELEMENT_LEN + 2
        self.UNICODE_ENCODING_IN_FILE = CMN.DEF.UNICODE_ENCODING_IN_FILE
        self.url_format = "http://isin.twse.com.tw/isin/C_public.jsp?strMode=%d"
        self.encoding = "big5"
        self.select_flag = "table tr"

        self.company_profile_dict = None
        self.company_profile_list = None
        self.company_group_profile_list = None;
        self.company_group_num2name_list = None
        self.company_group_name2num_dict = None
        self.update_from_web = False
        self.__company_group_size = 0
        self.__company_amount = 0

        self.timeslice_generator = None

# A lookup table used when failing to parse company number and name
        self.failed_company_name_lookup = {
            "8349": u"恒耀",
        }
# A lookup table used when the comapny does NOT exist actually
        self.failed_company_name_lookup = {
            "8349": u"恒耀",
        }
        self.group_company_func_ptr = [
            self.__group_company_by_company_code_number_first_2_digit,
            self.__group_company_by_industry,
            self.__group_company_by_industry_and_market,
        ]
# A lookup table used when the comapny profile is reqiured to be modified
        self.modified_company_lookup = {
            "3709": {COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE: "2017-09-01"},
        }
        self.modified_company_number_list = self.modified_company_lookup.keys()


    def initialize(self):
        # import pdb; pdb.set_trace()
        self.__update_company_profile(False, False)


    def __get_timeslice_generator(self):
        if self.timeslice_generator is None:
            self.timeslice_generator = TimesliceGenerator.TimeSliceGenerator.Instance()
        return self.timeslice_generator


    def __cleanup_company_profile_data_structure(self):
        self.company_group_num2name_list = []
        self.company_group_name2num_dict = {}
        self.company_group_profile_list = []
        self.company_profile_list = []
        self.company_profile_dict = {}
        self.__company_amount = 0
        self.__company_group_size = 0


    def renew_table(self, need_check_company_diff=True):
        # import pdb; pdb.set_trace()
        if not self.update_from_web:
            self.__update_company_profile(True, need_check_company_diff)
        else:
            g_logger.info("The lookup table has already been the latest version !!!")


    def __update_company_profile(self, need_update_from_web=True, need_check_company_diff=True):
        # import pdb; pdb.set_trace()
# Update data from the file
        if not need_update_from_web:
            need_update_from_web = self.__update_company_profile_from_file()
            if need_check_company_diff and need_update_from_web:
                g_logger.warn("Fail to find the older config from the file[%s]. No need to compare the difference" % CMN.DEF.COMPANY_PROFILE_CONF_FILENAME)
                need_check_company_diff = False
# It's required to update the new data
        if need_update_from_web:
            old_company_profile_list = None
# Keep track the older company code number info if necessary
            if need_check_company_diff:
                # import pdb; pdb.set_trace()
                old_company_profile_list = self.company_profile_list
# Update data from the web
            self.__update_company_profile_from_web()
            cur_timestamp_str = CMN.FUNC.generate_cur_timestamp_str()
# Compare the difference of company code number info
            if need_check_company_diff:
                new_company_profile_list = self.company_profile_list
                (is_company_change, old_lost_list, new_added_list) = self.__diff_company_profile(old_company_profile_list, new_company_profile_list)
# Keep track of the chnage of the company profile
                if is_company_change:
                    self.__write_company_profile_change_list_to_file(old_lost_list, new_added_list, cur_timestamp_str)
            else:
                self.__write_company_profile_change_list_to_file(None, None, cur_timestamp_str)
# Write the result into the config file
            self.__write_company_profile_to_file(cur_timestamp_str)
# Copy the config file to the finance_analyzer/finance_recorder project
            # self.__copy_company_profile_config_file()
            self.update_from_web = True


    # def __copy_company_profile_config_file(self):
    #     # import pdb; pdb.set_trace()
    #     # current_path = os.path.dirname(os.path.realpath(__file__))
    #     [working_folder, project_name] = GV.PROJECT_FOLDERPATH.rsplit('/', 1)
    #     dst_folderpath_list = [
    #         "%s/%s/%s" % (working_folder, CMN.DEF.FINANCE_RECORDER_PROJECT_NAME, CMN.DEF.CONF_FOLDERNAME),
    #         "%s/%s/%s" % (working_folder, CMN.DEF.FINANCE_ANALYZER_PROJECT_NAME, CMN.DEF.CONF_FOLDERNAME),
    #     ]
    #     company_profile_src_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, CMN.DEF.CONF_FOLDERNAME, CMN.DEF.COMPANY_PROFILE_CONF_FILENAME)
    #     company_group_src_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, CMN.DEF.CONF_FOLDERNAME, CMN.DEF.COMPANY_GROUP_CONF_FILENAME)
    #     for dst_folderpath in dst_folderpath_list:
    #         # if os.path.exists(dst_folderpath):
    #         #     g_logger.debug("Copy the file[%s] to %s" % (CMN.DEF.COMPANY_PROFILE_CONF_FILENAME, dst_folderpath))
    #         #     shutil.copy2(src_filepath, dst_folderpath)
    #         CMN.FUNC.copy_file_if_exist(company_profile_src_filepath, dst_folderpath)
    #         CMN.FUNC.copy_file_if_exist(company_group_src_filepath, dst_folderpath)


    def __update_company_profile_from_file(self):
        # import pdb; pdb.set_trace()
        self.__cleanup_company_profile_data_structure()
        need_update_from_web = False
        # current_path = os.path.dirname(os.path.realpath(__file__))
        # [project_folder, lib_folder] = current_path.rsplit('/', 1)
        conf_filepath = "%s/%s" % (GV.FINANCE_DATASET_CONF_FOLDERPATH, CMN.DEF.COMPANY_PROFILE_CONF_FILENAME)
        g_logger.debug("Try to Acquire the Company Code Number data from the file: %s......" % conf_filepath)
        if not os.path.exists(conf_filepath):
            g_logger.warn("The Company Code Number config file does NOT exist")
            need_update_from_web = True
        else:
            # try:
            #     with open(conf_filepath, 'rb') as fp:
            #         for line in fp:
            #             line_unicode = line.rstrip("\n").decode(self.UNICODE_ENCODING_IN_FILE)
            #             element_list = re.split(r",", line_unicode, re.U)
            #             if len(element_list) != self.COMPANY_PROFILE_ELEMENT_EX_LEN:
            #                 raise ValueError("The Company Code Number length[%d] should be %d" % (len(element_list), self.COMPANY_PROFILE_ELEMENT_EX_LEN))
            #             self.company_profile_dict[element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]] = element_list
            #             self.company_profile_list.append(element_list)
            #             company_group_name = element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NAME]
            #             if self.company_group_name2num_dict.get(company_group_name, None) is None:
            #                 self.company_group_name2num_dict[company_group_name] = len(self.company_group_num2name_list)
            #                 self.company_group_num2name_list.append(company_group_name)
            # except Exception as e:
            #     g_logger.error("Error occur while parsing Company Code Number info, due to %s" % str(e))
            #     raise e
            # else:
            #     self.__company_group_size = len(self.company_group_num2name_list)
            #     self.__company_amount = len(self.company_profile_list)
            #     self.__generate_company_group_profile_list()
            line_unicode_list = CMN.FUNC.unicode_read_config_file_lines(CMN.DEF.COMPANY_PROFILE_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)
            for line_unicode in line_unicode_list:
                element_list = re.split(r",", line_unicode, re.U)
                if len(element_list) != self.COMPANY_PROFILE_ELEMENT_EX_LEN:
                    raise ValueError("The Company Code Number length[%d] should be %d" % (len(element_list), self.COMPANY_PROFILE_ELEMENT_EX_LEN))
                self.company_profile_dict[element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]] = element_list
                self.company_profile_list.append(element_list)
                company_group_name = element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NAME]
                if self.company_group_name2num_dict.get(company_group_name, None) is None:
                    self.company_group_name2num_dict[company_group_name] = len(self.company_group_num2name_list)
                    self.company_group_num2name_list.append(company_group_name)
                self.__company_group_size = len(self.company_group_num2name_list)
                self.__company_amount = len(self.company_profile_list)
                self.__generate_company_group_profile_list()
        return need_update_from_web


    def __update_company_profile_from_web(self):
        # import pdb; pdb.set_trace()
        self.__cleanup_company_profile_data_structure()
        g_logger.debug("Try to Acquire the Company Code Number info from the web......")
        time_start_second = int(time.time())
        g_logger.debug("###### Get the Code Number of the Stock Exchange Company ######")
        self.__scrape_company_profile_from_web(CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE)
        g_logger.debug("###### Get the Code Number of the Over-the-Counter Company ######")
        self.__scrape_company_profile_from_web(CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER)
        self.__company_group_size = len(self.company_group_num2name_list)
        self.__company_amount = len(self.company_profile_list)
        self.__generate_company_group_profile_list()
        time_end_second = int(time.time())
        g_logger.info("######### Time Lapse: %d second(s) #########" % (time_end_second - time_start_second))


    def __diff_company_profile(self, old_company_profile_list, new_company_profile_list):
        old_company_code_number_list = [int(old_company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]) for old_company_profile in old_company_profile_list]
        new_company_code_number_list = [int(new_company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]) for new_company_profile in new_company_profile_list]
        # import pdb; pdb.set_trace()
        old_company_code_number_list.sort()
        new_company_code_number_list.sort()
        old_company_code_number_list_len = len(old_company_code_number_list)
        new_company_code_number_list_len = len(new_company_code_number_list)
        old_index = 0
        new_index = 0
        old_lost_list = []
        new_added_list = []
        while old_index != old_company_code_number_list_len and new_index != new_company_code_number_list_len:
            if old_company_code_number_list[old_index] == new_company_code_number_list[new_index]:
                old_index += 1
                new_index += 1
            elif old_company_code_number_list[old_index] > new_company_code_number_list[new_index]:
                new_added_list.append(str(new_company_code_number_list[new_index]))
                new_index += 1
            else:
                old_lost_list.append(str(old_company_code_number_list[old_index]))
                old_index += 1
        if old_index < old_company_code_number_list_len:
            while old_index != old_company_code_number_list_len:
                old_lost_list.append(str(old_company_code_number_list[old_index]))
                old_index += 1
        elif new_index < new_company_code_number_list_len:
            while new_index != new_company_code_number_list_len:
                new_added_list.append(str(new_company_code_number_list[old_index]))
                new_index += 1
        assert (old_index == old_company_code_number_list_len), "old_index[%d] is NOT equal to old_company_code_number_list_len[%d]" % (old_index, old_company_code_number_list_len)
        assert (new_index == new_company_code_number_list_len), "new_index[%d] is NOT equal to new_company_code_number_list_len[%d]" % (new_index, new_company_code_number_list_len)
        is_company_change = False
        if len(old_lost_list) != 0:
            is_company_change = True
            res_str = "Some old company lost:"
            for old_lost in old_lost_list:
                res_str += (" %s" % old_lost)
            print res_str
        if len(new_added_list) != 0:
            is_company_change = True
            res_str = "Some new company added:"
            for new_added in new_added_list:
                res_str += (" %s" % new_added)
            print res_str
        return (is_company_change, old_lost_list, new_added_list)


    def __scrape_company_profile_from_web(self, market_type):
        # import pdb; pdb.set_trace()
        def get_company_group_name(company_profile):
            company_group_name = None
            if company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] == u"":
                company_group_name = self.__get_exceptional_company_industry_by_company_code_number_first_2_digit(company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
            else:
                company_group_name = company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY]
                if company_group_name in LARGE_INDUSTRY_COMPANY_GROUP_LIST:
                    company_group_name = u"%s-%s" % (company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY], company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_MARKET_TYPE])
            return company_group_name

        str_mode = None
        if market_type == CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE:
            str_mode = 2
        elif market_type == CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER:
            str_mode = 4
        else:
            raise ValueError("Unknown Market Type: %d", self.market_type)
# Assemble the URL
        url = self.url_format % str_mode
# Scrap the web data
        req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
# Select the section we are interested in
        req.encoding = self.encoding
        # print res.text
        soup = BeautifulSoup(req.text)
        web_data = soup.select(self.select_flag)
        if len(web_data) == 0:
            raise RuntimeError("Fail to find the compay code number info")

        company_group_index = 0
# Caution: handle the data based on Unicode
        for tr in web_data[2:]:
            td = tr.select('td')
            if len(td) != self.COMPANY_PROFILE_ELEMENT_LEN:
                continue
# The Regular Expression Template ([\w-]+) is used for the F-XX company name
            mobj = re.match(r"(\w+)\s+([\w-]+)", td[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER].text, re.U)
            failed_case = False
            if mobj is None:
                # import pdb; pdb.set_trace()
                g_logger.warn(u"Error! Fail to parse: %s, try another way......" % td[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER].text)
                mobj = re.match(r"([\d]{4,})", td[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER].text, re.U)
                if mobj is None:
                    raise ValueError(u"Unknown data format: %s" % td[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER].text)
                failed_case = True
                # res_list = re.split(r"\s+", td[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER].text, re.U)

# Filter the data which are NOT interested in
            # company_number = str(mobj.group(1))
            # if not re.match("^[\d][\d]{2}[\d]$", company_number):
            company_number = mobj.group(1)
            if not re.match(r"^[\d][\d]{2}[\d]$", company_number, re.U):
                continue
# Filter TDR
            if re.match(FILTERED_COMPANY_CODE_NUMBER_PATTERN, company_number, re.U):
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
#                     if i == COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE:
#                         assert (re.match(r"[\d]{4}/[\d]{2}/[\d]{2}", td[i].text, re.U)), u"Incorrect date format: %s" % td[i].text
# # Transform into the standard date format
#                         element_list.append(td[i].text.replace("/", "-"))
#                     elif i == COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY:
#                         if re.match(self.ETF_COMPANY_CODE_NUMBER_PATTERN, element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER], re.U):
#                             import pdb; pdb.set_trace()
#                             assert (td[i].text == u""), u"The company[%s] Industry is NOT Empty: %s" % (element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER], td[i].text)
#                             element_list.append(COMPANY_GROUP_EXCEPTION_DICT[COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT])
#                         else:
#                             element_list.append(td[i].text)
#                     else:
                    element_list.append(td[i].text)
                except Exception as e:
                    g_logger.error(u"Fail to transform unicode[%s] to str: %s, due to: %s" % (self.encoding, td[i].text, str(e)))
                    raise e
# Add the group info into the entry
            company_group_name = get_company_group_name(element_list)
            if self.company_group_name2num_dict.get(company_group_name, None) is None:
                self.company_group_name2num_dict[company_group_name] = len(self.company_group_num2name_list)
                self.company_group_num2name_list.append(company_group_name)
            element_list.append(company_group_name)
            element_list.append(u"%d" % self.company_group_name2num_dict[company_group_name])

# Modify the some element content slightly
# Modify the date to the standard format
            assert (re.match(r"[\d]{4}/[\d]{2}/[\d]{2}", element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE], re.U)), u"Incorrect date format: %s" % element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE]
            element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE] = element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE].replace("/", "-")
# Setup the TDR and ETF industry name
            mobj = re.match(r"([\d]{2})[\d]{2}", element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER], re.U)
            assert (mobj is not None), u"Unknow company code number format: %s" % element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]
            if COMPANY_GROUP_EXCEPTION_DICT.get(mobj.group(1), None) is not None:
                assert (element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] == u""), u"The company[%s] Industry is NOT Empty: %s" % (element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER], element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY])
                element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] = COMPANY_GROUP_EXCEPTION_DICT[mobj.group(1)]
            # if re.match(self.ETF_COMPANY_CODE_NUMBER_PATTERN, element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER], re.U):
            #     # import pdb; pdb.set_trace()
            #     assert (element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] == u""), u"The company[%s] Industry is NOT Empty: %s" % (element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER], element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY])
            #     element_list[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] = COMPANY_GROUP_EXCEPTION_DICT[COMPANY_GROUP_ETF_BY_COMPANY_CODE_NUMBER_FIRST_TWO_DIGIT]
# Check if the company is required to modify the data by myself
            if company_number in self.modified_company_number_list:
                g_logger.warn("WARNING: The company[%s] is required to update the data in the profile by myself......" % company_number)
                modified_company_lookup = self.modified_company_lookup[company_number]
                for key, value in modified_company_lookup.items():
                    element_list[key] = value

            self.company_profile_list.append(element_list)
# 有價證券代號及名稱
# 國際證券辨識號碼(ISIN Code) 
# 上市日 
# 市場別 
# 產業別 
# CFICode 
# Company Group (Added)
# Company Group Number (Added)


    def __generate_company_group_profile_list(self):
        # import pdb; pdb.set_trace()
        # if self.company_group_profile_list != None:
        #     raise ValueError("The self.company_group_profile_list is NOT None");
        self.company_group_profile_list = []
        for index in range(self.__company_group_size):
            self.company_group_profile_list.append([])

        for company_profile in self.company_profile_list:
            company_group_number = company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NUMBER];
            self.company_group_profile_list[int(company_group_number)].append(company_profile)


    def __write_company_profile_to_file(self, cur_timestamp_str=None):
        # import pdb; pdb.set_trace()
# File for keeping track of the company code number info
#         conf_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, CMN.DEF.CONF_FOLDER, CMN.DEF.COMPANY_PROFILE_CONF_FILENAME)
#         g_logger.debug("Write the Company Code Number info to the file: %s......" % conf_filepath)
#         with open(conf_filepath, 'wb') as fp:
#             try:
#                 for company_profile in self.company_profile_list:
#                     self.company_profile_dict[company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]] = company_profile
#                     company_profile_unicode = u",".join(company_profile)
#                     # g_logger.debug(u"Company Code Number Data: %s", company_profile_unicode)
# # Can be readable for the CSV reader by encoding utf-8 unicode
#                     fp.write(company_profile_unicode.encode(self.UNICODE_ENCODING_IN_FILE) + "\n") 
#             except Exception as e:
#                 g_logger.error(u"Error occur while writing Company Code Number info into config file, due to %s" %str(e))
#                 # g_logger.error(u"Error occur while writing Company Code Number[%s] info into config file, due to %s" % (company_profile_unicode, str(e)))
#                 raise e
        if cur_timestamp_str is None:
            cur_timestamp_str = CMN.FUNC.generate_cur_timestamp_str()
        g_logger.debug("Write the Company Code Number info to the file: %s......" % CMN.DEF.COMPANY_PROFILE_CONF_FILENAME)
        company_profile_unicode_list = []
# Add the timestamp in the first line
        company_profile_unicode_list.append(cur_timestamp_str)
        for company_profile in self.company_profile_list:
            self.company_profile_dict[company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]] = company_profile
            company_profile_unicode = u",".join(company_profile)
            company_profile_unicode_list.append(company_profile_unicode)
            # g_logger.debug(u"Company Code Number Data: %s", company_profile_unicode)
# Can be readable for the CSV reader by encoding utf-8 unicode
        CMN.FUNC.unicode_write_config_file_lines(company_profile_unicode_list, CMN.DEF.COMPANY_PROFILE_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)
# File for keeping track of the company group info
#         conf_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, CMN.DEF.CONF_FOLDER, CMN.DEF.COMPANY_GROUP_CONF_FILENAME)
#         g_logger.debug("Write the Company Group info to the file: %s......" % conf_filepath)
#         with open(conf_filepath, 'wb') as fp:
#             try:
#                 for index, company_group in enumerate(self.company_group_num2name_list):
#                     company_group_unicode = u"%d %s" % (index, company_group)
# # Can be readable for the CSV reader by encoding utf-8 unicode
#                     fp.write(company_group_unicode.encode(self.UNICODE_ENCODING_IN_FILE) + "\n") 
#             except Exception as e:
#                 g_logger.error(u"Error occur while writing Company Group into config file, due to %s" %str(e))
#                 # g_logger.error(u"Error occur while writing Company Code Number[%s] info into config file, due to %s" % (company_profile_unicode, str(e)))
#                 raise e
        g_logger.debug("Write the Company Group info to the file: %s......" % CMN.DEF.COMPANY_GROUP_CONF_FILENAME)
        company_group_unicode_list = []
        company_group_unicode_list.append(cur_timestamp_str)
        for index, company_group in enumerate(self.company_group_num2name_list):
            company_group_unicode = u"%d %s" % (index, company_group)
            company_group_unicode_list.append(company_group_unicode)
# Can be readable for the CSV reader by encoding utf-8 unicode
        CMN.FUNC.unicode_write_config_file_lines(company_group_unicode_list, CMN.DEF.COMPANY_GROUP_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)


    def __write_company_profile_change_list_to_file(self, old_lost_list=None, new_added_list=None, cur_timestamp_str=None):
        # import pdb; pdb.set_trace()
        if cur_timestamp_str is None:
            cur_timestamp_str = CMN.FUNC.generate_cur_timestamp_str()
        config_line_list = [cur_timestamp_str,]
        if old_lost_list is None and new_added_list is None:
# Remove the old config file if exist
            CMN.FUNC.remove_config_file_if_exist(CMN.DEF.COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME)
            config_line_list.append("Initial update")
            CMN.FUNC.write_config_file_lines(config_line_list, CMN.DEF.COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)
        else:
# Append the company change result
            if not CMN.FUNC.check_config_file_exist(CMN.DEF.COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH):
                raise ValueError("The config file[%s] does NOT exist" % CMN.DEF.COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME)
            company_change_str = ""
            if old_lost_list is not None:
                company_change_str += "OldLost:" + ",".join(old_lost_list) + " "
            if new_added_list is not None:
                company_change_str += "NewAdded:" + ",".join(new_added_list) + " "
            config_line_list.append(company_change_str)
            CMN.FUNC.write_config_file_lines_ex(config_line_list, CMN.DEF.COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME, "a+", GV.FINANCE_DATASET_CONF_FOLDERPATH)


    def __write_company_group_to_file(self):
        # import pdb; pdb.set_trace()
#         conf_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, CMN.DEF.CONF_FOLDER, CMN.DEF.COMPANY_PROFILE_CONF_FILENAME)
#         g_logger.debug("Write the Company Code Number info to the file: %s......" % conf_filepath)
#         with open(conf_filepath, 'wb') as fp:
#             try:
#                 for company_profile in self.company_profile_list:
#                     self.company_profile_dict[company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]] = company_profile
#                     company_profile_unicode = u",".join(company_profile)
#                     # g_logger.debug(u"Company Code Number Data: %s", company_profile_unicode)
# # Can be readable for the CSV reader by encoding utf-8 unicode
#                     fp.write(company_profile_unicode.encode(self.UNICODE_ENCODING_IN_FILE) + "\n") 
#             except Exception as e:
#                 g_logger.error(u"Error occur while writing Company Code Number info into config file, due to %s" %str(e))
#                 # g_logger.error(u"Error occur while writing Company Code Number[%s] info into config file, due to %s" % (company_profile_unicode, str(e)))
#                 raise e
        cur_timestamp_str = CMN.FUNC.generate_cur_timestamp_str()
        company_profile_unicode_list = []
        company_profile_unicode_list.append(cur_timestamp_str)
        for company_profile in self.company_profile_list:
            self.company_profile_dict[company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]] = company_profile
            company_profile_unicode = u",".join(company_profile)
            company_profile_unicode_list.append(company_profile_unicode)
            # g_logger.debug(u"Company Code Number Data: %s", company_profile_unicode)
# Can be readable for the CSV reader by encoding utf-8 unicode
        CMN.FUNC.unicode_write_config_file_lines(company_profile_unicode_list, CMN.DEF.COMPANY_PROFILE_CONF_FILENAME, GV.FINANCE_DATASET_CONF_FOLDERPATH)


    def __get_exceptional_company_industry_by_company_code_number_first_2_digit(self, company_code_number):
        # import pdb; pdb.set_trace()
        for key, value in COMPANY_GROUP_EXCEPTION_DICT.items():
            pattern = r"%s" % str(key)
            if re.match(pattern, company_code_number, re.U):
                return value
        raise ValueError(u"Unknown exceptional company group: %s" % company_code_number)


    def __group_company_by_company_code_number_first_2_digit(self, show_detail):
        group_dict = {}
        for company_profile in self.company_profile_list:
            company_code_number = str(company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
            company_code_number_first_2_digit = company_code_number[0:2]
            if group_dict.get(company_code_number_first_2_digit, None) is None:
                group_dict[company_code_number_first_2_digit] = []
            group_dict[company_code_number_first_2_digit].append(company_code_number)
        # for group_key, group_value in group_dict.items():
        #     print "Group: %s, Len: %d; %s" % (group_key, len(group_value), ",".join(group_value))
        for group_key in sorted(group_dict):
            if show_detail:
                print "Group: %s, Len: %d; %s" % (group_key, len(group_dict[group_key]), ",".join(group_dict[group_key]))
            else:
                print "Group: %s, Len: %d" % (group_key, len(group_dict[group_key]))
        print "There are totally %d groups" % len(group_dict.keys())


    def __group_company_by_industry(self, show_detail):

        def get_company_group_name(company_profile):
            company_group_name = None
            if company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] == u"":
                company_group_name = self.__get_exceptional_company_industry_by_company_code_number_first_2_digit(company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
            else:
                company_group_name = company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY]
            return company_group_name

        group_dict = {}
        for company_profile in self.company_profile_list:
            company_group_name = get_company_group_name(company_profile)
            if group_dict.get(company_group_name, None) is None:
                group_dict[company_group_name] = []       
            group_dict[company_group_name].append(company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
        # for group_key, group_value in group_dict.items():
        #     print "Group: %s, Len: %d; %s" % (group_key, len(group_value), ",".join(group_value))
        for group_key in sorted(group_dict):
            if show_detail:
                print "Group: %s, Len: %d; %s" % (group_key, len(group_dict[group_key]), ",".join(group_dict[group_key]))
            else:
                print "Group: %s, Len: %d" % (group_key, len(group_dict[group_key]))
        print "There are totally %d groups" % len(group_dict.keys())


    def __group_company_by_industry_and_market(self, show_detail):

        def get_company_group_name(company_profile):
            company_group_name = None
            if company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY] == u"":
                company_group_name = self.__get_exceptional_company_industry_by_company_code_number_first_2_digit(company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
            else:
                company_group_name = company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY]
                if company_group_name in LARGE_INDUSTRY_COMPANY_GROUP_LIST:
                    company_group_name = u"%s-%s" % (company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_INDUSTRY], company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_MARKET_TYPE])
            return company_group_name

        group_dict = {}
        for company_profile in self.company_profile_list:
            company_group_name = get_company_group_name(company_profile)
            if group_dict.get(company_group_name, None) is None:
                group_dict[company_group_name] = []       
            group_dict[company_group_name].append(company_profile[COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
        # for group_key, group_value in group_dict.items():
        #     print "Group: %s, Len: %d; %s" % (group_key, len(group_value), ",".join(group_value))
        for group_key in sorted(group_dict):
            if show_detail:
                print "Group: %s, Len: %d; %s" % (group_key, len(group_dict[group_key]), ",".join(group_dict[group_key]))
            else:
                print "Group: %s, Len: %d" % (group_key, len(group_dict[group_key]))
        print "There are totally %d groups" % len(group_dict.keys())


    def iterator(self):
        return self.company_profile_list


    def group_iterator(self, company_group_index):
        # import pdb; pdb.set_trace()
        if not (0 <= company_group_index < self.__company_group_size):
            raise ValueError("The company group index[%d] is Out Of Range [0, %d)" % (company_group_index, self.__company_group_size))
        return self.company_group_profile_list[company_group_index]


    def group_company(self, method_number, show_detail):
        (self.group_company_func_ptr[method_number])(show_detail)


    @property
    def CompanyGroupSize(self):
        return self.__company_group_size


    @property
    def CompanyAmount(self):
        return self.__company_amount


    def get_company_group_description(self, index):
        if index < 0 or index >= self.__company_group_size:
            raise ValueError("index[%d] is Out Of Range [0, %d)" % (index, self.__company_group_size));
        return self.company_group_num2name_list[index];


    def lookup_company_profile(self, company_number, disable_exception=False):
        # import pdb; pdb.set_trace()
        company_number_unicode = CMN.FUNC.to_unicode(company_number, self.UNICODE_ENCODING_IN_FILE)
        company_profile = self.company_profile_dict.get(company_number_unicode, None)
        if company_profile is None:
            # import pdb; pdb.set_trace()
            if not disable_exception:
                raise ValueError("Fail to find the company profile of company number: %s" % company_number)
        return company_profile


    def lookup_company_listing_date(self, company_number):
        COMPANY_PROFILE = self.lookup_company_profile(company_number)
        return COMPANY_PROFILE[COMPANY_PROFILE_ENTRY_FIELD_INDEX_LISTING_DATE]


    def lookup_company_market_type(self, company_number):
        COMPANY_PROFILE = self.lookup_company_profile(company_number)
        return COMPANY_PROFILE[COMPANY_PROFILE_ENTRY_FIELD_INDEX_MARKET_TYPE]


    def lookup_company_market_type_index(self, company_number):
        company_market_type = self.lookup_company_market_type(company_number)
        company_market_type_index = COMPANY_MARKET_TYPE_DICT.get(company_market_type, None)
        if company_market_type_index is None:
            raise ValueError(u"Unknown Company[%s] Market Type: %d", company_number, company_market_type)
        return company_market_type_index


    def lookup_company_group_name(self, company_number):
        COMPANY_PROFILE = self.lookup_company_profile(company_number, disable_exception)
        return COMPANY_PROFILE[COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NAME]


    def lookup_company_group_number(self, company_number, disable_exception=False):
        COMPANY_PROFILE = self.lookup_company_profile(company_number)
        if not disable_exception and COMPANY_PROFILE is None:
            return None
        return COMPANY_PROFILE[COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NUMBER]


    def lookup_company_first_data_date(self, company_number, time_unit):
        start_date_str = str(self.lookup_company_listing_date(company_number))
        datetime_now = datetime.today()
        end_date_str = CMN.FUNC.transform_date_str(datetime_now.year, datetime_now.month, datetime_now.day)
 
        start_time = end_time = None
        if time_unit == CMN.DEF.DATA_TIME_UNIT_DAY or time_unit == CMN.DEF.DATA_TIME_UNIT_WEEK:
            start_time = CMN.CLS.FinanceDate(start_date_str)
            end_time = CMN.CLS.FinanceDate(end_date_str)
        elif time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
            start_time = CMN.CLS.FinanceMonth.get_finance_month_from_date(start_date_str)
            end_time = CMN.CLS.FinanceMonth.get_finance_month_from_date(end_date_str)
        elif time_unit == CMN.DEF.DATA_TIME_UNIT_QUARTER:
            start_time = CMN.CLS.FinanceQuarter.get_start_finance_quarter_from_date(start_date_str)
            end_time = CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(end_date_str)
        else:
            raise CMN.EXCEPTION.WebScrapyUnDefiedCaseException("UnSupported time unit: %d" % time_unit)

        if start_time <= end_time:
            timeslice_generate_method = CMN.DEF.TIMESLICE_GENERATE_TO_TIME_UNIT_MAPPING[time_unit]
            time_slice_generator_cfg = {
                "company_code_number": company_number, 
                "time_duration_start": start_time, 
                "time_duration_end": end_time,
            }
                # import pdb; pdb.set_trace()
# Generate the time slice
            timeslice_iterable = self.__get_timeslice_generator().generate_time_slice(timeslice_generate_method, **time_slice_generator_cfg)
            count = 0
            for timeslice in timeslice_iterable:
                if count == 1:
                    return timeslice
                count += 1
        return None


    def is_company_exist(self, company_number, company_group_number=None):
        COMPANY_PROFILE = self.lookup_company_profile(company_number, True)
        if COMPANY_PROFILE is None:
            return False
# Check company is in certain a group if necessary
        if company_group_number is not None:
            if int(COMPANY_PROFILE[COMPANY_PROFILE_ENTRY_FIELD_INDEX_GROUP_NUMBER]) != int(company_group_number):
                return False
        return True


    def is_company_etf(self, company_number):
        assert self.is_company_exist(company_number), "The company number[%s] does NOT exist" % company_number
        return True if (re.match(ETF_COMPANY_CODE_NUMBER_PATTERN, company_number, re.U) is not None) else False


    def is_company_group_number_in_range(self, company_group_number):
        company_group_number = int(company_group_number)
        return False if (company_group_number < 0 or company_group_number >= self.__company_group_size) else True
