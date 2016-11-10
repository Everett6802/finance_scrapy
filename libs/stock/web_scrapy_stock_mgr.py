# -*- coding: utf8 -*-

import os
import re
import sys
import time
import requests
import shutil
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
import web_scrapy_company_profile as CompanyProfile
import web_scrapy_company_group_set as CompanyGroupSet
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyStockMgr(BASE.MGR_BASE.WebSracpyMgrBase):

    company_profile = None
    def __init__(self):
        super(WebSracpyStockMgr, self).__init__()
        self.company_group_set = None
        self.source_type_csv_time_duration_dict = None


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    def __get_finance_folderpath_format(self):
        return ("%s/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_STOCK_FOLDERNAME)) + "%02d"


    def _create_finance_folder_if_not_exist(self):
        self._create_finance_root_folder_if_not_exist()
        folderpath_format = self.__get_finance_folderpath_format()
        for index in range(self.__get_company_profile().CompanyGroupSize):
            folderpath = folderpath_format % index
            g_logger.debug("Try to create new folder: %s" % folderpath)
            CMN.FUNC.create_folder_if_not_exist(folderpath)


    def _remove_old_finance_folder(self):
# Remove the old data if necessary
        folderpath_format = self.__get_finance_folderpath_format()
        for index in range(self.__get_company_profile().CompanyGroupSize):
            folderpath = folderpath_format % index
            g_logger.debug("Remove old folder: %s" % folderpath)
            shutil.rmtree(folderpath, ignore_errors=True)


    def _init_csv_time_duration(self):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration_dict is None, "self.source_type_csv_time_duration_dict should be None"
        self.source_type_csv_time_duration_dict = {}
        for company_group_number, company_code_number_list in self.company_group_set.items():
            for company_code_number in company_code_number_list:
                # csv_time_duration_list = [None] * CMN.DEF.DEF_DATA_SOURCE_STOCK_SIZE
                self.source_type_csv_time_duration_dict[company_code_number] = {}


    def _read_old_csv_time_duration(self):
        assert self.source_type_csv_time_duration_dict is not None, "self.source_type_csv_time_duration_dict should NOT be None"
        # whole_company_number_in_group_dict = CompanyGroupSet.get_whole_company_number_in_group_dict()
        folderpath_format = self.__get_finance_folderpath_format()
        # self.source_type_csv_time_duration_dict = {}
        # for company_group_number, company_code_number_list in whole_company_number_in_group_dict:
        for company_group_number, company_code_number_list in self.company_group_set.items():
            folderpath_in_group = folderpath_format % (company_group_number)
# If the company group folder does NOT exist, ignore it...
            if not CMN.DEF.check_file_exist(folderpath_in_group):
                continue
            for company_code_number in company_code_number_list:
                csv_data_folderpath = "%s/%s" % (folderpath_in_group, company_code_number) 
                g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
                csv_time_duration_dict = CMN.FUNC.parse_csv_time_duration_config_file(CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
                if csv_time_duration_dict is None:
                    g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME)
                    continue
# update the time range of each source type of comapny from config files
                # csv_time_duration_list = [None] * CMN.DEF.DEF_DATA_SOURCE_STOCK_SIZE
                # for source_type_index, time_duration_tuple in csv_time_duration_dict.items():
                #     csv_time_duration_list[source_type_index - CMN.DEF.DEF_DATA_SOURCE_STOCK_START] = time_duration_tuple
                self.source_type_csv_time_duration_dict[company_code_number] = csv_time_duration_dict


    def _update_new_csv_time_duration(self, web_scrapy_obj):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration_dict is not None, "self.source_type_csv_time_duration_dict should NOT be None"
        new_csv_time_duration_dict = web_scrapy_obj.get_new_csv_time_duration_dict()
        # source_type_index_offset = web_scrapy_obj.SourceTypeIndex - CMN.DEF.DEF_DATA_SOURCE_STOCK_START
        for company_number, time_duration_tuple in new_csv_time_duration_dict.items():
            self.source_type_csv_time_duration_dict[company_number][web_scrapy_obj.SourceTypeIndex] = time_duration_tuple


    def _write_new_csv_time_duration(self):
        # import pdb; pdb.set_trace()
        folderpath_format = self.__get_finance_folderpath_format()
        for company_group_number, company_code_number_list in self.company_group_set.items():
            folderpath_in_group = folderpath_format % int(company_group_number)
            for company_code_number in company_code_number_list:
                csv_data_folderpath = "%s/%s" % (folderpath_in_group, company_code_number) 
                g_logger.debug("Try to write CSV time range config in the folder: %s ......" % csv_data_folderpath)
                CMN.FUNC.write_csv_time_duration_config_file(CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath, self.source_type_csv_time_duration_dict[company_code_number])


    def __transform_company_word_list_to_group_set(self, company_word_list):
        """
        The argument type:
        Company code number: 2347
        Company code number range: 2100-2200
        Company group number: [Gg]12
        Company code number/number range hybrid: 2347,2100-2200,2362,g2,1500-1510
        """
        self.company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet()
        for company_number in company_word_list:
            mobj = re.match("([\d]{4})-([\d]{4})", company_number)
            if mobj is None:
# Check if data is company code/group number
                mobj = re.match("[Gg]([\d]{1,})", company_number)
                if mobj is None:
# Company code number
                    if not re.match("([\d]{4})", company_number):
                        raise ValueError("Unknown company number format: %s" % company_number)
                    self.company_group_set.add_company(company_number)
                else:
# Compgny group number
                    company_group_number = int(mobj.group(1))
                    self.company_group_set.add_company_group(company_group_number)
            else:
# Company code number Range
                start_company_number_int = int(mobj.group(1))
                end_company_number_int = int(mobj.group(2))
                number_list = []
                for number in range(start_company_number_int, end_company_number_int + 1):
                    number_list.append("%04d" % number)
                self.company_group_set.add_company_word_list(number_list)
        self.company_group_set.add_done()


    def set_company_from_file(self, filename):
        company_word_list = CMN.FUNC.parse_source_type_time_range_config_file(filename)
        self.__transform_company_word_list_to_group_set(company_word_list)


    def set_company(self, company_word_list):
        self.__transform_company_word_list_to_group_set(company_word_list)


    # def initialize(**kwargs):
    #     super(WebSracpyStockMgr, self).initialize(**kwargs)
    #     if kwargs.get("company_word_list", None) is not None:
    #         company_group_set = WebScrapyCompanyGroupSet()
    #         for company_number in kwargs["company_word_list"]:
    #             company_group_set.add_company(company_number)
    #         company_group_set.add_done();
    #         if kwargs.get("company_group_set", None) is not None:
    #             g_logger.warn("The company_group_set field is ignored......")
    #     elif kwargs.get("company_group_set", None) is not None:
    #         self.xcfg["company_group_set"] = kwargs["company_group_set"]
    #     else:
    #         self.xcfg["company_group_set"] = CompanyGroupSet.get_whole_company_group_set()


    def _add_cfg_for_scrapy_obj(self, scrapy_obj_cfg):
        super(WebSracpyStockMgr, self)._add_cfg_for_scrapy_obj(scrapy_obj_cfg)
        scrapy_obj_cfg["company_group_set"] = self.company_group_set
        scrapy_obj_cfg["csv_time_duration_table"] = self.source_type_csv_time_duration_dict


    def do_scrapy(self):
        self._scrap_data()


    def check_scrapy(self):
        file_not_found_list = []
        file_is_empty_list = []
        for source_type_time_range in self.source_type_time_range_list:
            for company_group_number, company_code_number_list in self.company_group_set.items():
                for company_code_number in company_code_number_list:
                    csv_filepath = WebScrapyStockBase.assemble_csv_filepath(source_type_time_range.source_type_index, company_code_number, company_group_index)
# Check if the file exists
                    if not os.path.exists(csv_filepath):
                        file_not_found_list.append(
                            {
                                "index": source_type_time_range.source_type_index,
                                "filename" : WebSracpyMgrBase._get_csv_filename_from_filepath(csv_filepath),
                            }
                        )
                    elif os.path.getsize(csv_filepath) == 0:
                        file_is_empty_list.append(
                            {
                                "index": source_type_time_range.source_type_index,
                                "filename" : WebSracpyMgrBase._get_csv_filename_from_filepath(csv_filepath),
                            }
                        )
        return (file_not_found_list, file_is_empty_list)
