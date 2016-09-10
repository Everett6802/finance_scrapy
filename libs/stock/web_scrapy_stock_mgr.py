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
    company_group_size = 0
    def __init__(self):
        super(WebSracpyStockMgr, self).__init__()
        self.company_group_set = None


    @classmethod
    def __get_finance_folderpath_format(cls):
        return ("%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME)) + "%02d"


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
            cls.company_group_size = cls.company_profile.get_company_group_size()
        return cls.company_profile


    @classmethod
    def _create_finance_folder_if_not_exist(cls):
        folderpath_format = cls.__get_finance_folderpath_format()
        for index in range(cls.company_group_size):
        	folderpath = folderpath_format % index
        	CMN.FUNC.create_folder_if_not_exist(folderpath)


    @classmethod
    def _remove_old_finance_folder(cls):
# Remove the old data if necessary
        folderpath_format = cls.__get_finance_folderpath_format()
        for index in range(cls.company_group_size):
            folderpath = folderpath_format % index
            shutil.rmtree(folderpath, ignore_errors=True)


    def __transform_company_list_to_group_set(self, company_number_list):
        """
        The argument type:
        Company code number: 2347
        Company code number range: 2100-2200
        Company code number/number range hybrid: 2347,2100-2200,2362,1500-1510
        Company group number: [Gg]12

        """
        self.company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet()
        for company_number in company_number_list:
            mobj = re.match("([\d]{d})-([\d]{4})", company_number)
            if mobj is None:
# Check if data is company code/group number
                mobj = re.match("[Gg]([\d]{1,})", company_number)
                if mobj is None:
# Company code number
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
                self.company_group_set.add_company_list(number_list)


    def set_company_from_file(self, filename):
        company_list = CMN.FUNC.parse_source_type_time_range_config_file(filename)
        self.__transform_company_list_to_group_set(company_list)


    def set_company(self, company_list):
        self.__transform_company_list_to_group_set(company_list)


    # def initialize(**kwargs):
    #     super(WebSracpyStockMgr, self).initialize(**kwargs)
    #     if kwargs.get("company_number_list", None) is not None:
    #         company_group_set = WebScrapyCompanyGroupSet()
    #         for company_number in kwargs["company_number_list"]:
    #             company_group_set.add_company(company_number)
    #         company_group_set.add_done();
    #         if kwargs.get("company_group_set", None) is not None:
    #             g_logger.warn("The company_group_set field is ignored......")
    #     elif kwargs.get("company_group_set", None) is not None:
    #         self.xcfg["company_group_set"] = kwargs["company_group_set"]
    #     else:
    #         self.xcfg["company_group_set"] = CompanyGroupSet.get_whole_company_group_set()


    def do_scrapy(self):
        self._scrap_data(self.xcfg["reserve_old_finance_folder"])


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
