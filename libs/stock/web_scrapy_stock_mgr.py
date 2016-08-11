# -*- coding: utf8 -*-

import os
import sys
import time
import requests
import shutil
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
import libs.web_scrapy_mgr_base as MgrBase
import web_scrapy_company_profile as CompanyProfile
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyStockMgr(MgrBase.WebSracpyMgrBase):

	company_profile = None
    def __init__(self):
        super(WebSracpyStockMgr, self).__init__(**kwargs)


    @classmethod
    def __get_finance_folderpath_format(cls):
        return folderpath_format = ("%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME)) + "%02d"


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def _create_finance_folder_if_not_exist(cls):
    	company_group_size = cls.__get_company_profile().get_company_group_size()
        folderpath_format = cls.__get_finance_folderpath_format()
        for index in range(company_group_size):
        	folderpath = folderpath_format % index
        	CMN.FUNC.create_folder_if_not_exist(folderpath)


    @classmethod
    def _remove_old_finance_folder(cls):
# Remove the old data if necessary
        folderpath_format = cls.__get_finance_folderpath_format()
        for index in range(company_group_size):
            folderpath = folderpath_format % index
            shutil.rmtree(folderpath, ignore_errors=True)


    def do_scrapy(self, source_type_time_range_list):
        raise NotImplementedError


    def check_scrapy(self, source_type_time_range_list):
        raise NotImplementedError
