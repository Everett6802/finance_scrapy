# -*- coding: utf8 -*-

import os
import sys
import time
import requests
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
import libs.web_scrapy_mgr_base as MgrBase
import web_scrapy_company_profile as CompanyProfile
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyStockMgr(MgrBase.WebSracpyMgrBase):

	company_profile = None
    def __init__(self):
        pass


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def _create_finance_folder_if_not_exist(cls):
    	company_group_size = cls.__get_company_profile().get_company_group_size()
        folderpath_format = ("%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME)) + "%02d"
        for index in range(company_group_size):
        	folderpath = folderpath_format % index
        	CMN.FUNC.create_folder_if_not_exist(folderpath)
