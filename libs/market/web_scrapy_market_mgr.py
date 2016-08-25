# -*- coding: utf8 -*-

import os
import sys
import time
import requests
import shutil
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyMarketMgr(BASE.MGR_BASE.WebSracpyMgrBase):

    def __init__(self, **kwargs):
        super(WebSracpyMarketMgr, self).__init__(**kwargs)


    @classmethod
    def __get_finance_folderpath(cls):
    	return "%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME)


    @classmethod
    def _create_finance_folder_if_not_exist(cls):
        folderpath = cls.__get_finance_folderpath()
        CMN.FUNC.create_folder_if_not_exist(folderpath)


    @classmethod
    def _remove_old_finance_folder(cls):
# Remove the old data if necessary
        folderpath = cls.__get_finance_folderpath()
        shutil.rmtree(folderpath, ignore_errors=True)


    def do_scrapy(self, source_type_time_range_list):
       # import pdb; pdb.set_trace()
        self._scrap_data(source_type_time_range_list, self.xcfg["need_remove_old_finance_folder"], self.xcfg["try_to_scrap_all"])


    def check_scrapy(self, source_type_time_range_list):
        raise NotImplementedError
