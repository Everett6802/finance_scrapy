# -*- coding: utf8 -*-

import os
import sys
import time
import requests
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
# import libs.base.web_scrapy_mgr_base as MgrBase
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyMarketMgr(BASE.MGR_BASE.WebSracpyMgrBase):

    def __init__(self):
        pass


    @classmethod
    def _create_finance_folder_if_not_exist(cls):
        folderpath = "%s/%s" % (CMN.DEF.DEF_CSV_FILE_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME)
        CMN.FUNC.create_folder_if_not_exist(folderpath)
