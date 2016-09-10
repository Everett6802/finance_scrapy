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

    def __init__(self):
        super(WebSracpyMarketMgr, self).__init__()


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


    def do_scrapy(self):
       # import pdb; pdb.set_trace()
        self._scrap_data(self.xcfg["reserve_old_finance_folder"])


    def check_scrapy(self):
       # import pdb; pdb.set_trace()
        file_not_found_list = []
        file_is_empty_list = []
        for source_type_time_range in self.source_type_time_range_list:
            csv_filepath = WebScrapyMarketBase.assemble_csv_filepath(source_type_time_range.source_type_index)
# Check if the file exists
            if not os.path.exists(csv_filepath):
                file_not_found_list.append(
                    {
                        "index": source_type_time_range.source_type_index,
                        "filename" : WebSracpyMgrBase._get_csv_filename_from_filepath(csv_filepath)
                    }
                )
            elif os.path.getsize(csv_filepath) == 0:
                file_is_empty_list.append(
                    {
                        "index": source_type_time_range.source_type_index,
                        "filename" : WebSracpyMgrBase._get_csv_filename_from_filepath(csv_filepath)
                    }
                )
        return (file_not_found_list, file_is_empty_list)
