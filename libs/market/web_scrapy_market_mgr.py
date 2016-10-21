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
        self.csv_time_duration_list = None


    def __get_finance_folderpath(self):
    	return "%s/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_MARKET_FOLDERNAME)


    def _create_finance_folder_if_not_exist(self):
        folderpath = self.__get_finance_folderpath()
        g_logger.debug("Try to create new folder: %s" % folderpath)
        CMN.FUNC.create_folder_if_not_exist(folderpath)


    def _remove_old_finance_folder(self):
# Remove the old data if necessary
        folderpath = self.__get_finance_folderpath()
        g_logger.debug("Remove old folder: %s" % folderpath)
        shutil.rmtree(folderpath, ignore_errors=True)


    def _update_csv_data_time_duration(self):
        self.csv_time_duration_list = [None] * CMN.DEF.DEF_DATA_SOURCE_MARKET_SIZE
        csv_data_folderpath = self.__get_finance_folderpath()
        g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
        csv_time_duration_dict = CMN.DEF.parse_csv_time_duration_config_file(CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
        if csv_time_duration_dict is None:
            g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME)
            return
# update the time range of each source type from csv files
        for source_type_index, time_duration_tuple in csv_time_duration_dict.items():
            self.csv_time_duration_list[source_type_index] = time_duration_tuple


    def _add_cfg_for_scrapy_obj(self, scrapy_obj_cfg):
        super(WebSracpyMarketMgr, self)._add_cfg_for_scrapy_obj(scrapy_obj_cfg)
        scrapy_obj_cfg["csv_time_duration_table"] = self.csv_time_duration_list


    def do_scrapy(self):
       # import pdb; pdb.set_trace()
        self._scrap_data()


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
