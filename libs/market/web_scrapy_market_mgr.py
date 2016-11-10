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
        self.source_type_csv_time_duration = None


    def __get_finance_folderpath(self):
    	return "%s/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.DEF_CSV_MARKET_FOLDERNAME)


    def _create_finance_folder_if_not_exist(self):
        self._create_finance_root_folder_if_not_exist()
        folderpath = self.__get_finance_folderpath()
        g_logger.debug("Try to create new folder: %s" % folderpath)
        CMN.FUNC.create_folder_if_not_exist(folderpath)


    def _remove_old_finance_folder(self):
# Remove the old data if necessary
        folderpath = self.__get_finance_folderpath()
        g_logger.debug("Remove old folder: %s" % folderpath)
        shutil.rmtree(folderpath, ignore_errors=True)


    def _init_csv_time_duration(self):
        assert self.source_type_csv_time_duration is None, "self.source_type_csv_time_duration should be None"
        # self.source_type_csv_time_duration = [None] * CMN.DEF.DEF_DATA_SOURCE_MARKET_SIZE
        self.source_type_csv_time_duration = {}


    def _read_old_csv_time_duration(self):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration is not None, "self.source_type_csv_time_duration should NOT be None"
        csv_data_folderpath = self.__get_finance_folderpath()
        g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
        csv_time_duration_dict = CMN.FUNC.parse_csv_time_duration_config_file(CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
        if csv_time_duration_dict is None:
            g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME)
            return
# # update the time range of each source type from config file
#         for source_type_index, time_duration_tuple in csv_time_duration_dict.items():
#             self.source_type_csv_time_duration[source_type_index] = time_duration_tuple
        self.source_type_csv_time_duration = csv_time_duration_dict


    def _update_new_csv_time_duration(self, web_scrapy_obj):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration is not None, "self.source_type_csv_time_duration should NOT be None"
        new_csv_time_duration = web_scrapy_obj.get_new_csv_time_duration()
        self.source_type_csv_time_duration[web_scrapy_obj.SourceTypeIndex] = new_csv_time_duration


    def _write_new_csv_time_duration(self):
        # import pdb; pdb.set_trace()
        csv_data_folderpath = self.__get_finance_folderpath()
        g_logger.debug("Try to write CSV time range config in the folder: %s ......" % csv_data_folderpath)
        CMN.FUNC.write_csv_time_duration_config_file(CMN.DEF.DEF_CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath, self.source_type_csv_time_duration)


    def _add_cfg_for_scrapy_obj(self, scrapy_obj_cfg):
        super(WebSracpyMarketMgr, self)._add_cfg_for_scrapy_obj(scrapy_obj_cfg)
        scrapy_obj_cfg["csv_time_duration_table"] = self.source_type_csv_time_duration


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
