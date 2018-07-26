# -*- coding: utf8 -*-

import os
import sys
import time
import requests
import shutil
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
# import web_scrapy_market_base as MarketBase
# import scrapy_configurer as Configurer
g_logger = CMN.LOG.get_logger()


class MarketMgr(BASE.MGR_BASE.MgrBase):

    finance_mode = CMN.DEF.FINANCE_MODE_MARKET
    def __init__(self, **cfg):
        super(MarketMgr, self).__init__(**cfg)
        self.source_type_csv_time_duration = None


    # def __get_finance_folderpath(self, finance_root_folderpath=None):
    #     if finance_root_folderpath is None:
    #         finance_root_folderpath = self.xcfg["finance_root_folderpath"]
    #     if finance_root_folderpath is None:
    #         finance_root_folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH
    # 	# return "%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_MARKET_FOLDERNAME)
    #     return CMN.FUNC.assemble_finance_data_folder(finance_root_folderpath, -1)


    # def _get_configurer(self):
    #     if self.configurer is None:
    #         self.configurer = Configurer.ScrapyConfigurer.Instance()
    #     return self.configurer


    def _create_finance_folder_if_not_exist(self, finance_root_folderpath=None):
        # self._create_finance_root_folder_if_not_exist(finance_root_folderpath)
        # folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        # g_logger.debug("Try to create new folder: %s" % folderpath)
        # CMN.FUNC.create_folder_if_not_exist(folderpath)
        CMN.FUNC.create_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)


    def _remove_old_finance_folder(self, finance_root_folderpath=None):
# Remove the old data if necessary
        # folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        # g_logger.debug("Remove old folder: %s" % folderpath)
        # shutil.rmtree(folderpath, ignore_errors=True)
        CMN.FUNC.delete_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)


    def _init_csv_time_duration(self):
        assert self.source_type_csv_time_duration is None, "self.source_type_csv_time_duration should be None"
        # self.source_type_csv_time_duration = [None] * CMN.DEF.DATA_SOURCE_MARKET_SIZE
        self.source_type_csv_time_duration = {}


    def __parse_csv_time_duration_cfg(self, finance_root_folderpath=None):
        # csv_data_folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        csv_data_folderpath = CMN.FUNC.assemble_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
        csv_time_duration_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
        if csv_time_duration_dict is None:
            g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME)
# # update the time range of each source type from config file
#         for scrapy_class_index, time_duration_tuple in csv_time_duration_dict.items():
#             self.source_type_csv_time_duration[scrapy_class_index] = time_duration_tuple
        return csv_time_duration_dict


    def _read_old_csv_time_duration(self):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration is not None, "self.source_type_csv_time_duration should NOT be None"
        source_type_csv_time_duration = self.__parse_csv_time_duration_cfg()
        if source_type_csv_time_duration is not None:
            self.source_type_csv_time_duration = source_type_csv_time_duration


    def _update_new_csv_time_duration(self, web_scrapy_obj):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration is not None, "self.source_type_csv_time_duration should NOT be None"
        # new_csv_time_duration = web_scrapy_obj.get_new_csv_time_duration()
        self.source_type_csv_time_duration[web_scrapy_obj.SourceTypeIndex] = web_scrapy_obj.new_csv_time_duration


    def __write_new_csv_time_duration_to_cfg(self, finance_root_folderpath=None, source_type_csv_time_duration=None):
        # import pdb; pdb.set_trace()
        # csv_data_folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        csv_data_folderpath = CMN.FUNC.assemble_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        if source_type_csv_time_duration is None:
            source_type_csv_time_duration = self.source_type_csv_time_duration
        # g_logger.debug("Try to write CSV time range config in the folder: %s ......" % csv_data_folderpath)
        CMN.FUNC.write_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath, source_type_csv_time_duration)


    def _write_new_csv_time_duration(self):
        self.__write_new_csv_time_duration_to_cfg()


    def _scrape_class_data(self, scrapy_class_time_duration):
# Setup the time duration configuration for the scrapy object
        scrapy_obj_cfg = self._init_cfg_for_scrapy_obj(scrapy_class_time_duration)
        scrapy_obj_cfg["csv_time_duration_table"] = self.source_type_csv_time_duration
        scrapy_obj_cfg["disable_flush_scrapy_while_exception"] = self.xcfg["disable_flush_scrapy_while_exception"]
# Create the scrapy object to transform the data from Web to CSV
        self._scrape_web_data_to_csv_file(scrapy_class_time_duration.scrapy_class_index, **scrapy_obj_cfg)


    def _find_existing_source_type_finance_folder_index(self, csv_time_duration_cfg_list, scrapy_class_index):
# Search for the index of the finance folder which the specific source type index exists
# -1 if not found
# Exception occur if the source type is found in more than one finance folder
        finance_folder_index = -1
        for index, csv_time_duration_cfg in enumerate(csv_time_duration_cfg_list):
            if csv_time_duration_cfg.has_key(scrapy_class_index):
                if finance_folder_index != -1:
                    raise ValueError("The source type index[%d] is duplicate" % scrapy_class_index)
                else:
                    finance_folder_index = index
        return finance_folder_index


    def merge_finance_folder(self, finance_folderpath_src_list, finance_folderpath_dst):
# Check the source folder exist
        self._check_merge_finance_folder_exist(finance_folderpath_src_list, finance_folderpath_dst)
        if CMN.FUNC.check_file_exist(finance_folderpath_dst):
            raise ValueError("The destination folder[%s] after mering has already exist" % finance_folderpath_dst)
        self._create_finance_folder_if_not_exist(finance_folderpath_dst)
# Find source type list in each source finance folder
        csv_time_duration_cfg_list = []
        for finance_folderpath_src in finance_folderpath_src_list:
            csv_time_duration_cfg_list.append(self.__parse_csv_time_duration_cfg(finance_folderpath_src))
# Merge the finance folder
# Copy the CSV files from source folder to destiantion one
        (scrapy_class_index_start, scrapy_class_index_end) = CMN.FUNC.get_scrapy_class_index_range()
        new_source_type_csv_time_duration = {}
        for scrapy_class_index in range(scrapy_class_index_start, scrapy_class_index_end):
            finance_folder_index = self._find_existing_source_type_finance_folder_index(csv_time_duration_cfg_list, scrapy_class_index)
            if finance_folder_index == -1:
                continue
            src_csv_filepath = CMN.FUNC.assemble_market_csv_filepath(finance_folderpath_src_list[finance_folder_index], scrapy_class_index)
            CMN.FUNC.create_folder_if_not_exist(CMN.FUNC.assemble_market_csv_folderpath(finance_folderpath_dst))
            dst_csv_filepath = CMN.FUNC.assemble_market_csv_filepath(finance_folderpath_dst, scrapy_class_index)
            CMN.FUNC.copy_file(src_csv_filepath, dst_csv_filepath)
# Update the new time duration config
            new_source_type_csv_time_duration[scrapy_class_index] = csv_time_duration_cfg_list[finance_folder_index][scrapy_class_index]
        # import pdb; pdb.set_trace()
        self.__write_new_csv_time_duration_to_cfg(finance_folderpath_dst, new_source_type_csv_time_duration)


    def enable_multithread(self, thread_amount):
        raise ValueError("Multi-Thread is NOT supported in market mode")
