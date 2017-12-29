# -*- coding: utf8 -*-

import os
import sys
import time
import requests
import shutil
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
import web_scrapy_market_base as MarketBase
import web_scrapy_market_configurer as Configurer
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyMarketMgr(BASE.MGR_BASE.WebScrapyMgrBase):

    finance_mode = CMN.DEF.FINANCE_MODE_MARKET
    def __init__(self, **cfg):
        super(WebScrapyMarketMgr, self).__init__(**cfg)
        self.source_type_csv_time_duration = None


    def __get_finance_folderpath(self, finance_root_folderpath=None):
        if finance_root_folderpath is None:
            finance_root_folderpath = self.xcfg["finance_root_folderpath"]
        if finance_root_folderpath is None:
            finance_root_folderpath = CMN.DEF.SCRAPY_METHOD_DESCRIPTIONCSV_ROOT_FOLDERPATH
    	return "%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_MARKET_FOLDERNAME)


    def _get_configurer(self):
        if self.configurer is None:
            self.configurer = Configurer.WebScrapyMarketConfigurer.Instance()
        return self.configurer


    def _create_finance_folder_if_not_exist(self, finance_root_folderpath=None):
        self._create_finance_root_folder_if_not_exist(finance_root_folderpath)
        folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        g_logger.debug("Try to create new folder: %s" % folderpath)
        CMN.FUNC.create_folder_if_not_exist(folderpath)


    def _remove_old_finance_folder(self, finance_root_folderpath=None):
# Remove the old data if necessary
        folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        g_logger.debug("Remove old folder: %s" % folderpath)
        shutil.rmtree(folderpath, ignore_errors=True)


    def _init_csv_time_duration(self):
        assert self.source_type_csv_time_duration is None, "self.source_type_csv_time_duration should be None"
        # self.source_type_csv_time_duration = [None] * CMN.DEF.DATA_SOURCE_MARKET_SIZE
        self.source_type_csv_time_duration = {}


    def __parse_csv_time_duration_cfg(self, finance_root_folderpath=None):
        csv_data_folderpath = self.__get_finance_folderpath(finance_root_folderpath)
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
        csv_data_folderpath = self.__get_finance_folderpath(finance_root_folderpath)
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
# Create the scrapy object to transform the data from Web to CSV
        self._scrape_web_data_to_csv_file(scrapy_class_time_duration.scrapy_class_index, **scrapy_obj_cfg)


#     def check_scrapy(self):
#         # import pdb; pdb.set_trace()
#         file_not_found_list = []
#         file_is_empty_list = []
#         for scrapy_class_time_duration in self.scrapy_class_time_duration_list:
#             csv_filepath = CMN.FUNC.assemble_market_csv_filepath(self.xcfg["finance_root_folderpath"], scrapy_class_time_duration.scrapy_class_index)
# # Check if the file exists
#             if not os.path.exists(csv_filepath):
#                 file_not_found_list.append(
#                     {
#                         "index": scrapy_class_time_duration.scrapy_class_index,
#                         "filename" : CMN.FUNC.get_filename_from_filepath(csv_filepath)
#                     }
#                 )
#             elif os.path.getsize(csv_filepath) == 0:
#                 file_is_empty_list.append(
#                     {
#                         "index": scrapy_class_time_duration.scrapy_class_index,
#                         "filename" : CMN.FUNC.get_filename_from_filepath(csv_filepath)
#                     }
#                 )
# # Output the missing CSV to the file if necessary
#         if not self.xcfg["disable_output_missing_csv"]:
#             file_not_found_list_len = len(file_not_found_list)
#             file_is_empty_list_len = len(file_is_empty_list)  
#             if file_not_found_list_len != 0 or file_is_empty_list_len != 0:
#                 missing_csv_filepath = "%s/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.MISSING_CSV_MARKET_FILENAME)
#                 g_logger.debug("Write missing CSVs to the file: %s......" % missing_csv_filepath)
#                 with open(missing_csv_filepath, 'wb') as fp:
#                     try:
# # Output the file not found list
#                         if file_not_found_list_len != 0:
#                             fp.write("[FileNotFound]\n") 
#                             for file_not_found in file_not_found_list[:-1]:
#                                 fp.write("%d;" % file_not_found["index"])
#                             fp.write("%d\n" % file_not_found_list[-1]["index"])
# # Output the file is empty list
#                         if file_is_empty_list_len != 0:
#                             fp.write("[FileIsEmpty]\n")
#                             for file_is_empty in file_is_empty_list[:-1]:
#                                 fp.write("%d;" % file_is_empty["index"])
#                             fp.write("%d\n" % file_is_empty_list[-1]["index"])
#                     except Exception as e:
#                         g_logger.error(u"Fail to write data to missing CSV file, due to %s" %str(e))
#                         # g_logger.error(u"Error occur while writing Company Code Number[%s] info into config file, due to %s" % (company_profile_unicode, str(e)))
#                         raise e
#         return (file_not_found_list, file_is_empty_list)


#     def check_scrapy_to_string(self):
#         (file_not_found_list, file_is_empty_list) = self.check_scrapy()
#         error_msg = None
#         error_msg_list = []
#         for file_not_found in file_not_found_list:
#             error_msg = u"FileNotFound: %s, %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[file_not_found['index']], file_not_found['filename'])
#             error_msg_list.append(error_msg)
#         for file_is_empty in file_is_empty_list:
#             error_msg = u"FileIsEmpty: %s, %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[file_is_empty['index']], file_is_empty['filename'])
#             error_msg_list.append(error_msg)
#         if len(error_msg_list) != 0:
#             error_msg = "\n".join(error_msg_list)
#         return error_msg


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


    # def _increment_scrapy_class_type_progress_count(self, scrapy_class_index):
    #     self.scrapy_class_type_progress_count += 1


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


    # def count_scrapy_amount(self):
    #     if self.scrapy_amount is None:
    #         self.scrapy_amount = len(self.scrapy_class_time_duration_list)
    #     g_logger.debug("There are totally %d scrapy times" % self.scrapy_amount)
    #     return self.scrapy_amount


    # def count_scrapy_progress(self):
    #     if self.scrapy_amount is None:
    #         raise ValueError("self.scrapy_amount shoudl NOT be None")
    #     return (float(self.scrapy_class_type_progress_count) / self.scrapy_amount * 100.0)


    # @property
    # def ScrapyAmount(self):
    #     return self.count_scrapy_amount()


    # @property
    # def ScrapyProgress(self):
    #     return self.count_scrapy_progress()
