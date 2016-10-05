# -*- coding: utf8 -*-

import os
import sys
import time
import requests
from datetime import datetime
from abc import ABCMeta, abstractmethod
import libs.common as CMN
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyMgrBase(object):

    __metaclass__ = ABCMeta
    def __init__(self):
        self.xcfg = {
            "reserve_old_finance_folder": False,
            "try_to_scrap_all": True,
            "dry_run_only": False
        }
        # self.xcfg.update(kwargs)
        self.source_type_time_duration_list = None


    @classmethod
    def __import_module(cls, module_folder, module_name):
        # import pdb; pdb.set_trace()
        module_path = "%s/%s" % (CMN.DEF.DEF_PROJECT_LIB_FOLDERPATH, module_folder)
        sys.path.insert(0, module_path)
        module_file = '%s/%s.py' % (module_path, module_name)
        assert os.path.exists(module_file), "module file does not exist: %s" % module_file
        try:
            module = __import__(module_name)
            if module:
                # print 'Import file: %s.py (%s)' % (module_name, module)
                return reload(module)
        except Exception:
            msg = 'Import template file failure: %s.py' % module_name
            raise Exception(msg)


    @classmethod
    def __get_class_for_name(cls, module_folder, module_name, class_name):
        # import pdb; pdb.set_trace()
        m = cls.__import_module(module_folder, module_name)
        parts = module_name.split('.')
        parts.append(class_name)
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m


    @classmethod
    def __instantiate_web_scrapy_object(cls, source_type_index, **kwargs):
        # import pdb; pdb.set_trace()
        module_folder = CMN.DEF.DEF_WEB_SCRAPY_MODULE_FOLDER_MAPPING[source_type_index]
        module_name = CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX + CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[source_type_index]
        class_name = CMN.DEF.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[source_type_index]
        g_logger.debug("Try to initiate %s.%s" % (module_name, class_name))
# Find the module
        web_scrapy_class_type = cls.__get_class_for_name(module_folder, module_name, class_name)
# Instantiate the class 
        web_scrapy_class_obj = web_scrapy_class_type(**kwargs)
        return web_scrapy_class_obj


    @classmethod
    def __scrap_web_data_to_csv_file(cls, source_type_index, **kwargs):
        # import pdb; pdb.set_trace()
        web_scrapy_class_obj = cls.__instantiate_web_scrapy_object(source_type_index, **kwargs)
        if web_scrapy_class_obj is None:
            raise RuntimeError("Fail to allocate WebScrapyBase derived class")
        g_logger.debug("Start to scrap %s......", web_scrapy_class_obj.get_description())
        web_scrapy_class_obj.scrap_web_to_csv()


    def _scrap_data(self, reserve_old_finance_folder):
        if not self.xcfg["reserve_old_finance_folder"]:
            self._remove_old_finance_folder()
        self._create_finance_folder_if_not_exist()
        total_errmsg = ""
        # import pdb; pdb.set_trace()
        for source_type_time_duration in self.source_type_time_duration_list:
            try:
                scrapy_obj_cfg = {
                    "time_duration_type": source_type_time_duration.time_duration_type,  
                    "time_duration_start": source_type_time_duration.time_duration_start, 
                    "time_duration_end": source_type_time_duration.time_duration_end,
                    "dry_run_only": self.xcfg["dry_run_only"],
                }
                self.__scrap_web_data_to_csv_file(source_type_time_duration.source_type_index, **scrapy_obj_cfg)
            except Exception as e:
                errmsg = u"Scraping %s fails, due to: %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_time_duration.source_type_index], str(e))
                g_logger.error(errmsg)
                total_errmsg += errmsg
                print total_errmsg
                if not self.xcfg["try_to_scrap_all"]:
                    break
        if total_errmsg:
            RuntimeError(total_errmsg)


    @classmethod
    def _get_csv_filename_from_filepath(cls, csv_filename):
        return csv_filepath.rsplit("/", 1)[-1]


    @classmethod
    def do_scrapy_debug(cls, source_type_index, silent_mode=False):
        # import pdb; pdb.set_trace()
        web_scrapy_class_obj = WebSracpyMgrBase.__instantiate_web_scrapy_object(source_type_index)
        web_scrapy_class_obj.do_debug(silent_mode)


    def __check_source_type_in_correct_finance_mode(self):
        g_logger.debug("************* Source Type Time Range *************")
        for source_type_time_duration in self.source_type_time_duration_list:
            if not CMN.FUNC.check_source_type_index_in_range(source_type_time_duration.source_type_index):
                raise ValueError("The source type index[%d] is NOT in %s mode" % (source_type_time_duration.source_type_index, CMN.DEF.FINANCE_MODE_DESCRIPTION[CMN.DEF.FINANCE_MODE]))
            g_logger.debug("[%s] %s-%s" % (
                CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_time_duration.source_type_index], 
                source_type_time_duration.time_duration_start, 
                source_type_time_duration.time_duration_end
                )
            )
        g_logger.debug("**************************************************")


    def set_source_type_time_duration_from_file(self, filename, time_duration_type):
        # import pdb; pdb.set_trace()
        self.source_type_time_duration_list = CMN.FUNC.parse_source_type_time_duration_config_file(filename, time_duration_type)
        self.__check_source_type_in_correct_finance_mode()


    def set_source_type_time_duration(self, source_type_index_list, time_duration_type, time_duration_start, time_duration_end):
        if source_type_index_list is None:
            source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
        self.source_type_time_duration_list = []
        for source_type_index in source_type_index_list:
            self.source_type_time_duration_list.append(
                CMN.CLS.SourceTypeTimeRangeTuple(source_type_index, time_duration_type, time_duration_start, time_duration_end)
            )
        self.__check_source_type_in_correct_finance_mode()


    def need_reserve_old_finance_folder(self, enable):
        self.xcfg["reserve_old_finance_folder"] = enable


    def enable_dry_run(self, enable):
        self.xcfg["dry_run_only"] = enable

#     def initialize(**kwargs):
#         # import pdb; pdb.set_trace()
# # Determine the source type method first
#         if kwargs.get("source_type_method", None) is not None:
#             self.xcfg["source_type_method"] = kwargs["source_type_method"]
#         else: # Set the default value if it is None
#             self.xcfg["source_type_method"] = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX
#         try:
#             method_index = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index(method)
#         except ValueError as e:
#             g_logger.error("Unsupported method: %s" % method)
#             raise e

#         source_type_time_duration_list = None
#         if method_index != CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX:
#             conf_filename = CMN.DEF.DEF_TODAY_CONFIG_FILENAME if method_index == CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX else CMN.DEF.DEF_HISTORY_CONFIG_FILENAME
#             source_type_time_duration_list = CMN.FUNC.parse_source_type_time_duration_config_file(conf_filename)
#             if source_type_time_duration_list is None:
#                 errmsg = "Fail to parse the config file: %s" % conf_filename
#                 g_logger.error(errmsg)
#                 raise ValueError(errmsg)
#         else:
#             if kwargs.get("source_type_index_list", None) is not None:
#                 self.xcfg["source_type_index_list"] = kwargs["source_type_index_list"]
#             else:
#                 self.xcfg["source_type_index_list"] = []
#                 if CMN.DEF.IS_FINANCE_MARKET_MODE: # Set all market soure type as the default value if it is None
#                     for index in range(CMN.DEF.DEF_DATA_SOURCE_MARKET_START, CMN.DEF.DEF_DATA_SOURCE_MARKET_END):
#                         self.xcfg["source_type_index_list"].append(index)
#                 elif CMN.DEF.IS_FINANCE_STOCK_MODE: # Set all stock soure type as the default value if it is None
#                     for index in range(CMN.DEF.DEF_DATA_SOURCE_STOCK_START, CMN.DEF.DEF_DATA_SOURCE_STOCK_END):
#                         self.xcfg["source_type_index_list"].append(index)

#         if kwargs.get("time_duration_start", None) is not None:
#             self.xcfg["time_duration_start"] = kwargs["time_duration_start"]
#         else:
#             self.xcfg["time_duration_start"] = None
#         if kwargs.get("time_duration_end", None) is not None:
#             self.xcfg["time_duration_end"] = kwargs["time_duration_end"]
#         else:
#             self.xcfg["time_duration_end"] = None
#         for source_type_index in self.xcfg["source_type_index_list"]:
#             source_type_time_duration_list.append(
#                 CMN.CLS.SourceTypeTimeRangeTuple(source_type_index, time_duration_start, time_duration_end)
#             )


    @abstractmethod
    def _create_finance_folder_if_not_exist(cls):
        """IMPORTANT: This is a class method, override it with @classmethod !"""
        raise NotImplementedError


    @abstractmethod
    def _remove_old_finance_folder(cls):
        """IMPORTANT: This is a class method, override it with @classmethod !"""
        raise NotImplementedError


    @abstractmethod
    def do_scrapy(self):
       raise NotImplementedError


    @abstractmethod
    def check_scrapy(self):
        raise NotImplementedError
