# -*- coding: utf8 -*-

import os
import sys
import time
import requests
import threading
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod
import libs.common as CMN
import web_scrapy_thread_pool as ThreadPool
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebSracpyMgrBase(object):

    __metaclass__ = ABCMeta
    def __init__(self, **cfg):
        self.xcfg = {
            "old_finance_folder_reservation": False,
            "try_to_scrap_all": True,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.DEF_CSV_ROOT_FOLDERPATH,
            "multi_thread_amount": None,
            "show_progress": False,
            "need_estimate_complete_time": True,
            "start_estimate_complete_time_threshold": 2,
            "disable_output_missing_csv": False,
            # "csv_time_duration_table": None
        }
        self.xcfg.update(cfg)
        self.source_type_time_duration_list = None
        # self.multi_thread_amount = None
        # self.xcfg["show_progress"] = False
        self.scrapy_amount = None
        self.scrapy_source_type_progress_count = 0
        self.web_scrapy_obj_list = None
        self.web_scrapy_obj_list_thread_lock = threading.Lock()
        self.web_scrapy_start_datetime = None
        self.emtpy_web_data_list = None


    def _scrap_web_data_to_csv_file(self, source_type_index, **kwargs):
        # import pdb; pdb.set_trace()
        web_scrapy_obj = CMN.FUNC.instantiate_web_scrapy_object(source_type_index, **kwargs)
        if web_scrapy_obj is None:
            raise RuntimeError("Fail to allocate WebScrapyBase derived class")
        with self.web_scrapy_obj_list_thread_lock:
            self.web_scrapy_obj_list = []
            self.web_scrapy_obj_list.append(web_scrapy_obj)
        g_logger.debug("Start to scrap %s......", web_scrapy_obj.get_description())
        web_scrapy_obj.scrap_web_to_csv()
# Update the empty web data list
        if web_scrapy_obj.EmptyWebDataFound:
            self.emtpy_web_data_list.extend(web_scrapy_obj.EmptyWebDataList)
# Update the new CSV time duration
        self._update_new_csv_time_duration(web_scrapy_obj)
        with self.web_scrapy_obj_list_thread_lock:
            self.web_scrapy_obj_list = None


    def _multi_thread_scrap_web_data_to_csv_file(self, source_type_index, scrapy_obj_cfg_list):
        # import pdb; pdb.set_trace()
# Start the thread to scrap data
        thread_pool = ThreadPool.WebScrapyThreadPool(self.xcfg["multi_thread_amount"])
        for index in range(len(scrapy_obj_cfg_list)):
            with self.web_scrapy_obj_list_thread_lock:
                if self.web_scrapy_obj_list is None:
                    self.web_scrapy_obj_list = []
                web_scrapy_obj = CMN.FUNC.instantiate_web_scrapy_object(source_type_index, **(scrapy_obj_cfg_list[index]))
                self.web_scrapy_obj_list.append(web_scrapy_obj)
                # time.sleep(3)
            g_logger.debug("Start to scrap %s...... %d" % (web_scrapy_obj.get_description(), index))
            thread_pool.add_scrap_web_to_csv_task(web_scrapy_obj)
        thread_pool.wait_completion()
        for web_scrapy_obj in self.web_scrapy_obj_list:
# Update the empty web data list
            if web_scrapy_obj.EmptyWebDataFound:
                self.emtpy_web_data_list.extend(web_scrapy_obj.EmptyWebDataList)
# Update the new CSV time duration
            self._update_new_csv_time_duration(web_scrapy_obj)
        with self.web_scrapy_obj_list_thread_lock:
            self.web_scrapy_obj_list = None


    def _init_cfg_for_scrapy_obj(self, source_type_time_duration):
        scrapy_obj_cfg = {
            "time_duration_type": source_type_time_duration.time_duration_type,  
            "time_duration_start": source_type_time_duration.time_duration_start, 
            "time_duration_end": source_type_time_duration.time_duration_end,
            "dry_run_only": self.xcfg["dry_run_only"],
            "finance_root_folderpath": self.xcfg["finance_root_folderpath"]
            }
        return scrapy_obj_cfg


    def do_scrapy(self):
        # import pdb; pdb.set_trace()
        self._init_csv_time_duration()
        if not self.xcfg["old_finance_folder_reservation"]:
            self._remove_old_finance_folder()
        else:
            self._read_old_csv_time_duration()
        self._create_finance_folder_if_not_exist()
        total_errmsg = ""
        # import pdb; pdb.set_trace()
        self.scrapy_source_type_progress_count = 0
        show_progress_timer_thread = None
        if self.xcfg["show_progress"]:
            self.web_scrapy_start_datetime = datetime.now()
            self.count_scrapy_amount()
            progress_string = "Total Scrapy Times: %d" % self.ScrapyAmount
            g_logger.info(progress_string)
            if CMN.DEF.CAN_PRINT_CONSOLE:
                print progress_string
            show_progress_timer_thread = CMN.CLS.FinanceTimerThread(interval=30)
            show_progress_timer_thread.start_timer(WebSracpyMgrBase.show_scrapy_progress, self)
        # import pdb; pdb.set_trace()
        self.emtpy_web_data_list = []
        for source_type_time_duration in self.source_type_time_duration_list:
            try:
                self._scrap_single_source_data(source_type_time_duration)
            except CMN.EXCEPTION.WebScrapyException as e:
                if isinstance(e.message, str):
                    errmsg = "Scraping %s fails, due to: %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_time_duration.source_type_index], e.message)
                else:
                    errmsg = u"Scraping %s fails, due to: %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_time_duration.source_type_index], e.message)
                CMN.FUNC.try_print(CMN.FUNC.get_full_stack_traceback())
                g_logger.error(errmsg)
                raise e
            except Exception as e:
                if isinstance(e.message, str):
                    errmsg = "Scraping %s fails, due to: %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_time_duration.source_type_index], e.message)
                else:
                    errmsg = u"Scraping %s fails, due to: %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_time_duration.source_type_index], e.message)
                CMN.FUNC.try_print(CMN.FUNC.get_full_stack_traceback())
                g_logger.error(errmsg)
                total_errmsg += errmsg
                # print total_errmsg
                if not self.xcfg["try_to_scrap_all"]:
                    break
            self.scrapy_source_type_progress_count += 1
        if self.xcfg["show_progress"]:
            show_progress_timer_thread.stop_timer()
        if total_errmsg:
            RuntimeError(total_errmsg)
# Write the new CSV data time range into file
        self._write_new_csv_time_duration()


    @classmethod
    def do_scrapy_debug(cls, source_type_index, silent_mode=False):
        web_scrapy_class = CMN.FUNC.get_web_scrapy_class(source_type_index)
        web_scrapy_class.do_debug(silent_mode)


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
        self.source_type_time_duration_list = CMN.FUNC.read_source_type_time_duration_config_file(filename, time_duration_type)
        self.__check_source_type_in_correct_finance_mode()


    def set_source_type_time_duration(self, source_type_index_list, time_duration_type, time_duration_start, time_duration_end):
        if source_type_index_list is None:
            source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
        self.source_type_time_duration_list = []
        for source_type_index in source_type_index_list:
            self.source_type_time_duration_list.append(
                CMN.CLS.SourceTypeTimeDurationTuple(source_type_index, time_duration_type, time_duration_start, time_duration_end)
            )
        self.__check_source_type_in_correct_finance_mode()


    def set_finance_root_folderpath(self, csv_root_folderpath):
        g_logger.debug("Set CSV root folder path: %s" % csv_root_folderpath)
        self.xcfg["finance_root_folderpath"] = csv_root_folderpath


    @property
    def FinanceRootFolderPath(self):
        return self.xcfg["finance_root_folderpath"]


    def enable_old_finance_folder_reservation(self, enable):
        g_logger.debug("%s the old finance folder reservation" % ("Enable" if enable else "Disable"))
        self.xcfg["old_finance_folder_reservation"] = enable


    @property
    def OldFinanceFolderReservation(self):
        return self.xcfg["old_finance_folder_reservation"]


    def enable_dry_run(self, enable):
        if enable:
            g_logger.debug("Enable Dry-Run ......")
        self.xcfg["dry_run_only"] = enable


    @property
    def DryRun(self):
        return self.xcfg["dry_run_only"]


    def _create_finance_root_folder_if_not_exist(self, root_folderpath=None):
        if root_folderpath is None:
            root_folderpath = self.xcfg["finance_root_folderpath"]
        g_logger.debug("Try to create new root folder: %s" % root_folderpath)
        CMN.FUNC.create_folder_if_not_exist(root_folderpath)


    def _check_merge_finance_folder_exist(self, finance_folderpath_src_list, finance_folderpath_dst):
        for finance_folderpath_src in finance_folderpath_src_list:
            if not CMN.FUNC.check_file_exist(finance_folderpath_src):
                errmsg = "The source folderpath[%s] to be merged does NOT exist" % finance_folderpath_src
                g_logger.error(errmsg)
                raise ValueError(errmsg)
        # import pdb; pdb.set_trace()
        if self.xcfg["old_finance_folder_reservation"]:
            if CMN.FUNC.check_file_exist(finance_folderpath_dst):
                errmsg = "The destination folderpath[%s] after merging already exist" % finance_folderpath_dst
                g_logger.error(errmsg)
                raise ValueError(errmsg)
        else:
            CMN.FUNC.remove_folder_if_exist(finance_folderpath_dst)


    # def enable_show_progress(self, enable):
    #     self.xcfg["show_progress"] = enable


    def estimate_complete_time(self, cur_progress):
        cur_datetime = datetime.now()
        time_diff_in_seconds = (cur_datetime - self.web_scrapy_start_datetime).total_seconds()
        rest_of_time_in_seconds = int(time_diff_in_seconds * float(100.0 - cur_progress) / cur_progress + 0.5)
        return '{:%H:%M:%S}'.format(cur_datetime + timedelta(seconds=rest_of_time_in_seconds))


    # @property
    # def NeedEstimateCompleteTime(self):
    #     return self.xcfg["need_estimate_complete_time"]


    @property
    def StartEstimateCompleteTimeThreshold(self):
        return self.xcfg["start_estimate_complete_time_threshold"]


    @property
    def EmptyWebDataFound(self):
        if self.emtpy_web_data_list is None:
            raise ValueError("self.emtpy_web_data_list should NOT be None")
        return True if len(self.emtpy_web_data_list) != 0 else False


    @property
    def EmptyWebDataList(self):
        if self.emtpy_web_data_list is None:
            raise ValueError("self.emtpy_web_data_list should NOT be None")
        return self.emtpy_web_data_list


    @staticmethod
    def show_scrapy_progress(obj_handle):
        if not isinstance(obj_handle, WebSracpyMgrBase):
            raise AttributeError("obj_handle should be the WebSracpyMgrBase instance")
        scrapy_progress = obj_handle.ScrapyProgress
        progress_string = "[%s] Progress................... %03.1f" % (datetime.strftime(datetime.now(), '%H:%M:%S'), scrapy_progress)
        if obj_handle.ScrapyProgress >= obj_handle.StartEstimateCompleteTimeThreshold:
            progress_string += "  *Estimated Complete Time: %s" % obj_handle.estimate_complete_time(scrapy_progress)
        g_logger.info(progress_string)
        if CMN.DEF.CAN_PRINT_CONSOLE:
            print progress_string


    @abstractmethod
    def merge_finance_folder(self, finance_folderpath_src_list, finance_folderpath_dst):
        # """IMPORTANT: This is a class method, override it with @classmethod !"""
        raise NotImplementedError


    @abstractmethod
    def _create_finance_folder_if_not_exist(self, finance_root_folderpath=None):
        # """IMPORTANT: This is a class method, override it with @classmethod !"""
        raise NotImplementedError


    @abstractmethod
    def _remove_old_finance_folder(self, finance_root_folderpath=None):
        raise NotImplementedError


    @abstractmethod
    def _init_csv_time_duration(self):
        raise NotImplementedError


    @abstractmethod
    def _read_old_csv_time_duration(self):
        raise NotImplementedError


    @abstractmethod
    def _update_new_csv_time_duration(self, web_scrapy_obj):
        raise NotImplementedError


    @abstractmethod
    def _write_new_csv_time_duration(self):
        raise NotImplementedError


    @abstractmethod
    def _scrap_single_source_data(self, source_type_time_duration):
        raise NotImplementedError


    @abstractmethod
    def check_scrapy(self):
        raise NotImplementedError


    @abstractmethod
    def check_scrapy_to_string(self):
        raise NotImplementedError


    # @abstractmethod
    # def enable_multithread(self, thread_amount):
    #     raise NotImplementedError


    @abstractmethod
    def count_scrapy_amount(self):
        raise NotImplementedError


    @abstractmethod
    def count_scrapy_progress(self):
        raise NotImplementedError
