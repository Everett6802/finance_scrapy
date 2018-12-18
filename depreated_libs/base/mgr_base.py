# -*- coding: utf8 -*-

import os
import sys
import time
import requests
import threading
import collections
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod
import worker_thread_pool as ThreadPool
import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
import scrapy_configurer as Configurer
g_logger = CMN.LOG.get_logger()


class MgrBase(object):

    SHOW_PROGRESS_INTERVAL = 30

    __metaclass__ = ABCMeta
    def __init__(self, **cfg):
        self.xcfg = {
            "reserve_old_finance_folder": False,
            "disable_flush_scrapy_while_exception": False,
            "try_to_scrape_all": True,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "multi_thread_amount": None,
            "show_progress": False,
            "need_estimate_complete_time": True,
            "start_estimate_complete_time_threshold": 2,
            "disable_output_missing_csv": False,
            # "csv_time_duration_table": None
        }
        self.xcfg.update(cfg)
        self.scrapy_class_time_duration_list = None
        # self.multi_thread_amount = None
        # self.xcfg["show_progress"] = False
        self.scrapy_class_type_progress_amount = None
        self.scrapy_class_type_progress_count = 0
        self.web_scrapy_obj_list = None
        self.web_scrapy_obj_list_thread_lock = threading.Lock()
        self.web_scrapy_start_datetime = None
        self.csv_file_check_status_record_string_dict = None
        self.csv_file_check_status_record_description_dict = None
        self.finance_mode = None
        self.configurer = None


    def __append_no_scrapy_csv_file(self, web_scrapy_obj):
        # import pdb; pdb.set_trace()
# Collect the CSV file status record from each web scrapy object
        for index in range(web_scrapy_obj.CSVFileNoScrapyTypeSize):
            no_scrapy_type = web_scrapy_obj.CSVFileNoScrapyTypeList[index]
            if not web_scrapy_obj.CSVFileNoScrapyDescriptionDict.has_key(no_scrapy_type):
                continue
            if len(web_scrapy_obj.CSVFileNoScrapyDescriptionDict[no_scrapy_type]) == 0:
                continue
# Initialize the OrderedDict for keeping track of CSV files
            if self.csv_file_check_status_record_string_dict is None:
                self.csv_file_check_status_record_string_dict = collections.OrderedDict()
                self.csv_file_check_status_record_description_dict = collections.OrderedDict()
                for index_for_init in range(web_scrapy_obj.CSVFileNoScrapyTypeSize):
                    no_scrapy_type_for_init = web_scrapy_obj.CSVFileNoScrapyTypeList[index_for_init]
                    self.csv_file_check_status_record_string_dict[no_scrapy_type_for_init] = []
                    self.csv_file_check_status_record_description_dict[no_scrapy_type_for_init] = web_scrapy_obj.CSVFileNoScrapyTypeDescriptionList[index_for_init]
# Keep track of the data of each web scrapy object       
            self.csv_file_check_status_record_string_dict[no_scrapy_type].extend(web_scrapy_obj.CSVFileNoScrapyDescriptionDict[no_scrapy_type])


    def _get_configurer(self):
        if self.configurer is None:
            self.configurer = Configurer.ScrapyConfigurer.Instance()
        return self.configurer


    # def __get_finance_folderpath_format(self, finance_root_folderpath=None):
    def _get_finance_root_folderpath(self, finance_root_folderpath=None):
        folderpath = finance_root_folderpath
        if folderpath is None:
            folderpath = self.xcfg["finance_root_folderpath"]
        if folderpath is None:
            folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH
        # return ("%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_STOCK_FOLDERNAME)) + "%02d"
        return folderpath


    def _scrape_web_data_to_csv_file(self, scrapy_class_index, **kwargs):
        # import pdb; pdb.set_trace()
        web_scrapy_obj = CMN.FUNC.instantiate_web_scrapy_object(scrapy_class_index, **kwargs)
        if web_scrapy_obj is None:
            raise RuntimeError("Fail to allocate ScrapyBase derived class")
        with self.web_scrapy_obj_list_thread_lock:
            self.web_scrapy_obj_list = []
            self.web_scrapy_obj_list.append(web_scrapy_obj)
        g_logger.debug("Start to scrape %s......", web_scrapy_obj.get_description())
        web_scrapy_obj.scrape_web_to_csv()
# Keep track of the CSV file status
        self.__append_no_scrapy_csv_file(web_scrapy_obj)
# Update the new CSV time duration
        self._update_new_csv_time_duration(web_scrapy_obj)
        with self.web_scrapy_obj_list_thread_lock:
            self.web_scrapy_obj_list = None


    def _multi_thread_scrape_web_data_to_csv_file(self, scrapy_class_index, scrapy_obj_cfg_list):
        # import pdb; pdb.set_trace()
# Start the thread to scrap data
        thread_pool = ThreadPool.WorkerThreadPool(self.xcfg["multi_thread_amount"])
        for index in range(len(scrapy_obj_cfg_list)):
            with self.web_scrapy_obj_list_thread_lock:
                if self.web_scrapy_obj_list is None:
                    self.web_scrapy_obj_list = []
                web_scrapy_obj = CMN.FUNC.instantiate_web_scrapy_object(scrapy_class_index, **(scrapy_obj_cfg_list[index]))
                self.web_scrapy_obj_list.append(web_scrapy_obj)
                # time.sleep(3)
            g_logger.debug("Start to scrape %s...... %d" % (web_scrapy_obj.get_description(), index))
            thread_pool.add_scrape_web_to_csv_task(web_scrapy_obj)
        thread_pool.wait_completion()
        for web_scrapy_obj in self.web_scrapy_obj_list:
# Keep track of the CSV file status
            self.__append_no_scrapy_csv_file(web_scrapy_obj)
# Update the new CSV time duration
            self._update_new_csv_time_duration(web_scrapy_obj)
        with self.web_scrapy_obj_list_thread_lock:
            self.web_scrapy_obj_list = None


    def _init_cfg_for_scrapy_obj(self, scrapy_class_time_duration):
        scrapy_obj_cfg = {
            "time_duration_type": scrapy_class_time_duration.time_duration_type,  
            "time_duration_start": scrapy_class_time_duration.time_duration_start, 
            "time_duration_end": scrapy_class_time_duration.time_duration_end,
            "dry_run_only": self.xcfg["dry_run_only"],
            "finance_root_folderpath": self.xcfg["finance_root_folderpath"]
            }
        return scrapy_obj_cfg


    def do_scrapy(self):
        # import pdb; pdb.set_trace()
        self._init_csv_time_duration()
        if not self.xcfg["reserve_old_finance_folder"]:
            self._remove_old_finance_folder()
        else:
            self._read_old_csv_time_duration()
        self._create_finance_folder_if_not_exist()
        total_errmsg = ""
        # import pdb; pdb.set_trace()
        self.scrapy_class_type_progress_count = 0
        show_progress_timer_thread = None
        if self.xcfg["show_progress"]:
            self.web_scrapy_start_datetime = datetime.now()
            # self.count_scrapy_amount()
            self.scrapy_class_type_progress_amount = len(self.scrapy_class_time_duration_list)
            # progress_string = "Total Scrapy Times: %d" % self.ScrapyAmount
            # g_logger.info(progress_string)
            # if CMN.DEF.CAN_PRINT_CONSOLE:
            #     print progress_string
            show_progress_timer_thread = CMN.CLS.FinanceTimerThread(interval = self.SHOW_PROGRESS_INTERVAL)
            show_progress_timer_thread.start_timer(MgrBase.show_scrapy_progress, self)
        # import pdb; pdb.set_trace()
        for scrapy_class_time_duration in self.scrapy_class_time_duration_list:
            try:
                self._scrape_class_data(scrapy_class_time_duration)
            except CMN.EXCEPTION.WebScrapyException as e:
                if isinstance(e.message, str):
                    errmsg = "Scraping %s fails, due to: %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], e.message)
                else:
                    errmsg = u"Scraping %s fails, due to: %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], e.message)
                CMN.FUNC.try_print(CMN.FUNC.get_full_stack_traceback())
                g_logger.error(errmsg)
                raise e
            except Exception as e:
                # import pdb; pdb.set_trace()
                if isinstance(e.message, str):
                    errmsg = "Scraping %s fails, due to: %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], e.message)
                else:
                    errmsg = u"Scraping %s fails, due to: %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], e.message)
                CMN.FUNC.try_print(CMN.FUNC.get_full_stack_traceback())
                g_logger.error(errmsg)
                total_errmsg += errmsg
                # print total_errmsg
                if not self.xcfg["try_to_scrape_all"]:
                    break
            # self._increment_scrapy_class_type_progress_count(scrapy_class_time_duration.scrapy_class_index)
            if self.xcfg["show_progress"]:
                with self.web_scrapy_obj_list_thread_lock:
                    self.scrapy_class_type_progress_count += 1
        if self.xcfg["show_progress"]:
            with self.web_scrapy_obj_list_thread_lock:
                show_progress_timer_thread.stop_timer()
        if total_errmsg:
            RuntimeError(total_errmsg)
# Write the new CSV data time range into file
        self._write_new_csv_time_duration()


    def show_no_scrapy(self):
        # import pdb; pdb.set_trace()
        assert (self.csv_file_check_status_record_string_dict is not None), "self.csv_file_check_status_record_string_dict is None" 
        no_scrapy_csv_file_string = ""
        for no_scrapy_type, no_scrapy_csv_file_list in self.csv_file_check_status_record_string_dict.items():
            if len(no_scrapy_csv_file_list) == 0:
                continue
            csv_file_check_status_record_description = self.csv_file_check_status_record_description_dict[no_scrapy_type]
            csv_file_check_status_record_string = "; ".join(no_scrapy_csv_file_list)
            no_scrapy_csv_file_string += "*** %s ***\n%s\n" % (csv_file_check_status_record_description, csv_file_check_status_record_string)
        if len(no_scrapy_csv_file_string) != 0:
            g_logger.info(no_scrapy_csv_file_string)
            if CMN.DEF.CAN_PRINT_CONSOLE:
                print no_scrapy_csv_file_string


    @classmethod
    def do_scrapy_debug(cls, scrapy_class_index, silent_mode=False):
        web_scrapy_class = CMN.FUNC.get_web_scrapy_class(scrapy_class_index)
        web_scrapy_class.do_debug(silent_mode)


    def __check_scrapy_class_in_correct_finance_mode(self):
        # import pdb; pdb.set_trace()
        g_logger.debug("************* Source Type Time Range *************")
        for scrapy_class_time_duration in self.scrapy_class_time_duration_list:
            if not CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_time_duration.scrapy_class_index):
                raise ValueError("The scrapy class index[%d] is NOT in %s mode" % (scrapy_class_time_duration.scrapy_class_index, CMN.DEF.FINANCE_MODE_DESCRIPTION[GV.FINANCE_MODE]))
            g_logger.debug("[%s] %s-%s" % (
                CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], 
                scrapy_class_time_duration.time_duration_start, 
                scrapy_class_time_duration.time_duration_end
                )
            )
        g_logger.debug("**************************************************")


    def set_config_from_file(self):
        # import pdb; pdb.set_trace()
        # time_range_str_list = None
        # if CMN.DEF.FINANCE_MODE == CMN.DEF.FINANCE_MODE_MARKET:
        #     time_range_str_list = self.configurer.get_config("MarketAllTimeRange")
        # elif CMN.DEF.FINANCE_MODE == CMN.DEF.FINANCE_MODE_STOCK:
        #     time_range_str_list = self.configurer.get_config("StockAllTimeRange")
        # else:
        #     raise ValueError("Unknown finance mode: %d" % CMN.DEF.FINANCE_MODE)
        method_index_list = self._get_configurer().Method
        time_duration_type = self._get_configurer().TimeType
        (time_duration_start, time_duration_end) = self._get_configurer().TimeDurationRange
        self.set_method_time_duration(method_index_list, time_duration_type, time_duration_start, time_duration_end)
        # method_time_duration_range_dict = self._get_configurer().MethodTimeDurationRange
        # self.scrapy_class_time_duration_list = []
        # for method_index in method_index_list:
        #     if not method_time_duration_range_dict.has_key(method_index):
        #         raise ValueError("The time duration of method[%s] is NOT defined in the config" % CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index])
        #     class_index_list = CMN.FUNC.get_scrapy_class_index_list_from_method_index(method_index)
        #     for class_index in class_index_list:
        #         self.scrapy_class_time_duration_list.append(
        #             CMN.CLS.ScrapyClassTimeDurationTuple(class_index, time_duration_type, method_time_duration_range_dict[method_index][0], method_time_duration_range_dict[method_index][1])
        #         )
        # # for time_range_str in time_range_str_list:
        # #     param_list = time_range_str.split(' ')
        # #     param_list_len = len(param_list)
        # #     class_index_list = CMN.FUNC.get_scrapy_class_index_list_from_method_description(param_list[0].decode(CMN_DEF.UNICODE_ENCODING_IN_FILE))
        # #     time_duration_start = None
        # #     if param_list_len >= 2:
        # #         time_duration_start = CMN.CLS.FinanceTimeBase.from_string(param_list[1])
        # #     time_duration_end = None
        # #     if param_list_len >= 3:
        # #         time_duration_end = CMN.CLS.FinanceTimeBase.from_string(param_list[2])
        # #     for class_index in class_index_list:
        # #         self.scrapy_class_time_duration_list.append(
        # #             CMN_CLS.ScrapyClassTimeDurationTuple(class_index, time_duration_type, time_duration_start, time_duration_end)
        # #         )
        # self.__check_scrapy_class_in_correct_finance_mode()


    def set_method_time_duration(self, method_index_list, time_duration_type, time_duration_start, time_duration_end):
        class_index_list = None
        if method_index_list is None:
            class_index_list = CMN.FUNC.get_scrapy_class_index_range_list()
        else:
            class_index_list = []
            for method_index in method_index_list:
                class_index_list += CMN.FUNC.get_scrapy_class_index_list_from_method_index(method_index)
        self.scrapy_class_time_duration_list = []
        if time_duration_type == CMN.DEF.DATA_TIME_DURATION_RANGE_ALL:
            method_time_duration_range_dict = self._get_configurer().MethodTimeDurationRange
            for method_index in method_index_list:
                if not method_time_duration_range_dict.has_key(method_index):
                    raise ValueError("The time duration of method[%s] is NOT defined in the config" % CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index])
            for class_index in class_index_list:
                self.scrapy_class_time_duration_list.append(
                    CMN.CLS.ScrapyClassTimeDurationTuple(class_index, time_duration_type, method_time_duration_range_dict[method_index][0], method_time_duration_range_dict[method_index][1])
                )
        else:
            for class_index in class_index_list:
                self.scrapy_class_time_duration_list.append(
                    CMN.CLS.ScrapyClassTimeDurationTuple(class_index, time_duration_type, time_duration_start, time_duration_end)
                )
        self.__check_scrapy_class_in_correct_finance_mode()


    def set_finance_root_folderpath(self, csv_root_folderpath=None, update_dataset=False):
        assert ((csv_root_folderpath is None) or (not update_dataset)), "incorrect arguments in the 'set_finance_root_folderpath' function"
        if update_dataset:
            csv_root_folderpath = GV.FINANCE_DATASET_DATA_FOLDERPATH
        g_logger.debug("Set CSV root folder path: %s" % csv_root_folderpath)
        self.xcfg["finance_root_folderpath"] = csv_root_folderpath


    @property
    def FinanceRootFolderPath(self):
        return self.xcfg["finance_root_folderpath"]


    def reserve_old_finance_folder(self, enable):
        g_logger.debug("Reserve Old Finance Folder: %s" % ("True" if enable else "False"))
        self.xcfg["reserve_old_finance_folder"] = enable


    @property
    def ReserveOld(self):
        return self.xcfg["reserve_old_finance_folder"]


    def disable_flush_scrapy_while_exception(self, disable):
        g_logger.debug("Flush Scrapy Data while Exception Occurs: %s" % ("False" if disable else "True"))
        self.xcfg["disable_flush_scrapy_while_exception"] = disable


    @property
    def DisableFlushScrapy(self):
        return self.xcfg["disable_flush_scrapy_while_exception"]


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
        if self.xcfg["reserve_old_finance_folder"]:
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


    # @property
    # def EmptyWebDataFound(self):
    #     if self.emtpy_web_data_list is None:
    #         raise ValueError("self.emtpy_web_data_list should NOT be None")
    #     return True if len(self.emtpy_web_data_list) != 0 else False


    # @property
    # def EmptyWebDataList(self):
    #     if self.emtpy_web_data_list is None:
    #         raise ValueError("self.emtpy_web_data_list should NOT be None")
    #     return self.emtpy_web_data_list


    @property
    def NoScrapyCSVFound(self):
        return True if self.csv_file_check_status_record_string_dict is not None else False


    @property
    def FinanceMode(self):
        assert self.finance_mode is not None, "finance_mode should NOT be None"
        return self.finance_mode


    @property
    def ScrapyProgress(self):
        scrapy_progress = 0
        with self.web_scrapy_obj_list_thread_lock:
            if self.web_scrapy_obj_list is not None:
                progress_ratio_sum = 0.0
                for web_scrapy_obj in self.web_scrapy_obj_list:
                    # print "ProgressRatio: %f" % web_scrapy_obj.ProgressRatio
                    progress_ratio_sum += web_scrapy_obj.ProgressRatio
                scrapy_progress = (self.scrapy_class_type_progress_count + progress_ratio_sum / len(self.web_scrapy_obj_list)) / self.scrapy_class_type_progress_amount
        return scrapy_progress


    @staticmethod
    def show_scrapy_progress(mgr_obj):
        if not isinstance(mgr_obj, MgrBase):
            raise AttributeError("mgr_obj should be the MgrBase instance")
        scrapy_progress = mgr_obj.ScrapyProgress
        # with mgr_obj.web_scrapy_obj_list_thread_lock:
        #     progress_ratio_sum = 0.0
        #     for web_scrapy_obj in mgr_obj.web_scrapy_obj_list:
        #         progress_ratio_sum += web_scrapy_obj.ProgressRatio
        #     scrapy_progress = (mgr_obj.scrapy_class_type_progress_count + progress_ratio_sum / len(mgr_obj.web_scrapy_obj_list)) / mgr_obj.scrapy_class_type_progress_amount
        progress_string = "[%s] Progress................... %03.1f%%" % (datetime.strftime(datetime.now(), '%H:%M:%S'), scrapy_progress * 100.0)
        if scrapy_progress >= mgr_obj.StartEstimateCompleteTimeThreshold:
            progress_string += "  *Estimated Complete Time: %s" % mgr_obj.estimate_complete_time(scrapy_progress)
        g_logger.info(progress_string)
        if CMN.DEF.CAN_PRINT_CONSOLE:
            print progress_string


    # def _increment_scrapy_class_type_progress_count(self, scrapy_class_index):
    #     raise NotImplementedError


    @abstractmethod
    def merge_finance_folder(self, finance_folderpath_src_list, finance_folderpath_dst):
        # """IMPORTANT: This is a class method, override it with @classmethod !"""
        raise NotImplementedError


    # @abstractmethod
    # def _get_configurer(self):
    #     raise NotImplementedError


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
    def _scrape_class_data(self, scrapy_class_time_duration):
        raise NotImplementedError


    # @abstractmethod
    # def check_scrapy(self):
    #     raise NotImplementedError


    # @abstractmethod
    # def check_scrapy_to_string(self):
    #     raise NotImplementedError


    # @abstractmethod
    # def enable_multithread(self, thread_amount):
    #     raise NotImplementedError


    # @abstractmethod
    # def count_scrapy_amount(self):
    #     raise NotImplementedError


    # @abstractmethod
    # def count_scrapy_progress(self):
    #     raise NotImplementedError
