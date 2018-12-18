# -*- coding: utf8 -*-

import os
import sys
import time
import requests
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyMgr(object):

    def __init__(self):
        self.MAX_CONCURRENT_THREAD_AMOUNT = 8
        self.SLEEP_INTERVAL_FOR_EACH_LOOP = 6

        self.thread_pool_list = []
        # self.retry_config_list = []

        # self.workday_canlendar = WorkdayCanlendar.WebScrapyWorkdayCanlendar.Instance()


    def __import_module(self, module_folder, module_name):
        # import pdb; pdb.set_trace()
        current_path = os.path.dirname(os.path.realpath(__file__))
        module_path = "%s/%s" % (current_path, module_folder)
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


    def __get_class_for_name(self, module_folder, module_name, class_name):
        # import pdb; pdb.set_trace()
        m = self.__import_module(module_folder, module_name)
        parts = module_name.split('.')
        parts.append(class_name)
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m


    def __instantiate_web_scrapy_object(self, module_folder, module_name, class_name, **kwargs):
# Find the module
        web_scrapy_class_type = self.__get_class_for_name(module_folder, module_name, class_name)
# Instantiate the class 
        web_scrapy_class_obj = web_scrapy_class_type(**kwargs)
        return web_scrapy_class_obj


    def __do_scrapy(self, module_folder, module_name, class_name, **kwargs):
        web_scrapy_class_obj = self.__instantiate_web_scrapy_object(module_folder, module_name, class_name, **kwargs)
        if web_scrapy_class_obj is None:
            raise RuntimeError("Fail to allocate WebScrapyBase derived class")
        g_logger.debug("Start to scrap %s......", web_scrapy_class_obj.get_description())
        # import pdb; pdb.set_trace()
        web_scrapy_class_obj.scrape_web_to_csv()

#         datetime_range_list = CMN.get_datetime_range_by_month_list(time_start, time_end)
#         #import pdb; pdb.set_trace()
#         for datetime_range in datetime_range_list:
#             web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name, datetime_range['start'], datetime_range['end'])
#             if web_scrapy_class_obj is None:
#                 raise RuntimeError("Fail to allocate WebScrapyBase derived class")
#             g_logger.debug("Start to scrap %s......", web_scrapy_class_obj.get_description())
#             try:
#                 web_scrapy_class_obj.scrape_web_to_csv()
#             except requests.exceptions.Timeout as e:
# # Fail to scrap the web data, put it to the re-try list
#                 g_logger.warn("Put %s in the re-try list" % web_scrapy_class_obj.get_description())
#                 self.retry_config_list.append(
#                     {
#                         'index': CMN.DEF.SCRAPY_MODULE_NAME_MAPPING,
#                         'start': time_start,
#                         'end': time_end,
#                     }
#                 )


#     def __do_scrapy_by_multithread(self, module_name, class_name, time_start, time_end):  
#         datetime_range_list = CMN.get_datetime_range_by_month_list(time_start, time_end)
#         datetime_range_list_len = len(datetime_range_list)
#         start_index = 0
#         end_index = 0
#         # import pdb; pdb.set_trace()
#         thread_list = []
#         all_trigger = False
#         check_dead_times = 0
#         while True:
#             if not all_trigger:
#                 thread_list_len = len(thread_list)
# # There are max max_concurrent_thread_amount cocurrent threads simultaneously
#                 if thread_list_len < self.MAX_CONCURRENT_THREAD_AMOUNT:
#                     new_concurrent_thread_amount = self.MAX_CONCURRENT_THREAD_AMOUNT - len(thread_list)
#                     end_index = start_index + new_concurrent_thread_amount
# # Initiate a list of thread classes to wrap the object
#                     for datetime_range in datetime_range_list[start_index : end_index]:
#                         web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name, datetime_range['start'], datetime_range['end'])
# # Start the thread for scraping web data
#                         web_scrapy_thread_obj = web_scrapy_thread.WebScrapyThread(web_scrapy_class_obj)
#                         g_logger.debug("Start the thread for scraping %s......", web_scrapy_thread_obj)
#                         web_scrapy_thread_obj.start()                
#                         thread_list.append(web_scrapy_thread_obj)

# # Sleep for a while before checking
#             time.sleep(self.SLEEP_INTERVAL_FOR_EACH_LOOP)
# # Check each worker thread is done
#             check_dead_times += 1
#             g_logger.debug("Check the thread status in the thread pool......%d", check_dead_times)
#             index_to_be_delete_list = [index for index, thread in enumerate(thread_list) if not thread.isAlive()]
#             index_to_be_delete_list_len = len(index_to_be_delete_list)
#             if index_to_be_delete_list_len != 0:
#                 g_logger.debug("Delete %d useless thread(s)", index_to_be_delete_list_len)
#                 index_to_be_delete_list.reverse()
# # Remove the dead worker thread
#                 for index_to_be_delete in index_to_be_delete_list:
#                     thread = thread_list[index_to_be_delete]
#                     if CMN.check_failure(thread.get_result()):
#                         web_scrapy_obj = thread.get_obj()
# # Fail to scrap the web data, put it to the re-try list
#                         g_logger.warn("Put %s in the re-try list" % web_scrapy_obj.get_description())
#                         self.retry_config_list.append(
#                             {
#                                 'index': web_scrapy_obj.get_data_source_index(),
#                                 'start': web_scrapy_obj.get_datetime_startday(),
#                                 'end': web_scrapy_obj.get_datetime_endday(),
#                             }
#                         )
# # Keep track of the error message
#                         # self.thread_errmsg_list[thread.index_in_threadpool] = thread.return_errmsg()
#                     g_logger.debug("The Thread for scraping %s...... DONE", thread)
#                     del thread_list[index_to_be_delete]
#             start_index = end_index          
#             if end_index > datetime_range_list_len and not all_trigger:
#                 all_trigger = True
#                 g_logger.debug("All threads are TRIGGERED")
# # Check if all worker threads are done
#             if all_trigger:
#                 if len(thread_list) == 0:
#                     g_logger.debug("All threads are DONE")
#                     break

    # def __assemble_csv_filepath(self, datetime_cfg, data_source_index):
    #     if CMN.DEF.DATA_SOURCE_WRITE2CSV_METHOD[data_source_index] == CMN.WRITE2CSV_ONE_MONTH_PER_FILE:
    #         file_name = CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%04d%02d.csv" % (datetime_cfg.year, datetime_cfg.month)
    #         file_path = CMN.DEF.CSV_FILE_PATH + "/" + file_name
    #     elif CMN.DEF.DATA_SOURCE_WRITE2CSV_METHOD[data_source_index] == CMN.WRITE2CSV_ONE_DAY_PER_FILE:
    #         folder_name = CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%04d%02d" % (datetime_cfg.year, datetime_cfg.month)
    #         file_name = (CMN.DATE_STRING_FORMAT + ".csv") % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)
    #         file_path = CMN.DEF.CSV_FILE_PATH + "/" + folder_name + "/" + file_name
    #     else:
    #         raise RuntimeError("Unknown data source index: %d" % data_source_index)
    #     return (file_path, file_name)


    def check_scrapy(self, config_list):
        raise RuntimeError("TBD")
        # import pdb; pdb.set_trace()
#         file_not_found_list = []
#         file_is_empty_list = []
#         for config in config_list:
#             data_source_index = config['index']
#             datetime_range_list = CMN.get_datetime_range_by_month_list(config['start'], config['end'])
#             for datetime_range in datetime_range_list:
#                 (file_path, file_name) = self.__assemble_csv_filepath(datetime_range['start'], data_source_index)
#                 # file_name = CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%04d%02d.csv" % (datetime_range['start'].year, datetime_range['start'].month)
#                 # file_path = CMN.DEF.CSV_FILE_PATH + "/" + file_name
# # Check if the file exists
#                 if not os.path.exists(file_path):
#                     file_not_found_list.append(
#                         {
#                             'index': data_source_index,
#                             'start': datetime_range['start'],
#                             'end': datetime_range['end'],
#                             'filename': file_name,
#                         }
#                     )
#                 elif os.path.getsize(file_path) == 0:
#                     file_is_empty_list.append(
#                         {
#                             'index': data_source_index,
#                             'start': datetime_range['start'],
#                             'end': datetime_range['end'],
#                             'filename': file_name,
#                         }
#                     )

#         return (file_not_found_list, file_is_empty_list)


    def do_scrapy(self, config_list, multi_thread=False, try_to_run_all=True):
        error_occur = False
        # import pdb; pdb.set_trace()
        for config in config_list:
            try:
                module_folder = CMN.DEF.SCRAPY_MODULE_FOLDER_MAPPING[config['index']]
                module_name = CMN.DEF.SCRAPY_MODULE_NAME_PREFIX + CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[config['index']]
                class_name = CMN.DEF.SCRAPY_CLASS_NAME_MAPPING[config['index']]
                g_logger.debug("Try to initiate %s.%s" % (module_name, class_name))
                if not multi_thread:
                    scrapy_obj_cfg = {"time_start": config['start'], "time_end": config['end']}
                    self.__do_scrapy(module_folder, module_name, class_name, **scrapy_obj_cfg)
                else:
                    raise ValueError("Multi-thread mode is NOT supported")
                    # self.__do_scrapy_by_multithread(module_name, class_name, config['start'], config['end'])
            except Exception as e:
                errmsg = u"Error occur while scraping %s data, due to: %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[config['index']], str(e))
                g_logger.error(errmsg)
                error_occur = True
                if not try_to_run_all:
                    raise e
        if error_occur:
            raise RuntimeError("Fail to scrapy web data") 

# # Retry to scrap the web data if error occurs
#         if need_retry and len(self.retry_config_list) != 0:
#             g_logger.warn("Retry to scrap the web data due to some errors.......")
#             for retry_config in self.retry_config_list:
#                 try:
#                     module_name = CMN.DEF.SCRAPY_MODULE_NAME_PREFIX + CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[retry_config['index']]
#                     class_name = CMN.DEF.SCRAPY_CLASS_NAME_MAPPING[retry_config['index']]
#                     g_logger.debug("Re-Try to initiate %s.%s" % (module_name, class_name))
#                     self.__do_scrapy(module_name, class_name, retry_config['start'], retry_config['end'])
#                 except Exception as e:
#                     g_logger.error("Error occur while ReTrying to scrap %s data, due to: %s" % (CMN.DEF.SCRAPY_CLASS_DESCRIPTION[retry_config['index']], str(e)))


    def do_debug(self, data_source_index):
        module_name = CMN.DEF.SCRAPY_MODULE_NAME_PREFIX + CMN.DEF.SCRAPY_MODULE_NAME_MAPPING[data_source_index]
        class_name = CMN.DEF.SCRAPY_CLASS_NAME_MAPPING[data_source_index]
        g_logger.debug("Try to initiate %s.%s for debugging......" % (module_name, class_name))
        web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name)
        web_scrapy_class_obj.do_debug()
