# -*- coding: utf8 -*-

import os
import sys
import time
from datetime import datetime
import common as CMN
import web_scrapy_thread
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyMgr(object):

    def __init__(self):
        self.max_concurrent_thread_amount = 4
        self.sleep_interval_for_each_loop = 10
        self.thread_pool_list = []


    def __import_module(self, name):
        current_path = os.path.dirname(os.path.realpath(__file__))
        sys.path.insert(0, current_path)
        module_file = '%s/%s.py' % (current_path, name)
        assert os.path.exists(module_file), "module file does not exist: %s" % module_file
        try:
            module = __import__(name)
            if module:
                print 'Import file: %s.py (%s)' % (name, module)
                return reload(module)
        except Exception:
            msg = 'Import template file failure: %s.py' % name
            raise Exception(msg)


    def __get_class_for_name(self, module_name, class_name):
        # import pdb; pdb.set_trace()
        m = self.__import_module(module_name)
        parts = module_name.split('.')
        parts.append(class_name)
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m


    def __create_web_scrapy_object(self, module_name, class_name, datetime_range_start=None, datetime_range_end=None):
        web_scrapy_class_type = self.__get_class_for_name(module_name, class_name)
        web_scrapy_class_obj = web_scrapy_class_type(datetime_range_start, datetime_range_end)
        return web_scrapy_class_obj


    def __do_scrapy(self, module_name, class_name, datetime_range_start, datetime_range_end):
        datetime_range_list = CMN.get_datetime_range_by_month_list(datetime_range_start, datetime_range_end)
        for datetime_range in datetime_range_list:
            web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name, datetime_range['start'], datetime_range['end'])
            if web_scrapy_class_obj is None:
                raise RuntimeError("Fail to allocate WebScrapyBase derived class")
            g_logger.debug("Start to scrap %s......", web_scrapy_class_obj.get_description())
            web_scrapy_class_obj.scrap_web_to_csv()


    def __do_scrapy_by_multithread(self, module_name, class_name, datetime_range_start, datetime_range_end):  
        datetime_range_list = CMN.get_datetime_range_by_month_list(datetime_range_start, datetime_range_end)
        datetime_range_list_len = len(datetime_range_list)
        start_index = 0
        end_index = 0
        # import pdb; pdb.set_trace()
        thread_list = []
        all_trigger = False
        check_dead_times = 0
        while True:
            if not all_trigger:
                thread_list_len = len(thread_list)
# There are max max_concurrent_thread_amount cocurrent threads simultaneously
                if thread_list_len < self.max_concurrent_thread_amount:
                    new_concurrent_thread_amount = self.max_concurrent_thread_amount - len(thread_list)
                    end_index = start_index + new_concurrent_thread_amount
# Initiate a list of thread classes to wrap the object
                    for datetime_range in datetime_range_list[start_index : end_index]:
                        web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name, datetime_range['start'], datetime_range['end'])
# Start the thread for scraping web data
                        web_scrapy_thread_obj = web_scrapy_thread.WebScrapyThread(web_scrapy_class_obj)
                        g_logger.debug("Start the thread for scraping %s......", web_scrapy_thread_obj)
                        web_scrapy_thread_obj.start()                
                        thread_list.append(web_scrapy_thread_obj)

# Sleep for a while before checking
            time.sleep(self.sleep_interval_for_each_loop)
# Check each worker thread is done
            check_dead_times += 1
            g_logger.debug("Check the thread status in the thread pool......%d", check_dead_times)
            index_to_be_delete_list = [index for index, thread in enumerate(thread_list) if not thread.isAlive()]
            if len(index_to_be_delete_list) != 0:
                index_to_be_delete_list.reverse()
# Remove the dead worker thread
                for index_to_be_delete in index_to_be_delete_list:
                    thread = thread_list[index_to_be_delete]
# Keep track of the error message
                        # self.thread_errmsg_list[thread.index_in_threadpool] = thread.return_errmsg()
                    g_logger.debug("The Thread for scraping %s...... DONE", thread)
                    del thread_list[index_to_be_delete]
# Check if all worker threads are done
            if all_trigger:
                if len(thread_list) == 0:
                    break

            start_index = end_index
            # end_index += self.max_concurrent_thread_amount            
            if end_index > datetime_range_list_len:
                all_trigger = True


#     def __do_scrapy_history_old(self, module_name, class_name, datetime_range_start, datetime_range_end):
#         datetime_range_list = CMN.get_datetime_range_by_month_list(datetime_range_start, datetime_range_end)
#         datetime_range_list_len = len(datetime_range_list)
#         start_index = 0
#         end_index = start_index + self.max_concurrent_thread_amount
#         # import pdb; pdb.set_trace()
#         while True:
#             thread_list = []
# # Initiate a list of thread classes to wrap the object
#             for datetime_range in datetime_range_list[start_index : end_index]:
#                 web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name, datetime_range['start'], datetime_range['end'])
#                 thread_list.append(web_scrapy_thread.WebScrapyThread(web_scrapy_class_obj))
# # Start the thread for scraping web data
#             thread_list_len = len(thread_list)
#             for index in range(thread_list_len):
#                 g_logger.debug("Start the thread for scraping %s......", thread_list[index])
#                 thread_list[index].start()
# # Check each worker thread is done
#             times = 0
#             while True:
#                 time.sleep(self.sleep_interval_for_each_loop)
#                 times += 1
#                 g_logger.debug("Check the thread status in the thread pool......%d", times)
#                 index_to_be_delete_list = [index for index, thread in enumerate(thread_list) if not thread.isAlive()]
#                 if len(index_to_be_delete_list) != 0:
#                     index_to_be_delete_list.reverse()
#                     for index_to_be_delete in index_to_be_delete_list:
#                         thread = thread_list[index_to_be_delete]
# # Keep track of the error message
#                         # self.thread_errmsg_list[thread.index_in_threadpool] = thread.return_errmsg()
#                         g_logger.debug("The Thread for scraping %s...... DONE", thread)
#                         del thread_list[index_to_be_delete]
#                 if len(thread_list) == 0:
#                     break

#             start_index = end_index
#             end_index += self.max_concurrent_thread_amount            
#             if end_index > datetime_range_list_len:
#                 break


    def do_scrapy(self, config_list, multi_thread=False):
        for config in config_list:
            try:
                module_name = CMN.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX + CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[config['index']]
                class_name = CMN.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[config['index']]
                g_logger.debug("Try to initiate %s.%s" % (module_name, class_name))
                if not multi_thread:
                    self.__do_scrapy(module_name, class_name, config['start'], config['end'])
                else:
                    self.__do_scrapy_by_multithread(module_name, class_name, config['start'], config['end'])
                # if config['start'] is None and config['end'] is None:
                #     self.__do_scrapy_today(module_name, class_name)
                # else:
                #     self.__do_scrapy_history(module_name, class_name, config['start'], config['end'])
            except Exception as e:
                g_logger.error("Error occur while scraping %s data, due to: %s" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], str(e)))
