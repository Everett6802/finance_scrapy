import os
import sys
import time
from datetime import datetime
import common as CMN
import web_scrapy_thread
# import web_scrapy_future_top10_dealers_and_legal_persons
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyMgr(object):

    def __init__(self):
        self.max_concurrent_thread_amount = 4
        self.sleep_interval_for_each_loop = 15
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


    def __do_scrapy_today(self, module_name, class_name):
        web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name)
        if web_scrapy_class_obj is None:
            raise RuntimeError("Fail to allocate WebScrapyBase derived class")
        web_scrapy_class_obj.scrap_web_to_csv()


    def __do_scrapy_history(self, module_name, class_name, datetime_range_start, datetime_range_end):
        datetime_range_list = CMN.get_datetime_range_by_month_list(datetime_range_start, datetime_range_end)
        datetime_range_list_len = len(datetime_range_list)
        start_index = 0
        end_index = start_index + self.max_concurrent_thread_amount
        thread_list = []
        while True:
            for datetime_range in datetime_range_list[start_index : end_index]:
                web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name, datetime_range['start'], datetime_range['end'])
                thread_list.append(web_scrapy_thread.WebScrapyThread(web_scrapy_class_obj))
                thread_list_len = len(thread_list)
                for index in range(thread_list_len):
                    # g_logger.debug("Start the Thread for scraping during %s/%s/%s - %s/%s/%s......" % (
                    #     datetime_range['start'].year,
                    #     datetime_range['start'].month,
                    #     datetime_range['start'].day,
                    #     datetime_range['end'].year,
                    #     datetime_range['end'].month,
                    #     datetime_range['end'].day,
                    #     )
                    # )
                    g_logger.debug("Start the thread for scraping %s......", thread_list[index])
                    thread_list[index].start()
                while True:
                    time.sleep(self.sleep_interval_for_each_loop)

                    g_logger.debug("Check the thread status in the thread pool......")
                    index_to_be_delete_list = [index for index, thread in enumerate(thread_list) if not thread.isAlive()]
                    if len(index_to_be_delete_list) != 0:
                        index_to_be_delete_list.reverse()
                        for index_to_be_delete in index_to_be_delete_list:
                            thread = self.thread_list[index_to_be_delete]
                            # print thread
# Keep track of the error message
                            # self.thread_errmsg_list[thread.index_in_threadpool] = thread.return_errmsg()
                            g_logger.debug("The Thread for scraping %s...... DONE", thread)
                            del self.thread_list[index_to_be_delete]
                    if len(thread_list) == 0:
                        break

            start_index = end_index
            end_index += self.max_concurrent_thread_amount            
            if end_index > datetime_range_list_len:
                break


    def do_scrapy(self, config_list):
        # import pdb; pdb.set_trace()
        for config in config_list:
            try:
                # import pdb; pdb.set_trace()
                module_name = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[config['index']]
                class_name = CMN.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[config['index']]
                g_logger.debug("Try to initiate %s.%s" % (module_name, class_name))
                if config['start'] is None and config['end'] is None:
                    g_logger.debug("Start to scrap %s data today" % CMN.DEF_FINANCE_DATA_INDEX_MAPPING[config['index']])
                    self.__do_scrapy_today(module_name, class_name)
                else:
                    # g_logger.debug("Start to scrap %s data during %d/%d - %d/%d" % (
                    #     CMN.DEF_FINANCE_DATA_INDEX_MAPPING[config['index']], 
                    #     config['start'].year, 
                    #     config['start'].month, 
                    #     config['end'].year, 
                    #     config['end'].month
                    #     )
                    # )
                    self.__do_scrapy_history(module_name, class_name, config['start'], config['end'])
            except Exception as e:
                g_logger.error("Error occur while scraping %s data, due to: %s" % (CMN.DEF_FINANCE_DATA_INDEX_MAPPING[config['index']], str(e)))
