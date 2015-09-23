import os
import sys
from datetime import datetime
import web_scrapy_future_top10_dealers_and_legal_persons
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyMgr(object):

    def __init__(self):
        self.max_concurrent_thread_amount = 4
        self.thread_pool_list = []
        self.web_scrapy_module_name = [
            "scrap_future_top10_dealers_and_legal_persons",
        ]


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


    def get_class_for_name(self, module_name, class_name):
        m = self.__import_module(module_name)
        parts = module_name.split('.')
        parts.append(class_name)
        for comp in parts[1:]:
            m = getattr(m, comp)            
        return m


    def __do_scrapy_today(self, module_name, class_name):
        class_type = self.__get_class_for_name(module_name, class_name)
        class_obj = class_type()
        class_obj.scrap_web_to_csv()


    def __do_scrapy_history(self, module_name, class_name, datetime_range_start, datetime_range_end):
        datetime_range_list = CMN.get_datetime_range_by_month_list(datetime_range_start, datetime_range_end)


    def do_scrapy(self, config_list):
        for config in config_list:
            try:
                module_name = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[config['index']]
                class_name = CMN.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[config['index']]
                g_logger.debug("Try to initiate %s.%s" % (module_name, class_name))
                if config['start'] is None and config['end'] is None:
                    self.__do_scrapy_today(module_name, class_name)
                else:
                    self.__do_scrapy_history(module_name, class_name, (config['start'], config['end'])
            except Exception as e:
                g_logger.error("Error occur while scraping %s data, due to: %s" % (CMN.DEF_FINANCE_DATA_INDEX_MAPPING[config['index']], str(e)))
