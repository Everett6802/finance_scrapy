# -*- coding: utf8 -*-

import os
import sys
# import time
# import requests
# from datetime import datetime
import common as CMN
# import web_scrapy_thread
# from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyMgrBase(object):

    def __init__(self):
        pass


    def __import_module(self, name):
        current_path = os.path.dirname(os.path.realpath(__file__))
        sys.path.insert(0, current_path)
        module_file = '%s/%s.py' % (current_path, name)
        assert os.path.exists(module_file), "module file does not exist: %s" % module_file
        try:
            module = __import__(name)
            if module:
                # print 'Import file: %s.py (%s)' % (name, module)
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


    def check_scrapy(self, config_list):
        raise NotImplementedError 


    def do_scrapy(self, config_list, multi_thread=False, need_retry=True):
        for config in config_list:
            try:
                module_name = CMN.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX + CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[config['index']]
                class_name = CMN.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[config['index']]
                g_logger.debug("Try to initiate %s.%s" % (module_name, class_name))
                if not multi_thread:
                    self.__do_scrapy(module_name, class_name, config['start'], config['end'])
                else:
                    self.__do_scrapy_by_multithread(module_name, class_name, config['start'], config['end'])
            except Exception as e:
                g_logger.error("Error occur while scraping %s data, due to: %s" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], str(e)))

# Retry to scrap the web data if error occurs
        if need_retry and len(self.retry_config_list) != 0:
            g_logger.warn("Retry to scrap the web data due to some errors.......")
            for retry_config in self.retry_config_list:
                try:
                    module_name = CMN.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX + CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[retry_config['index']]
                    class_name = CMN.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[retry_config['index']]
                    g_logger.debug("Re-Try to initiate %s.%s" % (module_name, class_name))
                    self.__do_scrapy(module_name, class_name, retry_config['start'], retry_config['end'])
                except Exception as e:
                    g_logger.error("Error occur while ReTrying to scrap %s data, due to: %s" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[retry_config['index']], str(e)))


    def do_debug(self, data_source_index):
        module_name = CMN.DEF_WEB_SCRAPY_MODULE_NAME_PREFIX + CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index]
        class_name = CMN.DEF_WEB_SCRAPY_CLASS_NAME_MAPPING[data_source_index]
        g_logger.debug("Try to initiate %s.%s for debugging......" % (module_name, class_name))
        web_scrapy_class_obj = self.__create_web_scrapy_object(module_name, class_name)
        web_scrapy_class_obj.do_debug()
