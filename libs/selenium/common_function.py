#! /usr/bin/python
# -*- coding: utf8 -*-
import libs.common as CMN
import common_definition as CMN_DEF


def get_selenium_web_scrapy_class(scrapy_method_index):
	if scrapy_method_index < 0 or scrapy_method_index >= CMN_DEF.SCRAPY_METHOD_LEN:
    	raise ValueError("scrapy_method_index[%d] is Out-Of-Range [0, %d)" % (scrapy_method_index, CMN_DEF.SCRAPY_METHOD_LEN))
    # import pdb; pdb.set_trace()
    module_folder = CMN_DEF.SCRAPY_MODULE_FOLDER
    module_name = CMN_DEF.SCRAPY_MODULE_NAME_MAPPING[scrapy_method_index]
    class_name = CMN_DEF.SCRAPY_CLASS_NAME_MAPPING[scrapy_method_index]
    g_logger.debug("Try to instantiate %s.%s" % (module_name, class_name))
# Find the module
    web_scrapy_class = CMN.FUNC.get_web_scrapy_class_for_name(module_folder, module_name, class_name)
    # if init_class_variables:
    #     web_scrapy_class.init_class_common_variables() # Caution: Must be called in the leaf derived class
    #     web_scrapy_class.init_class_customized_variables() # Caution: Must be called in the leaf derived class         
    return web_scrapy_class


def get_method_index_from_description(method_description, ignore_exception=False):
    method_index = -1
    try:
        method_index = CMN_DEF.SCRAPY_METHOD_DESCRIPTION.index(method_description)
    except ValueError as e:
        if not ignore_exception:
            raise e
        g_logger.warn("Unknown method description: %s", method_description);
    return method_index


def read_csv_time_duration_config_file(conf_filename, conf_folderpath):
    return CMN.FUNC.read_csv_time_duration_config_file(conf_filename, conf_folderpath, get_index_from_description_func_ptr=get_method_index_from_description)


def write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict):
    return CMN.FUNC.write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict, description_array=CMN_DEF.SCRAPY_METHOD_DESCRIPTION)