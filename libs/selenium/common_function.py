#! /usr/bin/python
# -*- coding: utf8 -*-
import libs.common as CMN
import common_definition as CMN_DEF
g_logger = CMN.LOG.get_logger()


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


def check_scrapy_method_index_in_range(scrapy_method_index, is_stock_method=None):
    if is_stock_method is None:
        return True if (CMN_DEF.SCRAPY_METHOD_START <= scrapy_method_index < CMN_DEF.SCRAPY_METHOD_END) else False
    else:
        if not is_stock_method:
            return True if (CMN_DEF.SCRAPY_MARKET_METHOD_START <= scrapy_method_index < CMN_DEF.SCRAPY_MARKET_METHOD_END) else False
        else:
            return True if (CMN_DEF.SCRAPY_STOCK_METHOD_START <= scrapy_method_index < CMN_DEF.SCRAPY_STOCK_METHOD_END) else False


def get_method_index_from_description(method_description, ignore_exception=False):
    method_index = -1
    try:
        method_index = CMN_DEF.SCRAPY_METHOD_DESCRIPTION.index(method_description)
    except ValueError as e:
        if not ignore_exception:
            raise e
        g_logger.warn("Unknown method description: %s", method_description);
    return method_index


def is_stock_scrapy_method(method_index):
    if CMN_DEF.SCRAPY_MARKET_METHOD_START <= method_index < CMN_DEF.SCRAPY_MARKET_METHOD_END:
        return False
    elif CMN_DEF.SCRAPY_STOCK_METHOD_START <= method_index < CMN_DEF.SCRAPY_STOCK_METHOD_END:
        return True
    else:
        raise ValueError("The method index is Out of range [%d, %d)" % (method_index, CMN_DEF.SCRAPY_METHOD_START, CMN_DEF.SCRAPY_METHOD_END))


def get_finance_data_csv_filepath(method_index, finance_parent_folderpath, company_group_number=None, company_number=None):
    folderpath = CMN.FUNC.get_finance_data_folderpath(finance_parent_folderpath, company_group_number, company_number)
#     if company_group_number is None:
# # Market mode
#         if company_number is not None:
#             raise ValueError("company_group_number and company_number should be both None")
#         if not (CMN_DEF.CRAPY_MARKET_METHOD_START <= method_index < CMN_DEF.CRAPY_MARKET_METHOD_END):
#             raise ValueError("The method index is NOT in the Market index range: [%d: %d)" % (CMN_DEF.CRAPY_MARKET_METHOD_START, CMN_DEF.CRAPY_MARKET_METHOD_END))
#     else:
# # Stock mode
#         if company_number is None:
#             raise ValueError("company_group_number and company_number should be both NOT None")
#         if not (CMN_DEF.CRAPY_STOCK_METHOD_START <= method_index < CMN_DEF.CRAPY_STOCK_METHOD_END):
#             raise ValueError("The method index is NOT in the Stock index range: [%d: %d)" % (CMN_DEF.CRAPY_STOCK_METHOD_START, CMN_DEF.CRAPY_STOCK_METHOD_END))
    if (CMN_DEF.SCRAPY_MARKET_METHOD_START <= method_index < CMN_DEF.SCRAPY_MARKET_METHOD_END):
# Market mode
        if company_group_number is not None:
            raise ValueError("company_group_number should be None")
        if company_number is not None:
            raise ValueError("company_number should be None")
    elif (CMN_DEF.SCRAPY_STOCK_METHOD_START <= method_index < CMN_DEF.SCRAPY_STOCK_METHOD_END):
# Stock mode
        if company_group_number is None:
            raise ValueError("company_group_number should NOT be None")
        if company_number is None:
            raise ValueError("company_number should NOT be None")
    else:
        raise ValueError("Incorrect method index: %d" % method_index)
    return "%s/%s.csv" % (folderpath, CMN_DEF.SCRAPY_CLASS_METHOD[method_index])


def read_csv_time_duration_config_file(conf_filename, conf_folderpath):
    return CMN.FUNC.read_csv_time_duration_config_file(conf_filename, conf_folderpath, get_index_from_description_func_ptr=get_method_index_from_description)


def write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict):
    return CMN.FUNC.write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict, description_array=CMN_DEF.SCRAPY_METHOD_DESCRIPTION)