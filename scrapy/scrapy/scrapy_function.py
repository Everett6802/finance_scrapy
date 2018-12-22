#! /usr/bin/python
# -*- coding: utf8 -*-

import scrapy.common as CMN
import scrapy_definition as SC_DEF
g_logger = CMN.LOG.get_logger()


__CAN_USE_SELEIUM__ = None
def can_use_selenium():
    try:
        from selenium import webdriver
    except ImportError:
        return False
    return True

def get_web_scrapy_class(scrapy_method_index):
    if scrapy_method_index < 0 or scrapy_method_index >= SC_DEF.SCRAPY_METHOD_LEN:
        raise ValueError("scrapy_method_index[%d] is Out-Of-Range [0, %d)" % (scrapy_method_index, SC_DEF.SCRAPY_METHOD_LEN))
    if __CAN_USE_SELEIUM__ is None:
        __CAN_USE_SELEIUM__ = can_use_selenium()

    if SC_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]['need_selenium']:
        if not __CAN_USE_SELEIUM__:
            g_logger.error("Selenium is NOT Install !!! Fail to scrape %s" % SC_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]["description"])
            return None

    # import pdb; pdb.set_trace()
    module_folder = SC_DEF.SCRAPY_MODULE_FOLDER
    module_name = SC_DEF.SCRAPY_MODULE_NAME_MAPPING[scrapy_method_index]
    class_name = SC_DEF.SCRAPY_CLASS_NAME_MAPPING[scrapy_method_index]
    assert type(module_name) == type(class_name), "The types of module and class name [%s, %s] should be identical" % (type(module_name) == type(class_name))
    g_logger.debug("Try to instantiate %s.%s" % (module_name, class_name))
# Find the module
    web_scrapy_class = CMN.FUNC.get_web_scrapy_class_for_name(module_folder, module_name, class_name)
    # if init_class_variables:
    #     web_scrapy_class.init_class_common_variables() # Caution: Must be called in the leaf derived class
    #     web_scrapy_class.init_class_customized_variables() # Caution: Must be called in the leaf derived class         
    return web_scrapy_class


def check_scrapy_method_index_in_range(scrapy_method_index, is_stock_method=None):
    if is_stock_method is None:
        return True if (SC_DEF.SCRAPY_METHOD_START <= scrapy_method_index < SC_DEF.SCRAPY_METHOD_END) else False
    else:
        if not is_stock_method:
            return True if (SC_DEF.SCRAPY_MARKET_METHOD_START <= scrapy_method_index < SC_DEF.SCRAPY_MARKET_METHOD_END) else False
        else:
            return True if (SC_DEF.SCRAPY_STOCK_METHOD_START <= scrapy_method_index < SC_DEF.SCRAPY_STOCK_METHOD_END) else False


def get_method_index_from_description(method_description, ignore_exception=False):
    method_index = -1
    try:
        method_index = SC_DEF.SCRAPY_METHOD_DESCRIPTION.index(method_description)
    except ValueError as e:
        if not ignore_exception:
            raise e
        g_logger.warn("Unknown method description: %s", method_description);
    return method_index


# def is_stock_scrapy_method(method_index):
#     if SC_DEF.SCRAPY_MARKET_METHOD_START <= method_index < SC_DEF.SCRAPY_MARKET_METHOD_END:
#         return False
#     elif SC_DEF.SCRAPY_STOCK_METHOD_START <= method_index < SC_DEF.SCRAPY_STOCK_METHOD_END:
#         return True
#     else:
#         raise ValueError("The method index is Out of range [%d, %d)" % (method_index, SC_DEF.SCRAPY_METHOD_START, SC_DEF.SCRAPY_METHOD_END))


def get_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr=None, post_check_web_data_func_ptr=None):
    # req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
    req = CMN.FUNC.request_from_url_and_check_return(url)
    if pre_check_web_data_func_ptr is not None: 
        pre_check_web_data_func_ptr(req)
    web_data = parse_url_method_func_ptr(req, cls.CLASS_CONSTANT_CFG)
    assert (web_data is not None), "web_data should NOT be None"
    if post_check_web_data_func_ptr is not None:
        post_check_web_data_func_ptr(web_data)
    return web_data


def try_get_web_data(url, parse_url_method_func_ptr, ignore_data_not_found_exception=False, pre_check_web_data_func_ptr=None, post_check_web_data_func_ptr=None):
    g_logger.debug("Scrape web data from URL: %s" % url)
    web_data = None
    try:
# Grab the data from website and assemble the data to the entry of CSV
        web_data = get_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr, post_check_web_data_func_ptr)
    except CMN.EXCEPTION.WebScrapyNotFoundException as e:
        if not ignore_data_not_found_exception:
            errmsg = None
            if isinstance(e.message, str):
                errmsg = "WebScrapyNotFoundException occurs while scraping URL[%s], due to: %s" % (url, e.message)
            else:
                errmsg = u"WebScrapyNotFoundException occurs while scraping URL[%s], due to: %s" % (url, e.message)
            CMN.FUNC.try_print(errmsg)
            g_logger.error(errmsg)
            raise e
    except CMN.EXCEPTION.WebScrapyServerBusyException as e:
# Server is busy, let's retry......
        RETRY_TIMES = 5
        SLEEP_TIME_BEFORE_RETRY = 15
        scrapy_success = False
        for retry_times in range(1, RETRY_TIMES + 1):
            if scrapy_success: break
            g_logger.warn("Server is busy, let's retry...... %d", retry_times)
            time.sleep(SLEEP_TIME_BEFORE_RETRY * retry_times)
            try:
                web_data = get_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr, post_check_web_data_func_ptr)
                assert (web_data is not None), "web_data should NOT be None"
                if post_check_web_data_func_ptr is not None:
                    post_check_web_data_func_ptr(web_data)
            except CMN.EXCEPTION.WebScrapyNotFoundException as e:
                if not ignore_data_not_found_exception:
                    errmsg = None
                    if isinstance(e.message, str):
                        errmsg = "RETRY[%d]! WebScrapyNotFoundException occurs while scraping URL[%s], due to: %s" % (retry_times, url, e.message)
                    else:
                        errmsg = u"RETRY[%d]! WebScrapyNotFoundException occurs while scraping URL[%s], due to: %s" % (retry_times, url, e.message)
                    CMN.FUNC.try_print(errmsg)
                    g_logger.error(errmsg)
                    raise e
                else:
                    scrapy_success = True
            except CMN.EXCEPTION.WebScrapyServerBusyException as e:
                pass
            else:
                scrapy_success = True
        if not scrapy_success:
            raise CMN.EXCEPTION.WebScrapyServerBusyException("Fail to scrape URL[%s] after retry for %d times" % (url, RETRY_TIMES))
    except Exception as e:
        # import pdb;pdb.set_trace()
        if isinstance(e.message, str):
            g_logger.warn("Exception occurs while scraping URL[%s], due to: %s" % (url, e.message))
        else:
            g_logger.warn(u"Exception occurs while scraping URL[%s], due to: %s" % (url, e.message))
# Caution: web_data should NOT be None. Exception occurs while exploiting len(web_data)
# The len() function can NOT calculate the length of the None object
        web_data = []
    return web_data


def get_finance_data_csv_folderpath(method_index, finance_parent_folderpath, company_group_number=None):
    if not is_stock_scrapy_method(method_index):
# Market mode
        if company_group_number is not None:
            raise ValueError("company_group_number should be None")
    else:
# Stock mode
        if company_group_number is None:
            raise ValueError("company_group_number should NOT be None")
    folderpath = CMN.FUNC.get_finance_data_folderpath(finance_parent_folderpath, (-1 if company_group_number is None else company_group_number))
    return folderpath


def get_finance_data_csv_filepath(method_index, finance_parent_folderpath, company_group_number=None, company_number=None):
    if not is_stock_scrapy_method(method_index):
# Market mode
        if company_group_number is not None:
            raise ValueError("company_group_number should be None")
        if company_number is not None:
            raise ValueError("company_number should be None")
    else:
# Stock mode
        if company_group_number is None:
            raise ValueError("company_group_number should NOT be None")
        if company_number is None:
            raise ValueError("company_number should NOT be None")
    # else:
    #     raise ValueError("Incorrect method index: %d" % method_index)
    folderpath = CMN.FUNC.get_finance_data_folderpath(finance_parent_folderpath, (-1 if company_group_number is None else company_group_number), company_number)
    return "%s/%s.csv" % (folderpath, SC_DEF.SCRAPY_CSV_FILENAME[method_index])


def read_csv_time_duration_config_file(conf_filename, conf_folderpath):
    return CMN.FUNC.read_csv_time_duration_config_file(conf_filename, conf_folderpath, get_index_from_description_func_ptr=get_method_index_from_description)


def write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict):
    return CMN.FUNC.write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict, description_array=SC_DEF.SCRAPY_METHOD_DESCRIPTION)
