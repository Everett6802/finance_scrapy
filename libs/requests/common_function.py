#! /usr/bin/python
# -*- coding: utf8 -*-
import libs.common as CMN
# import common_definition as CMN_DEF
g_logger = CMN.LOG.get_logger()


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
