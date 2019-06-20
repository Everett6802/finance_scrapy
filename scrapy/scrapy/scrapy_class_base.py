#! /usr/bin/python
# -*- coding: utf8 -*-

import copy
import time
from abc import ABCMeta, abstractmethod

import scrapy.common as CMN
import scrapy.libs as LIBS
g_logger = CMN.LOG.get_logger()


class ScrapyBase(object):

    _CAN_SET_TIME_RANGE = False

    @classmethod
    def request_web_data(cls, url, parse_url_method_func_ptr, pre_check_web_data_func_ptr=None, post_check_web_data_func_ptr=None, url_encoding=None):
        # req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
        req = CMN.FUNC.request_from_url_and_check_return(url)
        # import pdb; pdb.set_trace()
        if url_encoding is not None:
            req.encoding = url_encoding
        if pre_check_web_data_func_ptr is not None: 
            pre_check_web_data_func_ptr(req)
        web_data_name = None
        web_data = None
        web_data, web_data_name = parse_url_method_func_ptr(req)
        # assert (web_data is not None), "web_data should NOT be None"
        if post_check_web_data_func_ptr is not None:
            post_check_web_data_func_ptr(web_data)
        return web_data, web_data_name


    @classmethod
    def try_request_web_data(cls, url, parse_url_method_func_ptr, ignore_data_not_found_exception=False, pre_check_web_data_func_ptr=None, post_check_web_data_func_ptr=None, url_encoding=None):
        g_logger.debug("Scrape web data from URL: %s" % url)
        web_data_name = None
        web_data = None
        try:
# Grab the data from website and assemble the data to the entry of CSV
            web_data, web_data_name = cls.request_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr, post_check_web_data_func_ptr, url_encoding)
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
                    web_data, web_data_name = cls.request_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr, post_check_web_data_func_ptr, url_encoding)
                    # assert (web_data is not None), "web_data should NOT be None"
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
# # Caution: web_data should NOT be None. Exception occurs while exploiting len(web_data)
# # The len() function can NOT calculate the length of the None object
#             web_data = []
                web_data = None
                web_data_name = None
        return web_data, web_data_name


    @classmethod
    def get_scrapy_method_list(cls):
        # return cls.__METHOD_NAME_LIST
        method_name_list = None
        try:
            method_name_list = cls.__METHOD_NAME_LIST
        except AttributeError:
            raise AttributeError("The variable[__METHOD_NAME_LIST] is NOT defined in :%s" % cls.__name__)
        return method_name_list


    @classmethod
    def print_scrapy_method(cls):
        # print ", ".join(cls.__METHOD_NAME_LIST)
        print ", ".join(cls.get_scrapy_method_list())


    @classmethod
    def print_scrapy_method_time_unit_description(cls, scrapy_method):
        # print ", ".join(cls.__TIME_UNIT_DESCRIPTION_LIST[scrapy_method])
        time_unit_description_list = None
        try:
            time_unit_description_list = cls.__TIME_UNIT_DESCRIPTION_LIST
        except AttributeError:
            raise AttributeError("The variable[__TIME_UNIT_DESCRIPTION_LIST] is NOT defined in :%s" % cls.__name__)
        print ", ".join(time_unit_description_list[scrapy_method])


    @classmethod
    def _update_cfg_dict(cls, cfg):
        xcfg = {
            "dry_run_only": False,
            "append_before": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            # "config_filename": CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME,
            "max_data_count": None
        }
        xcfg.update(cfg)
        return xcfg


    @classmethod
    def _write_scrapy_field_data_to_config(cls, csv_data_field_list, scrapy_method_index, finance_parent_folderpath):
        conf_folderpath = "%s/%s" % (finance_parent_folderpath, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME)
        conf_filename = ("%s" % CMN.DEF.SCRAPY_CSV_FILENAME[scrapy_method_index]) + CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
        # import pdb; pdb.set_trace()
        CMN.FUNC.unicode_write_config_file_lines(csv_data_field_list, conf_filename, conf_folderpath)


    def __enter__(self):
        return self


    def __exit__(self, type, msg, traceback):
        return False


    def __init__(self):
        self.scrapy_method = None
        self.scrapy_method_index = None
        self.company_number = None
        self.company_group_number = None
        self.time_cfg = None
        # self.ignore_data_not_found_exception = False


    def update_csv_field_description(self):
        _, csv_data_field_list = self.scrape_web()
        self._write_scrapy_field_data_to_config(csv_data_field_list, self.scrapy_method_index, self.xcfg['finance_root_folderpath'])


    def scrape_web_to_csv(self, *args, **kwargs):
        # scrapy_method = CMN.DEF.SCRAPY_METHOD_CONSTANT_CFG[scrapy_method_index]["scrapy_class_method"]
        if kwargs.get("parse_data_name", False):
            g_logger.warn("the 'parse_data_name' will NOT take effect")
        kwargs["parse_data_name"] = False
# Set company number if necessary
        company_number = None
        company_group_number = None
        if CMN.FUNC.scrapy_method_need_company_number(self.scrapy_method_index):
            company_number = self.company_number
            company_group_number = self.company_group_number
# Set time if necessary
        set_time_range = False
        if self.time_cfg is not None:
            if not self._CAN_SET_TIME_RANGE:
                g_logger.warn("The method[%s] does NOT support time range setting !" % self.ScrapyMethod)
            else:
                set_time_range = True
        else:
            set_time_range = self._CAN_SET_TIME_RANGE
# Scrape web data
        # import pdb; pdb.set_trace()
        if set_time_range:
            time_cfg_start = None
            time_cfg_end = None
            time_cfg_slice_size = None
            if self.time_cfg is not None:
                time_cfg_start = self.time_cfg.get("start", None)
                time_cfg_end = self.time_cfg.get("end", None)
                time_cfg_slice_size = self.time_cfg.get("slice_size", None)

            time_slice_generator_kwargs = {}
            need_time_slice_size = CMN.FUNC.scrapy_method_need_time_slice_size(self.scrapy_method_index)
            if need_time_slice_size:
                time_slice_generator_kwargs["time_slice_size"] = time_cfg_slice_size if time_cfg_slice_size is not None else CMN.FUNC.scrapy_method_time_slice_default_size(self.scrapy_method_index)
            else:
                if time_cfg_slice_size is not None:
                    g_logger.debug("%s don't need to set time slice size, just ignore......" % self.scrapy_method)
            # import pdb; pdb.set_trace()
            with LIBS.CSVH.CSVHandler(self.scrapy_method, csv_parent_folderpath=self.xcfg['finance_root_folderpath'], company_number=company_number, company_group_number=company_group_number, append_before_mode=self.xcfg['append_before']) as csv_handler:
# Adjust the scrapy time range according to the CSV file
                web2csv_time_duration_update_tuple = csv_handler.find_scrapy_time_range(time_cfg_start, time_cfg_end)
                if web2csv_time_duration_update_tuple is None:
                    g_logger.debug(u"The data[%s] is Update-to-Date" % CMN.FUNC.assemble_scrapy_method_description(self.scrapy_method_index, company_number))
                    return 
                time_slice_generator = LIBS.TSG.TimeSliceGenerator.Instance()
                for web2csv_time_duration_update in web2csv_time_duration_update_tuple:
                    # import pdb; pdb.set_trace()
                    # print "Time Slice range: %s" % web2csv_time_duration_update
                    for time_slice in time_slice_generator.generate_time_slice(CMN.FUNC.scrapy_method_scrapy_time_unit(self.scrapy_method_index), web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, **time_slice_generator_kwargs):
# Update the sub time range to the config for scraping data
                        slice_kwargs = copy.deepcopy(kwargs)
                        time_start = time_end = None
                        if need_time_slice_size:
                            slice_kwargs["time"] = {
                                "start": time_slice[0],
                                "end": time_slice[1],
                            }
                            time_start = time_slice[0]
                            time_end = time_slice[1]
                            # print "%s, %s" % (time_start, time_end) 
                        else:
                            slice_kwargs["time"] = time_slice
                            time_start = time_end = time_slice
                            # print "%s" % time_slice
# Scrape data in each sub time range
                        # import pdb; pdb.set_trace()
                        csv_data_list, _ = self.scrape_web(*args, **slice_kwargs)                    
                        csv_handler.write(csv_data_list, time_start, time_end, web2csv_time_duration_update.append_direction)     
        else:
            csv_data_list, _ = self.scrape_web(*args, **kwargs)
            # import pdb; pdb.set_trace()
            time_start = str(csv_data_list[0][0])
            time_end = str(csv_data_list[-1][0])
            with LIBS.CSVH.CSVHandler(self.scrapy_method, csv_parent_folderpath=self.xcfg['finance_root_folderpath'], company_number=company_number, company_group_number=company_group_number) as csv_handler:
                # self._write_scrapy_data_to_csv(csv_data_list, time_range_start, time_range_end, self.scrapy_method_index, self.xcfg['finance_root_folderpath'], company_number, company_group_number, dry_run_only=self.xcfg['dry_run_only'])
                web2csv_time_duration_update_tuple = csv_handler.find_scrapy_time_range(time_start, time_end)
                if web2csv_time_duration_update_tuple is None:
                    g_logger.debug(u"The data[%s] is Update-to-Date" % CMN.FUNC.assemble_scrapy_method_description(self.scrapy_method_index, company_number))
                    return 
                csv_handler.write(csv_data_list, time_start, time_end, CMN.DEF.TIME_OVERLAP_AFTER) 


    @abstractmethod
    def scrape_web(self, *args, **kwargs):
        raise NotImplementedError


    # @abstractmethod
    # def update_csv_field(self):
    #     raise NotImplementedError


    @property
    def ScrapyMethod(self):
        return self.scrapy_method

    @ScrapyMethod.setter
    def ScrapyMethod(self, scrapy_method):
        scrapy_method_index = None
        try:
            scrapy_method_index = CMN.DEF.SCRAPY_METHOD_NAME.index(scrapy_method)
        except ValueError:
            errmsg = "The method[%s] is NOT support in %s" % (scrapy_method, CMN.FUNC.get_instance_class_name(self))
            g_logger.error(errmsg)
            raise ValueError(errmsg)
        self.scrapy_method = scrapy_method
        self.scrapy_method_index = scrapy_method_index


    @property
    def ScrapyMethodIndex(self):
        return self.scrapy_method_index

    @ScrapyMethodIndex.setter
    def ScrapyMethodIndex(self, scrapy_method_index):
        # import pdb; pdb.set_trace()
        obj_class_name = CMN.FUNC.get_instance_class_name(self)
        class_name = CMN.DEF.SCRAPY_METHOD_CLASS_NAME[scrapy_method_index]
        incorrect_class_name = False
        if type(class_name) is list:
            incorrect_class_name = (obj_class_name not in class_name)
        else:
            incorrect_class_name = (obj_class_name != class_name)
        if incorrect_class_name:
            raise ValueError("The scrapy index[%d] is NOT supported by the Scrapy class: %s" % (scrapy_method_index, CMN.FUNC.get_instance_class_name(self)))
        self.scrapy_method_index = scrapy_method_index
        self.scrapy_method = CMN.DEF.SCRAPY_METHOD_NAME[self.scrapy_method_index]


    @property
    def CompanyNumber(self):
        return self.company_number

    @CompanyNumber.setter
    def CompanyNumber(self, company_number):
        self.company_number = company_number


    @property
    def CompanyGroupNumber(self):
        return self.company_group_number

    @CompanyGroupNumber.setter
    def CompanyGroupNumber(self, company_group_number):
        self.company_group_number = company_group_number


    @property
    def TimeCfg(self):
        return self.time_cfg

    @TimeCfg.setter
    def TimeCfg(self, time_cfg):
        self.time_cfg = time_cfg
