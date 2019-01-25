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
    def request_web_data(cls, url, parse_url_method_func_ptr, pre_check_web_data_func_ptr=None, post_check_web_data_func_ptr=None):
        # req = CMN.FUNC.try_to_request_from_url_and_check_return(url)
        req = CMN.FUNC.request_from_url_and_check_return(url)
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
    def try_request_web_data(cls, url, parse_url_method_func_ptr, ignore_data_not_found_exception=False, pre_check_web_data_func_ptr=None, post_check_web_data_func_ptr=None):
        g_logger.debug("Scrape web data from URL: %s" % url)
        web_data_name = None
        web_data = None
        try:
# Grab the data from website and assemble the data to the entry of CSV
            web_data, web_data_name = cls.request_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr, post_check_web_data_func_ptr)
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
                    web_data, web_data_name = cls.request_web_data(url, parse_url_method_func_ptr, pre_check_web_data_func_ptr, post_check_web_data_func_ptr)
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
    def find_scrapy_new_time_range_from_csv(cls, time_duration_start, time_duration_end, scrapy_method_index, finance_parent_folderpath, company_number=None, company_group_number=None):
        def get_old_csv_time_duration_if_exist(scrapy_method_index, csv_time_duration_dict):
            if csv_time_duration_dict is not None:
                if csv_time_duration_dict.get(scrapy_method_index, None) is not None:
                    return csv_time_duration_dict[scrapy_method_index]
            return None

        # import pdb; pdb.set_trace()
        csv_time_duration_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(scrapy_method_index, finance_parent_folderpath, company_group_number)
        csv_time_duration_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_time_duration_folderpath)

        data_time_unit = CMN.DEF.SCRAPY_DATA_TIME_UNIT[scrapy_method_index]
# Caution: Need transfrom the time string from unicode to string
        if type(time_duration_start) is str: 
            time_duration_start = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_start, data_time_unit)
        if type(time_duration_end) is str: 
            time_duration_end = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_end, data_time_unit)

        csv_old_time_duration_tuple = get_old_csv_time_duration_if_exist(scrapy_method_index, csv_time_duration_dict)

        new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_duration_start, 
            time_duration_end,
            csv_old_time_duration_tuple
        )
        if csv_time_duration_dict is None:
            csv_time_duration_dict = {}
        csv_time_duration_dict[scrapy_method_index] = new_csv_extension_time_duration

        return csv_time_duration_dict, web2csv_time_duration_update_tuple


    @classmethod
    def _write_scrapy_data_to_csv(cls, csv_data_list, time_duration_start, time_duration_end, scrapy_method_index, finance_parent_folderpath, company_number=None, company_group_number=None, dry_run_only=False):
        assert csv_data_list is not None, "csv_data_list should NOT be None"
# Check data time range
        # import pdb; pdb.set_trace()
        data_time_unit = CMN.DEF.SCRAPY_DATA_TIME_UNIT[scrapy_method_index]
        if type(time_duration_start) is str: 
            time_duration_start = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_start, data_time_unit)
        csv_time_duration_start = CMN.CLS.FinanceTimeBase.from_time_string(str(csv_data_list[0][0]), data_time_unit)
        if time_duration_start > csv_time_duration_start:
            raise ValueError("Incorrect time range: time_duration_start[%s] > csv_time_duration_start[%s]" % (time_duration_start, csv_time_duration_start))
        if type(time_duration_end) is str: 
            time_duration_end = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_end_str, data_time_unit)
        csv_time_duration_end = CMN.CLS.FinanceTimeBase.from_time_string(str(csv_data_list[-1][0]), data_time_unit)
        if time_duration_end < csv_time_duration_end:
            raise ValueError("Incorrect time range: time_duration_end[%s] < csv_time_duration_end[%s]" % (time_duration_end, csv_time_duration_end))
        # import pdb; pdb.set_trace()
        csv_time_duration_dict, web2csv_time_duration_update_tuple = cls.find_scrapy_new_time_range_from_csv(time_duration_start, time_duration_end, scrapy_method_index, finance_parent_folderpath, company_number, company_group_number)
        if  web2csv_time_duration_update_tuple is None:
            msg = None
            if company_number is not None:
                msg = u"The data[%s:%s] is Update-to-Date" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index], company_number)
            else:
                msg = u"The data[%s] is Update-to-Date" % CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index]
            g_logger.debug(msg)
            return
# Find the file path for writing data into csv
        csv_filepath = CMN.FUNC.get_finance_data_csv_filepath(scrapy_method_index, finance_parent_folderpath, company_group_number, company_number)
# Scrape the web data from each time duration
        for web2csv_time_duration_update in web2csv_time_duration_update_tuple:
            scrapy_msg = None
            if company_number is not None:
                scrapy_msg = u"[%s:%s] %s:%s => %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index], company_number, web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
            else:
                scrapy_msg = "[%s] %s:%s => %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index], web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
            g_logger.info(scrapy_msg)
# Check if only dry-run
            if dry_run_only:
                print scrapy_msg
                continue
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
            web2csv_time_duration_update.backup_old_csv_if_necessary(csv_filepath)
            # sub_csv_data_list = cls._filter_scrapy_data(csv_data_list, web2csv_time_duration_update)
            sub_csv_data_list = []
            if web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
                for csv_data in csv_data_list:
                    time_duration = CMN.CLS.FinanceTimeBase.from_time_string(str(csv_data[0]), data_time_unit)
                    if time_duration > web2csv_time_duration_update.NewWebEnd:
                        break
                    sub_csv_data_list.append(csv_data)
            elif web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_AFTER:
                # for csv_data in reversed(csv_data_list):
                for csv_data in csv_data_list:
                    time_duration = CMN.CLS.FinanceTimeBase.from_time_string(str(csv_data[0]), data_time_unit)
                    if time_duration < web2csv_time_duration_update.NewWebStart:
                        continue
                    sub_csv_data_list.append(csv_data)
            else:
                raise ValueError("Unsupport AppendDirection: %d" % web2csv_time_duration_update.AppendDirection)

            g_logger.debug("Write %d data to %s" % (len(sub_csv_data_list), csv_filepath))
            CMN.FUNC.write_csv_data(sub_csv_data_list, csv_filepath)
# Append the old CSV data after the new web data if necessary
            web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath)
        if dry_run_only:
            return
        # import pdb; pdb.set_trace()
# Update the time duration
        csv_time_duration_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(scrapy_method_index, finance_parent_folderpath, company_group_number)
        CMN.FUNC.write_csv_time_duration(csv_time_duration_dict, csv_time_duration_folderpath)


    @classmethod
    def check_scrapy_field_description_exist(cls, scrapy_method_index, finance_parent_folderpath):
        conf_filepath = "%s/%s/%s%s" % (finance_parent_folderpath, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME, CMN.DEF.SCRAPY_CSV_FILENAME[scrapy_method_index], CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX)
        return CMN.FUNC.check_file_exist(conf_filepath)


    @classmethod
    def _write_scrapy_field_data_to_config(cls, csv_data_field_list, scrapy_method_index, finance_parent_folderpath):
        conf_folderpath = "%s/%s" % (finance_parent_folderpath, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME)
        conf_filename = ("%s" % CMN.DEF.SCRAPY_CSV_FILENAME[scrapy_method_index]) + CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
        CMN.FUNC.unicode_write_config_file_lines(csv_data_field_list, conf_filename, conf_folderpath)


    @classmethod
    def _extend_csv_data_time_range(cls, scrapy_method_index, finance_time_range, extended_time_range_start, extended_time_range_end):
        if not CMN.FUNC.scrapy_method_is_scrapy_and_data_time_unit_the_same(scrapy_method_index):
            data_time_unit = CMN.FUNC.scrapy_method_data_time_unit(scrapy_method_index)
            scrapy_time_unit = CMN.FUNC.scrapy_method_scrapy_time_unit(scrapy_method_index)
            if data_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY and scrapy_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
                assert extended_time_range_start == extended_time_range_end, "The extended time range start[%s] and end[%s] should be the same month" % (extended_time_range_start, extended_time_range_end)
                workday_canlendar = LIBS.WC.WorkdayCanlendar.Instance()
                # extended_time_range_date_start = None
                if workday_canlendar.FirstMonthOfWorkday == extended_time_range_start:
                    extended_time_range_start = workday_canlendar.FirstWorkday
                else:
                    extended_time_range_start = CMN.CLS.FinanceDate(extended_time_range_start.year, extended_time_range_start.month, 1)
                # extended_time_range_date_end = None
                if workday_canlendar.LastMonthOfWorkday == extended_time_range_end:
                    extended_time_range_end = workday_canlendar.LastWorkday
                else:
                    extended_time_range_end = CMN.CLS.FinanceDate(extended_time_range_end.year, extended_time_range_end.month, extended_time_range_end.get_last_date_of_month())
            else:
                raise ValueError("Unsupport time unit to extend, data: %d, scrapy: %d" % (data_time_unit, scrapy_time_unit))

        if finance_time_range is None:
            finance_time_range = CMN.CLS.FinanceTimeRange(extended_time_range_start, extended_time_range_end)
        else:
            if extended_time_range_start < finance_time_range.time_start:
                finance_time_range.time_start = extended_time_range_start
            if extended_time_range_end > finance_time_range.time_end:
                finance_time_range.time_end = extended_time_range_end
        return finance_time_range


    # @classmethod
    # def _set_scrapy_method(cls, obj, value):
    #     try:
    #         obj.method_list.index(value)
    #     except ValueError:
    #         errmsg = "The method[%s] is NOT support in %s" % (value, CMN.FUNC.get_instance_class_name(self))
    #         g_logger.error(errmsg)
    #         raise ValueError(errmsg)
    #     obj.scrapy_method = value
    #     if obj.scrapy_method_index is not None:
    #         g_logger.warn("The {0}::scrapy_method_index is reset since the {0}::scrapy_method is set ONLY".format(CMN.FUNC.get_instance_class_name(obj)))
    #         obj.scrapy_method_index = None
    #     raise NotImplementedError


    # @classmethod
    # def _set_scrapy_method_index(cls, obj, value):
    #     # import pdb; pdb.set_trace()
    #     if CMN.DEF.SCRAPY_METHOD_INDEX_CONSTANT_CFG[value]['class_name'] != CMN.FUNC.get_instance_class_name(obj):
    #         raise ValueError("The scrapy index[%d] is NOT supported by the Scrapy class: %s" % (value, CMN.FUNC.get_instance_class_name(obj)))
    #     obj.scrapy_method_index = value
    #     obj.scrapy_method = CMN.DEF.SCRAPY_METHOD_NAME[obj.scrapy_method_index]


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


    def update_csv_field(self):
        _, csv_data_field_list = self.scrape_web()
        self._write_scrapy_field_data_to_config(csv_data_field_list, self.scrapy_method_index, self.xcfg['finance_root_folderpath'])


    def scrape_web_to_csv(self, *args, **kwargs):
        # scrapy_method = CMN.DEF.SCRAPY_METHOD_CONSTANT_CFG[scrapy_method_index]["scrapy_class_method"]
        if kwargs.get("parse_data_name", False):
            g_logger.warn("the 'parse_data_name' will NOT take effect")
        kwargs["parse_data_name"] = False
        set_time_range = False
        if self.time_cfg is not None:
            if not self._CAN_SET_TIME_RANGE:
                g_logger.warn("The method[%s] does NOT support time range setting !" % self.ScrapyMethod)
            else:
                set_time_range = True
# Set company number if necessary
        company_number = None
        company_group_number = None
        if CMN.FUNC.scrapy_method_need_company_number(self.scrapy_method_index):
            company_number = self.company_number
            company_group_number = self.company_group_number
# Scrape web data
        # import pdb; pdb.set_trace()
        if set_time_range:
# Define the time slice
            time_slice_size = self.time_cfg.get("slice_size", None)
            if time_slice_size is not None:
                if CMN.FUNC.scrapy_method_need_time_slice_default_size(self.scrapy_method_index):
                    g_logger.warn("%s should use time slice default size: %d" % (self.scrapy_method, CMN.FUNC.scrapy_method_time_slice_default_size(self.scrapy_method_index)))
                    time_slice_size = None
            if time_slice_size is None:
                time_slice_size = CMN.FUNC.scrapy_method_time_slice_default_size(self.scrapy_method_index)
# Adjust the scrapy time range according to the CSV file
            _, web2csv_time_duration_update_tuple = self.find_scrapy_new_time_range_from_csv(self.time_cfg["start"], self.time_cfg["end"], self.scrapy_method_index, self.xcfg['finance_root_folderpath'], company_number, company_group_number)
            time_slice_generator = LIBS.TSG.TimeSliceGenerator.Instance()
            for web2csv_time_duration_update in web2csv_time_duration_update_tuple:
                total_csv_data_list = []
                total_csv_data_list_len = 0
                finance_time_range = None
                for time_range_slice in time_slice_generator.generate_time_range_slice(web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, time_slice_size, CMN.FUNC.scrapy_method_time_slice_unit(self.scrapy_method_index)):
                    # print "%s, %s" % (time_slice[0], time_slice[1])
# New sub time range
                    time_range_cfg = {
                        "start": time_range_slice[0],
                        "end": time_range_slice[1],
                    }

                    # if finance_time_range is None:
                    #     finance_time_range = CMN.CLS.FinanceTimeRange(time_range_slice[0], time_range_slice[1])
                    # else:
                    #     if time_range_slice[0] < finance_time_range.time_start:
                    #         finance_time_range.time_start = time_range_slice[0]
                    #     if time_range_slice[1] > finance_time_range.time_end:
                    #         finance_time_range.time_end = time_range_slice[1]
# Update the sub time range to the config for scraping data
                    slice_kwargs = copy.deepcopy(kwargs)
                    slice_kwargs["time_range"] = time_range_cfg
# Scrape data in each sub time range
                    # import pdb; pdb.set_trace()
                    csv_data_list, _ = self.scrape_web(*args, **slice_kwargs)
                    csv_data_list_len = len(csv_data_list)
                    finance_time_range = self._extend_csv_data_time_range(self.scrapy_method_index, finance_time_range, time_range_cfg["start"], time_range_cfg["end"])
                    total_csv_data_list.extend(csv_data_list)
                    total_csv_data_list_len += csv_data_list_len
                    if total_csv_data_list_len >= 100:
                        self._write_scrapy_data_to_csv(total_csv_data_list, finance_time_range.time_start, finance_time_range.time_end, self.scrapy_method_index, self.xcfg['finance_root_folderpath'], company_number, company_group_number, dry_run_only=self.xcfg['dry_run_only'])
                        total_csv_data_list = []
                        total_csv_data_list_len = 0
                        finance_time_range = None
                if total_csv_data_list_len != 0:
                    self._write_scrapy_data_to_csv(total_csv_data_list, finance_time_range.time_start, finance_time_range.time_end, self.scrapy_method_index, self.xcfg['finance_root_folderpath'], company_number, company_group_number, dry_run_only=self.xcfg['dry_run_only'])       
        else:
            csv_data_list, _ = self.scrape_web(*args, **kwargs)
            # import pdb; pdb.set_trace()
            time_range_start = str(csv_data_list[0][0])
            time_range_end = str(csv_data_list[-1][0])
            self._write_scrapy_data_to_csv(csv_data_list, self.scrapy_method_index, self.xcfg['finance_root_folderpath'], company_number, company_group_number, dry_run_only=self.xcfg['dry_run_only'])


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
