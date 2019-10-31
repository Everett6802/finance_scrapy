#! /usr/bin/python
# -*- coding: utf8 -*-

import time
import common as CMN
from common.common_variable import GlobalVar as GV
import libs as LIBS
g_logger = CMN.LOG.get_logger()


class ScrapyMgr(object):

    @classmethod
    def check_csv_field_description_exist(cls, scrapy_method_index, finance_parent_folderpath):
        conf_filepath = "%s/%s/%s%s" % (finance_parent_folderpath, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME, CMN.DEF.SCRAPY_CSV_FILENAME[scrapy_method_index], CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX)
        return CMN.FUNC.check_file_exist(conf_filepath)


    def __init__(self, **cfg):
        self.xcfg = {
            "reserve_old_finance_folder": False,
            "append_before": False,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "config_filename": CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME,
            "max_data_count": None,
            "time_unit_filter": None,
        }
        self.xcfg.update(cfg)

        self.configurer = None
        self.company_profile = None
        self.method_index_list = None
        self.company_group_set = None
        self.time_cfg = None
        self.scrapy_obj_args = []
        self.scrapy_obj_kwargs = {}


    def __get_configurer(self):
        if self.configurer is None:
            cfg = {'config_filename': self.xcfg["config_filename"]}
            self.configurer = LIBS.SC.ScrapyConfigurer.Instance(cfg)
        return self.configurer


    def __get_company_profile(self):
        if self.company_profile is None:
            self.company_profile = LIBS.CP.CompanyProfile.Instance()
        return self.company_profile


    def __get_finance_root_folderpath(self, finance_root_folderpath=None):
        folderpath = finance_root_folderpath
        if folderpath is None:
            folderpath = self.xcfg["finance_root_folderpath"]
        if folderpath is None:
            folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH
        # return ("%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_STOCK_FOLDERNAME)) + "%02d"
        return folderpath


    def __create_finance_folder_if_not_exist(self, finance_root_folderpath=None):
        # # self._create_finance_root_folder_if_not_exist(finance_root_folderpath)
        # # folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        # # g_logger.debug("Try to create new folder: %s" % folderpath)
        # # CMN.FUNC.create_folder_if_not_exist(folderpath)
        # CMN.FUNC.create_finance_data_folder(self.__get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        # CMN.FUNC.create_finance_stock_data_folders(self.__get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize)
        CMN.FUNC.create_finance_file_system(self.__get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize)


    def __remove_old_finance_folder(self, finance_root_folderpath=None):
        # CMN.FUNC.delete_finance_data_folder(self.__get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        # CMN.FUNC.delete_finance_stock_data_folders(self.__get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize)
        CMN.FUNC.remove_finance_file_system(self.__get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize, need_field_description=True)


    def __check_time_unit_filter(self, method_index):
        # import pdb; pdb.set_trace()
        if self.xcfg["time_unit_filter"] is None:
            return True
        data_time_unit = CMN.DEF.SCRAPY_DATA_TIME_UNIT[method_index]
        time_unit_str = self.xcfg["time_unit_filter"]
        is_greater_than = False
        if self.xcfg["time_unit_filter"].startswith("gt_"):
            is_greater_than = True
            time_unit_str = time_unit_str.split("_")[1]
        time_unit_str_index = None
        try:
            time_unit_str_index = CMN.DEF.DATA_TIME_UNIT_DESCRIPTION.index(time_unit_str.capitalize())
        except ValueError as e:
            g_logger.debug("Incorrect time unit string: %s" % self.xcfg["time_unit_filter"])
        if is_greater_than:
            if data_time_unit > time_unit_str_index:
                return True
        else:
            if data_time_unit == time_unit_str_index:
                return True
        return False



    def set_method(self, method_index_list_string=None, method_index_list=None):
        if method_index_list is not None:
            self.method_index_list = method_index_list
        else:
            if method_index_list_string is not None:
                self.method_index_list = CMN.FUNC.parse_method_str_to_list(method_index_list_string)
            else:
                # method_index_list_string = ",".join(map(str, range(CMN.DEF.SCRAPY_METHOD_END)))
                self.method_index_list = range(CMN.DEF.SCRAPY_METHOD_END)


    def set_company(self, company_word_list_string=None):
        if company_word_list_string is not None:
            self.company_group_set = LIBS.CGS.CompanyGroupSet.create_instance_from_string(company_word_list_string)
            # self.__transform_company_word_list_to_group_set(company_word_list)
        else:
            self.company_group_set = LIBS.CGS.CompanyGroupSet.get_whole_company_group_set()


    def set_time(self, time_string=None):
        if time_string is not None:
            # time_string = ",%s," % CMN.FUNC.generate_today_time_str()
            time_range_split = time_string.split(",")
            self.time_cfg = {
                "start": time_range_split[0] if len(time_range_split[0]) != 0 else None,
                "end": time_range_split[1] if len(time_range_split[1]) != 0 else None,
                "slice_size": int(time_range_split[2]) if len(time_range_split) == 3 else None,
            }
            g_logger.debug("time_cfg: %s" % self.time_cfg)
            # import pdb; pdb.set_trace()
        else:
            self.time_cfg = None


    def set_scrapy_config_from_file(self):
        method_index_list = self.__get_configurer().Method
        self.set_method(method_index_list=method_index_list)
        company_word_list_string = self.__get_configurer().Company
        self.set_company(company_word_list_string)
        time_string = self.__get_configurer().Time
        self.set_time(time_string)


    def set_scrapy_config(self, method_index_list_string=None, company_word_list_string=None, time_string=None):
        self.set_method(method_index_list_string)
        self.set_company(company_word_list_string)
        self.set_time(time_string)


    def set_finance_root_folderpath(self, csv_root_folderpath):
        g_logger.debug("Set CSV root folder path: %s" % csv_root_folderpath)
        self.xcfg["finance_root_folderpath"] = csv_root_folderpath


    def set_config_filename(self, config_filename):
        g_logger.debug("Set cofig filename: %s" % config_filename)
        self.xcfg["config_filename"] = config_filename


    @property
    def FinanceRootFolderPath(self):
        return self.xcfg["finance_root_folderpath"]


    def reserve_old_finance_folder(self, enable):
        g_logger.debug("Reserve Old Finance Folder: %s" % ("True" if enable else "False"))
        self.xcfg["reserve_old_finance_folder"] = enable


    @property
    def ReserveOld(self):
        return self.xcfg["reserve_old_finance_folder"]


    def disable_flush_scrapy_while_exception(self, disable):
        g_logger.debug("Flush Scrapy Data while Exception Occurs: %s" % ("False" if disable else "True"))
        self.xcfg["disable_flush_scrapy_while_exception"] = disable


    @property
    def DisableFlushScrapy(self):
        return self.xcfg["disable_flush_scrapy_while_exception"]


    def set_append_before_mode(self, append_before):
        if append_before:
            g_logger.debug("Set to Append Before Mode......")
        self.xcfg["append_before"] = append_before


    @property
    def AppendBefore(self):
        return self.xcfg["append_before"]


    def enable_dry_run(self, enable):
        if enable:
            g_logger.debug("Enable Dry-Run......")
        self.xcfg["dry_run_only"] = enable


    @property
    def DryRun(self):
        return self.xcfg["dry_run_only"]


    def __select_scrapy_class(self, scrapy_method_index, web_scrapy_class_list, **kwargs):
        assert type(web_scrapy_class_list) == list, "The type of the argument should be list"
        if scrapy_method_index == CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX:
            company_number = kwargs.get("company_number", None)
            assert company_number is not None, "company_number should NOT be NONE"
            market_type_index = self.__get_company_profile().lookup_company_market_type_index(company_number)
            if market_type_index == CMN.DEF.MARKET_TYPE_NONE:
                raise ValueError("The market type is Unsupported")
            return web_scrapy_class_list[market_type_index]
        else:
            raise ValueError("The scrapy method[%d] of selecting scrapy class is NOT supported" % scrapy_method_index)


    def do_scrapy(self):
        # import pdb; pdb.set_trace()
        if not self.xcfg["reserve_old_finance_folder"]:
            self.__remove_old_finance_folder()
        self.__create_finance_folder_if_not_exist()

        web_scrapy_cfg = {
            "dry_run_only": self.xcfg["dry_run_only"],
            'finance_root_folderpath': self.xcfg['finance_root_folderpath'],
            "max_data_count": self.xcfg['max_data_count'],
            "append_before": self.xcfg['append_before'],
        }

        # import pdb; pdb.set_trace()
        first_scrapy = True
        for i, method_index in enumerate(self.method_index_list):
            if not self.__check_time_unit_filter(method_index):
                g_logger.debug("Skip scrapping: %s[%s]......" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index], CMN.DEF.DATA_TIME_UNIT_DESCRIPTION[CMN.DEF.SCRAPY_DATA_TIME_UNIT[method_index]]))
                continue
            if not first_scrapy:
                sleep_time = 5
                g_logger.debug("Sleep %d seconds before scraping %s......" % (sleep_time, CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index]))
                time.sleep(sleep_time)
            if first_scrapy:
                first_scrapy = False
            if CMN.FUNC.scrapy_method_need_select_class(method_index):
# Check the field description file exist
                if not self.check_csv_field_description_exist(method_index, self.xcfg['finance_root_folderpath']):
                    g_logger.info(u"The CSV field config of %s does NOT exist, update it in %s......" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index], self.xcfg['finance_root_folderpath']))
                    self.update_csv_field_description(method_index)
# Scrape the web data
                web_scrapy_class_list = CMN.FUNC.get_scrapy_class(method_index)
                if not CMN.FUNC.scrapy_method_need_company_number(method_index):
                    raise NotImplementedError
                else:
                    for company_group_number, company_number_list in  self.company_group_set.items():
                        for company_number in company_number_list:
                            kwargs = {"company_number": company_number}
                            web_scrapy_class = self.__select_scrapy_class(method_index, web_scrapy_class_list, **kwargs)
# Update the config of the scrapy object
                            with web_scrapy_class(**web_scrapy_cfg) as web_scrapy_object:
                                web_scrapy_object.ScrapyMethodIndex = method_index
# The data type of the 'start' and 'end' in self.time_cfg is string
                                web_scrapy_object.TimeCfg = self.time_cfg

                                web_scrapy_object.CompanyNumber = company_number
                                web_scrapy_object.CompanyGroupNumber = company_group_number
# Scrape the web
                                # import pdb; pdb.set_trace()
                                web_scrapy_object.scrape_web_to_csv(*self.scrapy_obj_args, **self.scrapy_obj_kwargs)
            else:
# Check the field description file exist
                if not self.check_csv_field_description_exist(method_index, self.xcfg['finance_root_folderpath']):
                    g_logger.info(u"The CSV field config of %s does NOT exist, update it in %s......" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index], self.xcfg['finance_root_folderpath']))
                    self.update_csv_field_description(method_index)
# Scrape the web data
                web_scrapy_class = CMN.FUNC.get_scrapy_class(method_index)
                with web_scrapy_class(**web_scrapy_cfg) as web_scrapy_object:
                    web_scrapy_object.ScrapyMethodIndex = method_index
# The data type of the 'start' and 'end' in self.time_cfg is string
                    web_scrapy_object.TimeCfg = self.time_cfg
                    if not CMN.FUNC.scrapy_method_need_company_number(method_index):
# Market
                        web_scrapy_object.scrape_web_to_csv(*self.scrapy_obj_args, **self.scrapy_obj_kwargs)
                    else:
# Stock
                        for company_group_number, company_number_list in self.company_group_set.items():
                            for company_number in company_number_list:
# Update the config of the scrapy object
                                web_scrapy_object.CompanyNumber = company_number
                                web_scrapy_object.CompanyGroupNumber = company_group_number
# Scrape the web
                                # import pdb; pdb.set_trace()
                                web_scrapy_object.scrape_web_to_csv(*self.scrapy_obj_args, **self.scrapy_obj_kwargs)


    def update_csv_field_description(self, method_index_list=None):
        # import pdb; pdb.set_trace()
        if method_index_list is not None:
            if type(method_index_list) is int:
                method_index_list = [method_index_list,]
        else:
            method_index_list = self.method_index_list
        for method_index in method_index_list:
            web_scrapy_class = CMN.FUNC.get_scrapy_class(method_index)
            web_scrapy_cfg = {
                'finance_root_folderpath': self.xcfg['finance_root_folderpath'],
            }
            with web_scrapy_class(**web_scrapy_cfg) as web_scrapy_object:
                web_scrapy_object.ScrapyMethodIndex = method_index
                if CMN.FUNC.scrapy_method_need_company_number(method_index):
                    web_scrapy_object.CompanyNumber = CMN.DEF.SCCRAPY_CLASS_COMPANY_NUMBER
                    web_scrapy_object.CompanyGroupNumber = CMN.DEF.SCCRAPY_CLASS_COMPANY_GROUP_NUMBER            
                web_scrapy_object.update_csv_field_description()


    def get_csv_time_range(self, company_number_list=None):
        # import pdb; pdb.set_trace()
        csv_time_range_ret = None
        if company_number_list is None:
# market
            # csv_time_duration_folderpath = CMN.FUNC.get_finance_data_folderpath(self.xcfg["finance_root_folderpath"])
            csv_time_duration_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(None, self.xcfg["finance_root_folderpath"])
            csv_time_range_ret = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_time_duration_folderpath)
        else:
# stock
            if type(company_number_list) is str:
                company_number_list = [company_number_list,]
            csv_time_range_dict = {}
            for company_number in company_number_list:
                company_group_number = self.__get_company_profile().lookup_company_group_number(company_number)
                # csv_time_duration_folderpath = CMN.FUNC.get_finance_data_folderpath(self.xcfg["finance_root_folderpath"], company_group_number, company_number)
                csv_time_duration_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(None, self.xcfg["finance_root_folderpath"], company_group_number, company_number)
                csv_time_duration_cfg_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_time_duration_folderpath)
                csv_time_range_dict[company_number] = csv_time_duration_cfg_dict
            csv_time_range_ret = csv_time_range_dict
        return csv_time_range_ret


    def remove_csv_data(self, company_number_list=None):
        # import pdb; pdb.set_trace()
        remove_data_ret = None
        if company_number_list is None:
# market
            data_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(None, self.xcfg["finance_root_folderpath"])
            remove_data_ret = CMN.FUNC.remove_folder_if_exist(data_folderpath)
        else:
# stock
            if type(company_number_list) is str:
                company_number_list = [company_number_list,]
            remove_data_dict = {}
            for company_number in company_number_list:
                company_group_number = self.__get_company_profile().lookup_company_group_number(company_number)
                data_folderpath = CMN.FUNC.get_finance_data_csv_folderpath(None, self.xcfg["finance_root_folderpath"], company_group_number, company_number)
                remove_data_dict[company_number] = CMN.FUNC.remove_folder_if_exist(data_folderpath)
            remove_data_ret = remove_data_dict
        return remove_data_ret
