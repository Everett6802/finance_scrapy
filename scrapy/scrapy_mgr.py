#! /usr/bin/python
# -*- coding: utf8 -*-

import common as CMN
from common.common_variable import GlobalVar as GV
import libs as LIBS
# import scrapy.scrapy_definition as CMN.DEF
# import scrapy.scrapy_function as SC_FUNC
# import gui_scrapy_configurer as Configurer
# import cmoney_scrapy as CMS
# import statement_dog_scrapy as SDS
g_logger = CMN.LOG.get_logger()


class ScrapyMgr(object):

    def __init__(self, **cfg):
        self.xcfg = {
            "reserve_old_finance_folder": False,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "max_data_count": None
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
            self.configurer = LIBS.SC.ScrapyConfigurer.Instance()
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


    def set_time(self, time_range_string=None):
        if time_range_string is not None:
            # time_range_string = ",%s," % CMN.FUNC.generate_today_time_str()
            time_range_split = time_range_string.split(",")
            self.time_cfg = {
                "start": time_range_split[0] if len(time_range_split[0]) != 0 else None,
                "end": time_range_split[1] if len(time_range_split[1]) != 0 else None,
                "slice_size": int(time_range_split[2]) if len(time_range_split) == 3 else None,
            }
        else:
            self.time_cfg = None


    def set_scrapy_config_from_file(self):
        method_index_list = self.__get_configurer().Method
        self.set_method(method_index_list=method_index_list)
        company_word_list_string = self.__get_configurer().Company
        self.set_company(company_word_list_string)
        time_range_string = self.__get_configurer().TimeRange
        self.set_time(time_range_string)


    def set_scrapy_config(self, method_index_list_string=None, company_word_list_string=None, time_range_string=None):
        self.set_method(method_index_list_string)
        self.set_company(company_word_list_string)
        self.set_time(time_range_string)


    def set_finance_root_folderpath(self, csv_root_folderpath=None, update_dataset=False):
        assert ((csv_root_folderpath is None) or (not update_dataset)), "incorrect arguments in the 'set_finance_root_folderpath' function"
        if update_dataset:
            csv_root_folderpath = GV.FINANCE_DATASET_DATA_FOLDERPATH
        g_logger.debug("Set CSV root folder path: %s" % csv_root_folderpath)
        self.xcfg["finance_root_folderpath"] = csv_root_folderpath


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


    def enable_dry_run(self, enable):
        if enable:
            g_logger.debug("Enable Dry-Run ......")
        self.xcfg["dry_run_only"] = enable


    @property
    def DryRun(self):
        return self.xcfg["dry_run_only"]


    def do_scrapy(self):
        # import pdb; pdb.set_trace()
        if not self.xcfg["reserve_old_finance_folder"]:
            self.__remove_old_finance_folder()
        self.__create_finance_folder_if_not_exist()

        web_scrapy_cfg = {
            "dry_run_only": self.xcfg["dry_run_only"],
            'finance_root_folderpath': self.xcfg['finance_root_folderpath'],
            "max_data_count": self.xcfg['max_data_count'],
        }

        # import pdb; pdb.set_trace()
        for method_index in self.method_index_list:
            web_scrapy_class = CMN.FUNC.get_scrapy_class(method_index)
            if type(web_scrapy_class) is list:
                raise NotImplementedError
            else:
# Check the field description file exist
                if not web_scrapy_class.check_scrapy_field_description_exist(method_index, self.xcfg['finance_root_folderpath']):
                    g_logger.info(u"The CSV field config of %s does NOT exist, update it in %s......" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index], self.xcfg['finance_root_folderpath']))
                    self.update_csv_field(method_index)
# Scrape the web data
                with web_scrapy_class(**web_scrapy_cfg) as web_scrapy_object:
                    web_scrapy_object.ScrapyMethodIndex = method_index
# The data type of the 'start' and 'end' in self.time_cfg is string
                    web_scrapy_object.TimeCfg = self.time_cfg
                    if not CMN.FUNC.scrapy_method_need_company_number(method_index):
# Market
                        web_scrapy_object.scrape_web_to_csv(*self.scrapy_obj_args, **self.scrapy_obj_kwargs)
                    else:
# Stock
                        for company_group_number, company_number_list in  self.company_group_set.items():
                            for company_number in company_number_list:
# Update the config of the scrapy object
                                web_scrapy_object.CompanyNumber = company_number
                                web_scrapy_object.CompanyGroupNumber = company_group_number
# Scrape the web
                                # import pdb; pdb.set_trace()
                                web_scrapy_object.scrape_web_to_csv(*self.scrapy_obj_args, **self.scrapy_obj_kwargs)


    def update_csv_field(self, method_index_list=None):
        # if not self.xcfg["reserve_old_finance_folder"]:
        #     self.__remove_old_finance_folder()
        # self.__create_finance_folder_if_not_exist()
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
                # if CMN.DEF.SCRAPY_MARKET_METHOD_START <= method_index < CMN.DEF.SCRAPY_MARKET_METHOD_END:
                #     pass
                # elif CMN.DEF.SCRAPY_STOCK_METHOD_START <= method_index < CMN.DEF.SCRAPY_STOCK_METHOD_END:
                #     web_scrapy_object.CompanyNumber = '2330'
                #     web_scrapy_object.CompanyGroupNumber = 9
                # else:
                #     raise ValueError("Unknown scrapy method index: %d" % method_index)
                if CMN.FUNC.scrapy_method_need_company_number(method_index):
                # if SC_FUNC.is_stock_scrapy_method(method_index):
                    web_scrapy_object.CompanyNumber = '2330'
                    web_scrapy_object.CompanyGroupNumber = 9               
                web_scrapy_object.update_csv_field()
