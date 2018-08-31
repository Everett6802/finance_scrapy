#! /usr/bin/python
# -*- coding: utf8 -*-

import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
import libs.base as BASE
import gui_scrapy_configurer as Configurer
import cmoney_scrapy as CMS
import statement_dog_scrapy as SDS
g_logger = CMN.LOG.get_logger()


class GUIScrapyMgr(object):

    def __init__(self, **cfg):
        self.xcfg = {
            "reserve_old_finance_folder": False,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
        }
        self.xcfg.update(cfg)

        self.configurer = None
        self.method_index_list = None
        self.company_group_set = None
        self.scrapy_obj_args = []
        self.scrapy_obj_kwargs = {}


    def __get_configurer(self):
        if self.configurer is None:
            self.configurer = Configurer.GUIScrapyConfigurer.Instance()
        return self.configurer


    def __get_finance_root_folderpath(self, finance_root_folderpath=None):
        folderpath = finance_root_folderpath
        if folderpath is None:
            folderpath = self.xcfg["finance_root_folderpath"]
        if folderpath is None:
            folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH
        # return ("%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_STOCK_FOLDERNAME)) + "%02d"
        return folderpath


    def _remove_old_finance_folder(self, finance_root_folderpath=None):
        CMN.FUNC.delete_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        CMN.FUNC.delete_finance_stock_data_folders(self._get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize)


    def set_config_from_file(self):
        method_index_list = self.__get_configurer().Method
        self.set_method(method_index_list)
        company_word_list_string = self.__get_configurer().Company
        self.set_company(company_word_list_string)


    def set_method(self, method_index_list):
        self.method_index_list = method_index_list


    def set_company(self, company_word_list_string=None):
        if company_word_list_string is not None:
            self.company_group_set = BASE.CGS.CompanyGroupSet.create_instance_from_string(company_word_list_string)
            # self.__transform_company_word_list_to_group_set(company_word_list)
        else:
            self.company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_group_set()


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
        if not self.xcfg["reserve_old_finance_folder"]:
            self._remove_old_finance_folder()
        self._create_finance_folder_if_not_exist()

        for method_index in self.method_index_list:
            web_scrapy_class = CMN.FUNC.get_selenium_web_scrapy_class(method_index)
            with web_scrapy_class() as web_scrapy_object:
                if CMN_DEF.SCRAPY_STOCK_METHOD_START <= method_index < CMN_DEF.SCRAPY_STOCK_METHOD_END:
                    for company_group_number, company_number_list in  self.company_group_set.items():
                        for company_number in company_number_list:
# Update the config of the scrapy object
                            web_scrapy_object.CompanyNumber = company_number
                            web_scrapy_object.CompanyGroupNumber = company_group_number
# Scrape the web		
                            web_scrapy_object.scrape_web_to_csv(CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]['scrapy_class_method'], *self.scrapy_obj_args, **self.scrapy_obj_kwargs)
    # 						g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
                else:
                    raise ValueError("Unknown scrapy method index: %d" % scrapy_method_index)
