# -*- coding: utf8 -*-

import os
import re
import sys
import time
import requests
import shutil
import copy
from datetime import datetime
import libs.common as CMN
import libs.base as BASE
# import web_scrapy_company_profile as CompanyProfile
# import web_scrapy_company_group_set as CompanyGroupSet
# import scrapy_configurer as Configurer
g_logger = CMN.LOG.get_logger()


SHOW_STATMENT_FIELD_DIMENSION = True

class StockMgr(BASE.MGR_BASE.MgrBase):

    finance_mode = CMN.DEF.FINANCE_MODE_STOCK
    company_profile = None

    def __init__(self, **cfg):
        super(StockMgr, self).__init__(**cfg)
        self.is_whole_company_group_set = False
        self.company_group_set = None
        self.source_type_csv_time_duration_dict = None
        self.market_type_company_group_set_dict = None


    # def _get_configurer(self):
    #     if self.configurer is None:
    #         self.configurer = Configurer.StockConfigurer.Instance()
    #     return self.configurer


    def __get_market_type_company_group_set(self, market_type=CMN.DEF.MARKET_TYPE_NONE):
# Initialization
        if self.market_type_company_group_set_dict is None:
            self.market_type_company_group_set_dict = {}
            if self.company_group_set is None:
                self.is_whole_company_group_set = True
# Update the company group set if necessary
        if self.is_whole_company_group_set:
            if not self.market_type_company_group_set_dict.has_key(market_type):
                # self.market_type_company_group_set_dict[market_type] = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict(market_type)
                self.market_type_company_group_set_dict[market_type] = BASE.CGS.CompanyGroupSet.get_whole_company_group_set(market_type)
        else:
            if market_type == CMN.DEF.MARKET_TYPE_NONE:
                if not self.market_type_company_group_set_dict.has_key(CMN.DEF.MARKET_TYPE_NONE): 
                    self.market_type_company_group_set_dict[CMN.DEF.MARKET_TYPE_NONE] = self.company_group_set
            else:
                if not self.market_type_company_group_set_dict.has_key(market_type):
                    self.market_type_company_group_set_dict[market_type] = self.company_group_set.get_sub_company_group_set_in_market_type(market_type)
        return self.market_type_company_group_set_dict[market_type]


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = BASE.CP.CompanyProfile.Instance()
        return cls.company_profile


    # def __get_finance_folderpath_format(self, finance_root_folderpath=None):
    #     if finance_root_folderpath is None:
    #         finance_root_folderpath = self.xcfg["finance_root_folderpath"]
    #     if finance_root_folderpath is None:
    #         finance_root_folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH
    #     # return ("%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_STOCK_FOLDERNAME)) + "%02d"
    #     return CMN.FUNC.assemble_finance_data_folder(finance_root_folderpath, -1)


    def _create_finance_folder_if_not_exist(self, finance_root_folderpath=None):
        # self._create_finance_root_folder_if_not_exist(finance_root_folderpath)
        # folderpath_format = self.__get_finance_folderpath_format(finance_root_folderpath)
        # for index in range(self.__get_company_profile().CompanyGroupSize):
        #     folderpath = folderpath_format % index
        #     # g_logger.debug("Try to create new folder: %s" % folderpath)
        #     CMN.FUNC.create_folder_if_not_exist(folderpath)
        CMN.FUNC.create_finance_stock_data_folders(self._get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize)


    def _remove_old_finance_folder(self, finance_root_folderpath=None):
# Remove the old data if necessary
        # folderpath_format = self.__get_finance_folderpath_format(finance_root_folderpath)
        # for index in range(self.__get_company_profile().CompanyGroupSize):
        #     folderpath = folderpath_format % index
        #     # g_logger.debug("Remove old folder: %s" % folderpath)
        #     shutil.rmtree(folderpath, ignore_errors=True)
        CMN.FUNC.delete_finance_stock_data_folders(self._get_finance_root_folderpath(finance_root_folderpath), self.__get_company_profile().CompanyGroupSize)


    def _init_csv_time_duration(self, company_group_set=None):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration_dict is None, "self.source_type_csv_time_duration_dict should be None"
        if company_group_set is None:
            company_group_set = self.__get_market_type_company_group_set()
        # if company_group_set is None:
        #     company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        self.source_type_csv_time_duration_dict = {}
        for company_group_number, company_code_number_list in company_group_set.items():
            for company_code_number in company_code_number_list:
                # csv_time_duration_list = [None] * CMN.DEF.DATA_SOURCE_STOCK_SIZE
                self.source_type_csv_time_duration_dict[company_code_number] = {}


    def __parse_csv_time_duration_cfg(self, finance_root_folderpath=None, company_group_set=None):
        # whole_company_number_in_group_dict = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        # folderpath_format = self.__get_finance_folderpath_format(finance_root_folderpath)
        if company_group_set is None:
            company_group_set = self.__get_market_type_company_group_set()
        # if company_group_set is None:
        #     company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        source_type_csv_time_duration_dict = {}
        # for company_group_number, company_code_number_list in whole_company_number_in_group_dict:
        finance_root_folderpath = self._get_finance_root_folderpath(finance_root_folderpath)
        for company_group_number, company_code_number_list in company_group_set.items():
            # folderpath_in_group = folderpath_format % int(company_group_number)
            folderpath_in_group = CMN.FUNC.assemble_finance_data_folder(finance_root_folderpath, int(company_group_number))
# If the company group folder does NOT exist, ignore it...
            if not CMN.FUNC.check_file_exist(folderpath_in_group):
                continue
            for company_code_number in company_code_number_list:
                csv_data_folderpath = "%s/%s" % (folderpath_in_group, company_code_number) 
                g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
                csv_time_duration_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
                if csv_time_duration_dict is None:
                    g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME)
                    continue
# update the time range of each source type of comapny from config files
                source_type_csv_time_duration_dict[company_code_number] = csv_time_duration_dict
        return source_type_csv_time_duration_dict if source_type_csv_time_duration_dict else None


    def _read_old_csv_time_duration(self):
        assert self.source_type_csv_time_duration_dict is not None, "self.source_type_csv_time_duration_dict should NOT be None"
#         # whole_company_number_in_group_dict = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
#         folderpath_format = self.__get_finance_folderpath_format()
#         # self.source_type_csv_time_duration_dict = {}
#         # for company_group_number, company_code_number_list in whole_company_number_in_group_dict:
#         for company_group_number, company_code_number_list in self.company_group_set.items():
#             folderpath_in_group = folderpath_format % int(company_group_number)
# # If the company group folder does NOT exist, ignore it...
#             if not CMN.FUNC.check_file_exist(folderpath_in_group):
#                 continue
#             for company_code_number in company_code_number_list:
#                 csv_data_folderpath = "%s/%s" % (folderpath_in_group, company_code_number) 
#                 g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
#                 csv_time_duration_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
#                 if csv_time_duration_dict is None:
#                     g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME)
#                     continue
# # update the time range of each source type of comapny from config files
        source_type_csv_time_duration_dict = self.__parse_csv_time_duration_cfg()
        if source_type_csv_time_duration_dict is not None:
            self.source_type_csv_time_duration_dict = source_type_csv_time_duration_dict


    def _update_new_csv_time_duration(self, web_scrapy_obj):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration_dict is not None, "self.source_type_csv_time_duration_dict should NOT be None"
        # new_csv_time_duration_dict = web_scrapy_obj.get_new_csv_time_duration_dict()
        if web_scrapy_obj.new_csv_time_duration_dict is not None:
            for company_number, time_duration_tuple in web_scrapy_obj.new_csv_time_duration_dict.items():
                self.source_type_csv_time_duration_dict[company_number][web_scrapy_obj.SourceTypeIndex] = time_duration_tuple


    def __write_new_csv_time_duration_to_cfg(self, finance_root_folderpath=None, source_type_csv_time_duration_dict=None, company_group_set=None):
        # import pdb; pdb.set_trace()
        # folderpath_format = self.__get_finance_folderpath_format(finance_root_folderpath)
        if source_type_csv_time_duration_dict is None:
            source_type_csv_time_duration_dict = self.source_type_csv_time_duration_dict
        if company_group_set is None:
            company_group_set = self.__get_market_type_company_group_set()
        # if company_group_set is None:
        #     company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        finance_root_folderpath = self._get_finance_root_folderpath(finance_root_folderpath)
        for company_group_number, company_code_number_list in company_group_set.items():
            # folderpath_in_group = folderpath_format % int(company_group_number)
            for company_code_number in company_code_number_list:
                # csv_data_folderpath = "%s/%s" % (folderpath_in_group, company_code_number) 
# Create the folder for each company if not exist
                # CMN.FUNC.create_folder_if_not_exist(csv_data_folderpath)
                # g_logger.debug("Try to write CSV time range config in the folder: %s ......" % csv_data_folderpath)
                csv_data_folderpath = CMN.FUNC.create_finance_data_folder(finance_root_folderpath, int(company_group_number), company_code_number)
                CMN.FUNC.write_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath, source_type_csv_time_duration_dict[company_code_number])


    def _write_new_csv_time_duration(self):
        self.__write_new_csv_time_duration_to_cfg()


    # def __transform_company_word_list_to_group_set(self, company_word_list_string):
#         """
#         The argument type:
#         Company code number: 2347
#         Company code number range: 2100-2200
#         Company group number: [Gg]12
#         Company code number/number range hybrid: 2347,2100-2200,2362,g2,1500-1510
#         """
#         self.company_group_set = BASE.CGS.CompanyGroupSet()
#         for company_number in company_word_list:
#             mobj = re.match("([\d]{4})-([\d]{4})", company_number)
#             if mobj is None:
# # Check if data is company code/group number
#                 mobj = re.match("[Gg]([\d]{1,})", company_number)
#                 if mobj is None:
# # Company code number
#                     if not re.match("([\d]{4})", company_number):
#                         raise ValueError("Unknown company number format: %s" % company_number)
#                     self.company_group_set.add_company(company_number)
#                 else:
# # Compgny group number
#                     company_group_number = int(mobj.group(1))
#                     self.company_group_set.add_company_group(company_group_number)
#             else:
# # Company code number Range
#                 start_company_number_int = int(mobj.group(1))
#                 end_company_number_int = int(mobj.group(2))
#                 number_list = []
#                 for number in range(start_company_number_int, end_company_number_int + 1):
#                     number_list.append("%04d" % number)
#                 self.company_group_set.add_company_word_list(number_list)
#         self.company_group_set.add_done()


    def set_config_from_file(self):
        # import pdb; pdb.set_trace()
        super(StockMgr, self).set_config_from_file()
        # company_word_list = CMN.FUNC.read_company_config_file(filename)
        # self.set_company(company_word_list)
        self.set_company(self._get_configurer().Company)


    def set_company(self, company_word_list_string=None):
        if company_word_list_string is not None:
            self.company_group_set = BASE.CGS.CompanyGroupSet.create_instance_from_string(company_word_list_string)
            # self.__transform_company_word_list_to_group_set(company_word_list)
        else:
            self.company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_group_set()


    # def initialize(**kwargs):
    #     super(StockMgr, self).initialize(**kwargs)
    #     if kwargs.get("company_word_list", None) is not None:
    #         company_group_set = CompanyGroupSet()
    #         for company_number in kwargs["company_word_list"]:
    #             company_group_set.add_company(company_number)
    #         company_group_set.add_done();
    #         if kwargs.get("company_group_set", None) is not None:
    #             g_logger.warn("The company_group_set field is ignored......")
    #     elif kwargs.get("company_group_set", None) is not None:
    #         self.xcfg["company_group_set"] = kwargs["company_group_set"]
    #     else:
    #         self.xcfg["company_group_set"] = CompanyGroupSet.get_whole_company_group_set()


    def _scrape_class_data(self, scrapy_class_time_duration):
        # import pdb;pdb.set_trace()
# Setup the time duration configuration for the scrapy object
        scrapy_obj_cfg = self._init_cfg_for_scrapy_obj(scrapy_class_time_duration)
        scrapy_obj_cfg["csv_time_duration_table"] = self.source_type_csv_time_duration_dict
        scrapy_obj_cfg["disable_flush_scrapy_while_exception"] = self.xcfg["disable_flush_scrapy_while_exception"]
# Market type
        market_type = CMN.DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_class_time_duration.scrapy_class_index]["company_group_market_type"]
        not_support_multithread = False
        if self.xcfg["multi_thread_amount"] is not None:
            try:
                CMN.DEF.NO_SUPPORT_MULTITHREAD_SCRAPY_CLASS_INDEX.index(scrapy_class_time_duration.scrapy_class_index)
            except ValueError:
                g_logger.warn(u"%s does NOT support multi-threads......." % CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index])
                not_support_multithread = True
# Create the scrapy object to transform the data from Web to CSV
        if self.xcfg["multi_thread_amount"] is not None and (not not_support_multithread):
            g_logger.debug("Scrape %s in %d threads" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], self.xcfg["multi_thread_amount"]))
# Run in multi-threads
            sub_scrapy_obj_cfg_list = []
            sub_company_group_list = self.__get_market_type_company_group_set(market_type).get_sub_company_group_set_list(self.xcfg["multi_thread_amount"])
# Perpare the config for each thread
            for sub_company_group in sub_company_group_list:
                sub_scrapy_obj_cfg = copy.deepcopy(scrapy_obj_cfg)
                sub_scrapy_obj_cfg["company_group_set"] = sub_company_group
                sub_scrapy_obj_cfg_list.append(sub_scrapy_obj_cfg)
# Start the thread to scrap data
            self._multi_thread_scrape_web_data_to_csv_file(scrapy_class_time_duration.scrapy_class_index, sub_scrapy_obj_cfg_list)
        else:
            scrapy_obj_cfg["company_group_set"] = self.__get_market_type_company_group_set(market_type)
            self._scrape_web_data_to_csv_file(scrapy_class_time_duration.scrapy_class_index, **scrapy_obj_cfg)


    def _renew_scrapy_class_statement(self, scrapy_class_time_duration, dst_statement_field_list, dst_statement_column_field_list):
        if not CMN.FUNC.check_statement_scrapy_class_index_in_range(scrapy_class_time_duration.scrapy_class_index):
            raise ValueError("The source type[%d] is NOT in range [%d, %d]" % (scrapy_class_time_duration.scrapy_class_index, CMN.DEF.SCRAPY_STOCK_CLASS_STATMENT_START, CMN.DEF.DATA_SOURCE_STOCK_STATMENT_END))
        # import pdb;pdb.set_trace()
# Setup the time duration configuration for the scrapy object
        scrapy_obj_cfg = self._init_cfg_for_scrapy_obj(scrapy_class_time_duration)
        # scrapy_obj_cfg["csv_time_duration_table"] = self.source_type_csv_time_duration_dict
        scrapy_obj_cfg["company_group_set"] = self.__get_market_type_company_group_set()
# Create the scrapy object
        # import pdb; pdb.set_trace()
        web_scrapy_class = CMN.FUNC.get_web_scrapy_class(scrapy_class_time_duration.scrapy_class_index, True)
        # web_scrapy_class.init_class_common_variables() # Caution
        scrapy_obj_cfg["renew_statement_field"] = True
        web_scrapy_obj = CMN.FUNC.get_web_scrapy_object(web_scrapy_class, **scrapy_obj_cfg)
        if web_scrapy_obj is None:
            raise RuntimeError("Fail to allocate ScrapyStockBase derived class")
        g_logger.debug("Start to renew %s statement field......", web_scrapy_obj.get_description())
        # dst_statement_column_field_list = [] if web_scrapy_class.TABLE_COLUMN_FIELD_EXIST else None
        web_scrapy_obj.update_statement_field(dst_statement_field_list, dst_statement_column_field_list)
        # if SHOW_STATMENT_FIELD_DIMENSION:
        #     web_scrapy_class.show_statement_field_dimension()


    def show_company_list_in_finance_folder(self, finance_root_folderpath=None):
        # import pdb; pdb.set_trace()
        if not CMN.FUNC.check_file_exist(self.xcfg["finance_root_folderpath"]):
            print "The root finance folder[%s] does NOT exist" % self.xcfg["finance_root_folderpath"]
            return
        # company_group_folderpath_format = self.__get_finance_folderpath_format()
        # for index in range(self.__get_company_profile().CompanyGroupSize):
        whole_company_number_in_group_dict = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        finance_root_folderpath = self._get_finance_root_folderpath(finance_root_folderpath)
        for company_group_number, company_code_number_list in whole_company_number_in_group_dict.items():
            # company_group_folderpath = company_group_folderpath_format % company_group_number
            company_group_folderpath = CMN.FUNC.create_finance_data_folder(finance_root_folderpath, company_group_number)
            if not CMN.FUNC.check_file_exist(company_group_folderpath):
                raise ValueError("The folder[%s] should exist !!!" % company_group_folderpath)
            company_ode_number_in_group_list = []
            for company_code_number in company_code_number_list:
                company_folderpath = "%s/%s" % (company_group_folderpath, company_code_number)
                if not CMN.FUNC.check_file_exist(company_folderpath):
                    continue
                company_ode_number_in_group_list.append(company_code_number)
            if len(company_ode_number_in_group_list) != 0:
                print "%s: %s" % (company_group_number, ",".join(company_ode_number_in_group_list))


    def renew_statement_field(self):
        # import pdb; pdb.set_trace()
        for scrapy_class_time_duration in self.scrapy_class_time_duration_list:
            assert CMN.FUNC.check_statement_scrapy_class_index_in_range(scrapy_class_time_duration.scrapy_class_index), "The scrapy class index[%d] is NOT in the range: [%d, %d)" % (scrapy_class_time_duration.scrapy_class_index, CMN.DEF.SCRAPY_STOCK_METHOD_STATMENT_START, CMN.DEF.SCRAPY_STOCK_METHOD_STATMENT_END)
            web_scrapy_class = CMN.FUNC.get_web_scrapy_class(scrapy_class_time_duration.scrapy_class_index, False)
            table_column_field_exist = web_scrapy_class.TABLE_COLUMN_FIELD_EXIST
            conf_filename = CMN.DEF.STATEMENT_FIELD_NAME_CONF_FILENAME[scrapy_class_time_duration.scrapy_class_index - CMN.DEF.SCRAPY_STOCK_CLASS_STATMENT_START]
            dst_statement_field_list = None #[]
            dst_statement_column_field_list = None #([] if table_column_field_exist else None) 
            old_dst_statement_field_list = None #[]
            old_dst_statement_column_field_list = None #([] if table_column_field_exist else None) 
            config_file_exist = False
# Check if the original statement field exists
            if CMN.FUNC.check_config_file_exist(conf_filename):
# Read the original statement field list from file if exist
                if table_column_field_exist:
# If the column field exist......
                    total_statement_field_list = CMN.FUNC.unicode_read_config_file_lines(conf_filename)
                    try:
                        # import pdb; pdb.set_trace()
                        column_field_start_flag_index = total_statement_field_list.index(CMN.DEF.COLUMN_FIELD_START_FLAG_IN_CONFIG)
                        dst_statement_field_list = copy.deepcopy(total_statement_field_list[0:column_field_start_flag_index])
                        dst_statement_column_field_list = copy.deepcopy(total_statement_field_list[column_field_start_flag_index + 1:])
                    except ValueError:
                        raise CMN.EXCEPTION.WebScrapyIncorrectFormatException("The column field flag is NOT found in the config file" % conf_filename)
                else:
                    dst_statement_field_list = CMN.FUNC.unicode_read_config_file_lines(conf_filename)
# Keep track of the old data
                old_dst_statement_field_list = copy.deepcopy(dst_statement_field_list)
                if table_column_field_exist:
                    old_dst_statement_column_field_list = copy.deepcopy(dst_statement_column_field_list)
                config_file_exist = True
            else:
                dst_statement_field_list = []
                if table_column_field_exist:
                    dst_statement_column_field_list = []              
            try:
# Update the statement field
                self._renew_scrapy_class_statement(scrapy_class_time_duration, dst_statement_field_list, dst_statement_column_field_list)
            except Exception as e:
                if isinstance(e.message, str):
                    errmsg = "Renew %s statement fails, due to: %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], e.message)
                else:
                    errmsg = u"Renew %s statement fails, due to: %s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], e.message)
                CMN.FUNC.try_print(CMN.FUNC.get_full_stack_traceback())
                g_logger.error(errmsg)
                raise e
            need_renew = False
            # import pdb; pdb.set_trace()
# Compare the statement field in the old and new config file
            if config_file_exist:
                new_statement_field_list = list(set(dst_statement_field_list) - set(old_dst_statement_field_list))
                if len(new_statement_field_list) != 0:
                    msg = u"***** Add new field in statement[%s %s:%s] as below *****\n" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], scrapy_class_time_duration.time_duration_start, scrapy_class_time_duration.time_duration_end)
                    for new_statement_field in new_statement_field_list:
                        msg += u"%s\n" % new_statement_field
                    CMN.FUNC.try_print(msg)
                    g_logger.info(msg)
                    need_renew = True
                if table_column_field_exist:
                    new_statement_column_field_list = list(set(dst_statement_column_field_list) - set(old_dst_statement_column_field_list))
                    if len(new_statement_column_field_list) != 0:
                        msg = u"***** Add new column field in statement[%s %s:%s] as below *****\n" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], scrapy_class_time_duration.time_duration_start, scrapy_class_time_duration.time_duration_end)
                        for new_statement_column_field in new_statement_column_field_list:
                            msg += u"%s\n" % new_statement_column_field
                        CMN.FUNC.try_print(msg)
                        g_logger.info(msg)
                        need_renew = True
            else:
                need_renew = True
# Write the new statement field list into file if necessary
            # import pdb; pdb.set_trace()
            if need_renew:
                if table_column_field_exist:
                    dst_statement_field_list.append(CMN.DEF.COLUMN_FIELD_START_FLAG_IN_CONFIG)
                    dst_statement_field_list.extend(dst_statement_column_field_list)
                CMN.FUNC.unicode_write_config_file_lines(dst_statement_field_list, conf_filename)
                # CMN.FUNC.write_config_file_lines_ex(dst_statement_field_list, conf_filename, "wb")
            if SHOW_STATMENT_FIELD_DIMENSION:
                web_scrapy_class.show_statement_field_dimension()


#     def check_scrapy(self):
#         file_not_found_list = []
#         file_is_empty_list = []
#         for scrapy_class_time_duration in self.scrapy_class_time_duration_list:
#             for company_group_number, company_code_number_list in self.company_group_set.items():
#                 for company_code_number in company_code_number_list:
#                     csv_filepath = CMN.FUNC.assemble_stock_csv_filepath(self.xcfg["finance_root_folderpath"], scrapy_class_time_duration.scrapy_class_index, company_code_number, company_group_number)
# # Check if the file exists
#                     if not os.path.exists(csv_filepath):
#                         file_not_found_list.append(
#                             {
#                                 "company_code_number": company_code_number,
#                                 "index": scrapy_class_time_duration.scrapy_class_index,
#                                 "filename" : CMN.FUNC.get_filename_from_filepath(csv_filepath),
#                             }
#                         )
#                     elif os.path.getsize(csv_filepath) == 0:
#                         file_is_empty_list.append(
#                             {
#                                 "company_code_number": company_code_number,
#                                 "index": scrapy_class_time_duration.scrapy_class_index,
#                                 "filename" : CMN.FUNC.get_filename_from_filepath(csv_filepath),
#                             }
#                         )
# # Output the missing CSV to the file if necessary
#         if not self.xcfg["disable_output_missing_csv"]:
#             file_not_found_list_len = len(file_not_found_list)
#             file_is_empty_list_len = len(file_is_empty_list)  
#             if file_not_found_list_len != 0 or file_is_empty_list_len != 0:
#                 missing_csv_filepath = "%s/%s" % (self.xcfg["finance_root_folderpath"], CMN.DEF.MISSING_CSV_STOCK_FILENAME)
#                 g_logger.debug("Write missing CSVs to the file: %s......" % missing_csv_filepath)
#                 with open(missing_csv_filepath, 'wb') as fp:
#                     try:
# # Output the file not found list
#                         if file_not_found_list_len != 0:
#                             fp.write("[FileNotFound]\n") 
#                             for file_not_found in file_not_found_list[:-1]:
#                                 fp.write("%s:%d;" % (file_not_found["company_code_number"], file_not_found["index"]))
#                             fp.write("%s:%d\n" % (file_not_found_list[-1]["company_code_number"], file_not_found_list[-1]["index"]))
# # Output the file is empty list
#                         if file_is_empty_list_len != 0:
#                             fp.write("[FileIsEmpty]\n")
#                             for file_is_empty in file_is_empty_list[:-1]:
#                                 fp.write("%s:%d;" % (file_is_empty["company_code_number"], file_is_empty["index"]))
#                             fp.write("%s:%d\n" % (file_is_empty_list[-1]["company_code_number"], file_is_empty_list[-1]["index"]))
#                     except Exception as e:
#                         g_logger.error(u"Fail to write data to missing CSV file, due to %s" %str(e))
#                         # g_logger.error(u"Error occur while writing Company Code Number[%s] info into config file, due to %s" % (company_profile_unicode, str(e)))
#                         raise e
#         return (file_not_found_list, file_is_empty_list)


#     def check_scrapy_to_string(self):
#         (file_not_found_list, file_is_empty_list) = self.check_scrapy()
#         error_msg = None
#         error_msg_list = []
#         for file_not_found in file_not_found_list:
#             error_msg = u"FileNotFound: %s, %s/%s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[file_not_found['index']], file_not_found['company_code_number'], file_not_found['filename'])
#             error_msg_list.append(error_msg)
#         for file_is_empty in file_is_empty_list:
#             error_msg = u"FileIsEmpty: %s, %s/%s" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[file_is_empty['index']], file_not_found['company_code_number'], file_is_empty['filename'])
#             error_msg_list.append(error_msg)
#         if len(error_msg_list) != 0:
#             error_msg = "\n".join(error_msg_list)
#         return error_msg


    def _find_existing_source_type_finance_folder_index(self, csv_time_duration_cfg_list, scrapy_class_index, company_code_number):
# Search for the index of the finance folder which the specific source type index exists
# -1 if not found
# Exception occur if the source type is found in more than one finance folder
        finance_folder_index = -1
        for index, csv_time_duration_cfg in enumerate(csv_time_duration_cfg_list):
            if not csv_time_duration_cfg.has_key(company_code_number):
                continue
            # import pdb; pdb.set_trace()
            if csv_time_duration_cfg[company_code_number].has_key(scrapy_class_index):
                if finance_folder_index != -1:
                    raise ValueError("The source type index[%d] in %s is duplicate" % (scrapy_class_index, company_code_number))
                else:
                    finance_folder_index = index
        return finance_folder_index


    # def _increment_scrapy_class_type_progress_count(self, scrapy_class_index):
    #     market_type = CMN.DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_class_index]["company_group_market_type"]
    #     self.scrapy_class_type_progress_count += self.__get_market_type_company_group_set(market_type).CompanyAmount


    def merge_finance_folder(self, finance_folderpath_src_list, finance_folderpath_dst):
        self._check_merge_finance_folder_exist(finance_folderpath_src_list, finance_folderpath_dst)
        if CMN.FUNC.check_file_exist(finance_folderpath_dst):
            raise ValueError("The destination folder[%s] after mering has already exist" % finance_folderpath_dst)
        self._create_finance_folder_if_not_exist(finance_folderpath_dst)
# Find source type list in each source finance folder
        csv_time_duration_cfg_list = []
        for finance_folderpath_src in finance_folderpath_src_list:
            csv_time_duration_cfg_list.append(self.__parse_csv_time_duration_cfg(finance_folderpath_src))
# Merge the finance folder
# Copy the CSV files from source folder to destiantion one
        (scrapy_class_index_start, scrapy_class_index_end) = CMN.FUNC.get_scrapy_class_index_range()
        new_source_type_csv_time_duration = {}
        # import pdb; pdb.set_trace()
        company_group_set_dst = {}
        whole_company_number_in_group_dict = BASE.CGS.CompanyGroupSet.get_whole_company_number_in_group_dict()
        for company_group_number, company_code_number_list in whole_company_number_in_group_dict.items():
            for company_code_number in company_code_number_list:
                new_source_type_csv_time_duration_for_one_company = {}           
                for scrapy_class_index in range(scrapy_class_index_start, scrapy_class_index_end):
                    finance_folder_index = self._find_existing_source_type_finance_folder_index(csv_time_duration_cfg_list, scrapy_class_index, company_code_number)
                    if finance_folder_index == -1:
                        continue
                    src_csv_filepath = CMN.FUNC.assemble_stock_csv_filepath(finance_folderpath_src_list[finance_folder_index], scrapy_class_index, company_code_number, company_group_number)
                    CMN.FUNC.create_folder_if_not_exist(CMN.FUNC.assemble_stock_csv_folderpath(finance_folderpath_dst, company_code_number, company_group_number))
                    dst_csv_filepath = CMN.FUNC.assemble_stock_csv_filepath(finance_folderpath_dst, scrapy_class_index, company_code_number, company_group_number)
                    CMN.FUNC.copy_file(src_csv_filepath, dst_csv_filepath)
# Keep track of the company code number of exsiting data
                    if not company_group_set_dst.has_key(company_group_number):
                        company_group_set_dst[company_group_number] = set()
                    company_group_set_dst[company_group_number].add(company_code_number)
# Update the new time duration config
                    # import pdb; pdb.set_trace()
                    new_source_type_csv_time_duration_for_one_company[scrapy_class_index] = csv_time_duration_cfg_list[finance_folder_index][company_code_number][scrapy_class_index]
                new_source_type_csv_time_duration[company_code_number] = new_source_type_csv_time_duration_for_one_company
        # import pdb; pdb.set_trace()
        self.__write_new_csv_time_duration_to_cfg(finance_folderpath_dst, new_source_type_csv_time_duration, company_group_set_dst)


    # def count_scrapy_amount(self):
    #     if self.scrapy_amount is None:
    #         self.scrapy_amount = 0
    #         for scrapy_class_time_duration in self.scrapy_class_time_duration_list:
    #             market_type = CMN.DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_class_time_duration.scrapy_class_index]["company_group_market_type"]
    #             self.scrapy_amount += self.__get_market_type_company_group_set(market_type).CompanyAmount
    #         g_logger.debug("There are totally %d scrapy times" % self.scrapy_amount)
    #     return self.scrapy_amount


    # def count_scrapy_progress(self):
    #     if self.scrapy_amount is None:
    #         raise ValueError("self.scrapy_amount shoudl NOT be None")
    #     company_progress_count = 0
    #     with self.web_scrapy_obj_list_thread_lock:
    #         if self.web_scrapy_obj_list:
    #             for web_scrapy_obj in self.web_scrapy_obj_list:
    #                 # import pdb; pdb.set_trace()
    #                 company_progress_count += web_scrapy_obj.CompanyProgressCount
    #     progress_count = self.scrapy_class_type_progress_count + company_progress_count
    #     return (float(progress_count) / self.scrapy_amount * 100.0) 


    # @property
    # def ScrapyAmount(self):
    #     return self.count_scrapy_amount()


    # @property
    # def ScrapyProgress(self):
    #     return self.count_scrapy_progress()
