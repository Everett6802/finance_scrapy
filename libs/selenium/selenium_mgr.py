#! /usr/bin/python
# -*- coding: utf8 -*-

import cmoney_scrapy as CMS
import statement_dog_scrapy as SDS


class SeleniumMgr(object):

	def __init__(self, **cfg):
        self.xcfg = {
            "reserve_old_finance_folder": False,
            "disable_flush_scrapy_while_exception": False,
            "try_to_scrape_all": True,
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "multi_thread_amount": None,
            "show_progress": False,
            "need_estimate_complete_time": True,
            "start_estimate_complete_time_threshold": 2,
            "disable_output_missing_csv": False,
            # "csv_time_duration_table": None
        }
        self.xcfg.update(cfg)

	    self.company_group_set = None
	    # self.SCRAPY_FUNC_PTR = [
	    # ]


    def _get_finance_root_folderpath(self, finance_root_folderpath=None):
        folderpath = finance_root_folderpath
        if folderpath is None:
            folderpath = self.xcfg["finance_root_folderpath"]
        if folderpath is None:
            folderpath = CMN.DEF.CSV_ROOT_FOLDERPATH
        # return ("%s/%s" % (finance_root_folderpath, CMN.DEF.CSV_STOCK_FOLDERNAME)) + "%02d"
        return folderpath


    def set_company(self, company_word_list_string=None):
        if company_word_list_string is not None:
            self.company_group_set = BASE.CGS.CompanyGroupSet.create_instance_from_string(company_word_list_string)
            # self.__transform_company_word_list_to_group_set(company_word_list)
        else:
            self.company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_group_set()


	def scrape(self, scrapy_method_index, *args, **kwargs):
		web_scrapy_class = CMN.FUNC.get_selenium_web_scrapy_class(scrapy_method_index)
		with web_scrapy_class() as web_scrapy_object:
			for company_group_number, company_number_list in  self.company_group_set.items():
				web_scrapy_object.CompanyNumber = company_number
				(data_list, data_time_list, data_name_list) =  web_scrapy_object.scrape(CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]['scrapy_method_in_class'], *args, **kwargs)


#     def _scrape_class_data(self, scrapy_class_time_duration):
#         # import pdb;pdb.set_trace()
# # Setup the time duration configuration for the scrapy object
#         scrapy_obj_cfg = self._init_cfg_for_scrapy_obj(scrapy_class_time_duration)
#         scrapy_obj_cfg["csv_time_duration_table"] = self.source_type_csv_time_duration_dict
#         scrapy_obj_cfg["disable_flush_scrapy_while_exception"] = self.xcfg["disable_flush_scrapy_while_exception"]
# # Market type
#         market_type = CMN.DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_class_time_duration.scrapy_class_index]["company_group_market_type"]
#         not_support_multithread = False
#         if self.xcfg["multi_thread_amount"] is not None:
#             try:
#                 CMN.DEF.NO_SUPPORT_MULTITHREAD_SCRAPY_CLASS_INDEX.index(scrapy_class_time_duration.scrapy_class_index)
#             except ValueError:
#                 g_logger.warn(u"%s does NOT support multi-threads......." % CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index])
#                 not_support_multithread = True
# # Create the scrapy object to transform the data from Web to CSV
#         if self.xcfg["multi_thread_amount"] is not None and (not not_support_multithread):
#             g_logger.debug("Scrape %s in %d threads" % (CMN.DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_class_time_duration.scrapy_class_index], self.xcfg["multi_thread_amount"]))
# # Run in multi-threads
#             sub_scrapy_obj_cfg_list = []
#             sub_company_group_list = self.__get_market_type_company_group_set(market_type).get_sub_company_group_set_list(self.xcfg["multi_thread_amount"])
# # Perpare the config for each thread
#             for sub_company_group in sub_company_group_list:
#                 sub_scrapy_obj_cfg = copy.deepcopy(scrapy_obj_cfg)
#                 sub_scrapy_obj_cfg["company_group_set"] = sub_company_group
#                 sub_scrapy_obj_cfg_list.append(sub_scrapy_obj_cfg)
# # Start the thread to scrap data
#             self._multi_thread_scrape_web_data_to_csv_file(scrapy_class_time_duration.scrapy_class_index, sub_scrapy_obj_cfg_list)
#         else:
#             scrapy_obj_cfg["company_group_set"] = self.__get_market_type_company_group_set(market_type)
#             self._scrape_web_data_to_csv_file(scrapy_class_time_duration.scrapy_class_index, **scrapy_obj_cfg)




# def scrape_company(company_number, table_data_count=1):
