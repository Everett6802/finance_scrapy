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


    def __parse_csv_time_duration_cfg(self, finance_root_folderpath=None):
        # csv_data_folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        csv_data_folderpath = CMN.FUNC.get_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        g_logger.debug("Try to parse CSV time range config in the folder: %s ......" % csv_data_folderpath)
        csv_time_duration_dict = CMN.FUNC.read_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath)
        if csv_time_duration_dict is None:
            g_logger.debug("The CSV time range config file[%s] does NOT exist !!!" % CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME)
# # update the time range of each source type from config file
#         for scrapy_class_index, time_duration_tuple in csv_time_duration_dict.items():
#             self.source_type_csv_time_duration[scrapy_class_index] = time_duration_tuple
        return csv_time_duration_dict


    def _read_csv_time_duration(self, method_index, company_number=None, company_group_number=None):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration is not None, "self.source_type_csv_time_duration should NOT be None"
        source_type_csv_time_duration = self.__parse_csv_time_duration_cfg()
        if source_type_csv_time_duration is not None:
            self.source_type_csv_time_duration = source_type_csv_time_duration


    def _update_new_csv_time_duration(self, web_scrapy_obj):
        # import pdb; pdb.set_trace()
        assert self.source_type_csv_time_duration is not None, "self.source_type_csv_time_duration should NOT be None"
        # new_csv_time_duration = web_scrapy_obj.get_new_csv_time_duration()
        self.source_type_csv_time_duration[web_scrapy_obj.SourceTypeIndex] = web_scrapy_obj.new_csv_time_duration


    def __write_new_csv_time_duration_to_cfg(self, finance_root_folderpath=None, source_type_csv_time_duration=None):
        # import pdb; pdb.set_trace()
        # csv_data_folderpath = self.__get_finance_folderpath(finance_root_folderpath)
        csv_data_folderpath = CMN.FUNC.get_finance_data_folder(self._get_finance_root_folderpath(finance_root_folderpath), company_group_number=-1)
        if source_type_csv_time_duration is None:
            source_type_csv_time_duration = self.source_type_csv_time_duration
        # g_logger.debug("Try to write CSV time range config in the folder: %s ......" % csv_data_folderpath)
        CMN.FUNC.write_csv_time_duration_config_file(CMN.DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_data_folderpath, source_type_csv_time_duration)


    def _write_new_csv_time_duration(self):
        self.__write_new_csv_time_duration_to_cfg()


    def set_company(self, company_word_list_string=None):
        if company_word_list_string is not None:
            self.company_group_set = BASE.CGS.CompanyGroupSet.create_instance_from_string(company_word_list_string)
            # self.__transform_company_word_list_to_group_set(company_word_list)
        else:
            self.company_group_set = BASE.CGS.CompanyGroupSet.get_whole_company_group_set()


	def do_scrapy(self, scrapy_method_index, *args, **kwargs):
		web_scrapy_class = CMN.FUNC.get_selenium_web_scrapy_class(scrapy_method_index)
		with web_scrapy_class() as web_scrapy_object:
			if CMN_DEF.SCRAPY_STOCK_METHOD_START <= scrapy_method_index < CMN_DEF.SCRAPY_STOCK_METHOD_END:
				for company_group_number, company_number_list in  self.company_group_set.items():
					for company_number in company_number_list:
# # Find the old CSV time duration
# 						csv_time_duration_folderpath = CMN.FUNC.get_dataset_stock_folderpath(company_number, company_group_number)
# 						csv_time_duration = CMN_FUNC.read_csv_time_duration_config_file(CMN_FUNC.CSV_DATA_TIME_DURATION_FILENAME, conf_folderpath)
# # Update the config of the scrapy object 		
# 						web_scrapy_object.CSVTimeDuration = csv_time_duration
						web_scrapy_object.CompanyNumber = company_number
						web_scrapy_object.CompanyGroupNumber = company_group_number
		
						web_scrapy_object.scrape_web_to_csv(CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]['scrapy_class_method'], *args, **kwargs)
# 						g_logger.debug("Write %d data to %s" % (len(csv_data_list), csv_filepath))
# # Write the data into CSV 
# 						filepath = CMN.FUNC.get_dataset_stock_csv_filepath(scrapy_method_index, company_number, company_group_number)
# 	        			CMN.FUNC.write_csv_file_data(csv_data_list, csv_filepath)
# # Update the time duration
# 						CMN_FUNC.write_csv_time_duration_config_file(CMN_FUNC.CSV_DATA_TIME_DURATION_FILENAME, conf_folderpath, csv_time_duration)
			else:
				raise ValueError("Unknown scrapy method index: %d" % scrapy_method_index)

