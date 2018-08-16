#! /usr/bin/python
# -*- coding: utf8 -*-

import libs.common as CMN
import common_definition as CMN_DEF
import common_function as CMN_FUNC
# import libs.common as CMN
g_logger = CMN.LOG.get_logger()


class GUIWebScrapyBase(object):

	@classmethod
    def _write_scrapy_data_to_csv(cls, csv_data_list, scrapy_method_index, company_number=None, company_group_number=None):
		csv_time_duration_folderpath = CMN.FUNC.get_dataset_xxx_folderpath(company_number, company_group_number)
		csv_time_duration = CMN_FUNC.read_csv_time_duration_config_file(CMN_FUNC.CSV_DATA_TIME_DURATION_FILENAME, conf_folderpath)

		old_csv_time_start = None
		old_csv_time_end = None
		if csv_time_duration is not None:
			old_csv_time_start = csv_time_duration[scrapy_method_index]['time_duration_start']
			old_csv_time_end = csv_time_duration[scrapy_method_index]['time_duration_end']

		new_csv_data_list_tuple = self._filter_scrapy_data(csv_data_list, old_csv_time_start, old_csv_time_end)
		if new_csv_data_list_tuple is None:
			g_logger.debug("No new data to write")
			return

		if len(new_csv_data_list_tuple) == 1:
# No old data exist
			CMN.FUNC.write_csv_file_data(csv_data_list, csv_filepath)
		else:
			new_csv_data_list_before, new_csv_data_list_after = new_csv_data_list_tuple
			if new_csv_data_list_before is not None:
				g_logger.debug("Write %d new data(before) to %s" % (len(new_csv_data_list_before), csv_filepath))	
# Write the data into CSV 
				filepath = CMN.FUNC.get_dataset_xxx_csv_filepath(scrapy_method_index, company_number, company_group_number)
			    CMN.FUNC.write_csv_file_data(csv_data_list, csv_filepath)
			if new_csv_data_list_after is not None:
				g_logger.debug("Write %d data(after) to %s" % (len(new_csv_data_list_before), csv_filepath))	
# Write the data into CSV 
				filepath = CMN.FUNC.get_dataset_xxx_csv_filepath(scrapy_method_index, company_number, company_group_number)
			    CMN.FUNC.write_csv_file_data(csv_data_list, csv_filepath)
		
# Update the time duration
		CMN_FUNC.write_csv_time_duration_config_file(CMN_FUNC.CSV_DATA_TIME_DURATION_FILENAME, conf_folderpath, csv_time_duration)


    @abstractmethod
    def scrape_web(self, scrapy_method, *args, **kwargs):
        raise NotImplementedError


    @abstractmethod
    def _filter_scrapy_data(self, csv_data_list, old_csv_time_start=None, old_csv_time_end=None):
        raise NotImplementedError


	@property
	def CSVTimeDuration(self):
		raise NotImplementedError

	@CSVTimeDuration.setter
		raise NotImplementedError


	@property
	def CompanyNumber(self):
		raise NotImplementedError

	@CompanyNumber.setter
		raise NotImplementedError


	@property
	def CompanyGroupNumber(self):
		raise NotImplementedError

	@CompanyGroupNumber.setter
		raise NotImplementedError
