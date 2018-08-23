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
        def get_old_csv_time_duration_if_exist(scrapy_method_index, csv_time_duration_table):
            if csv_time_duration_table is not None:
                if csv_time_duration_table.get(scrapy_method_index, None) is not None:
                    return csv_time_duration_table[scrapy_method_index]
            return None

		csv_time_duration_folderpath = CMN.FUNC.get_dataset_xxx_folderpath(company_number, company_group_number)
		csv_time_duration_table = CMN_FUNC.read_csv_time_duration_config_file(CMN_FUNC.CSV_DATA_TIME_DURATION_FILENAME, conf_folderpath)
     
        scrapy_data_time_unit = CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index][scrapy_data_time_unit]
        time_duration_start = cls._transform_time_str2obj(scrapy_data_time_unit, csv_data_list[0][0])
        time_duration_end = cls._transform_time_str2obj(scrapy_data_time_unit, csv_data_list[-1][0])

        csv_old_time_duration_tuple = get_old_csv_time_duration_if_exist(scrapy_method_index, csv_time_duration_table)

        new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_duration_start, 
            time_duration_end,
            csv_old_time_duration_tuple
        )

        csv_filepath = CMN.FUNC.get_dataset_xxx_csv_filepath(scrapy_method_index, company_number, company_group_number)
# Scrape the web data from each time duration
        for web2csv_time_duration_update in web2csv_time_duration_update_tuple: 
# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
            web2csv_time_duration_update.backup_old_csv_if_necessary(csv_filepath)
			# sub_csv_data_list = cls._filter_scrapy_data(csv_data_list, web2csv_time_duration_update)
            sub_csv_data_list = []
            if web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
                for csv_data in csv_data_list:
                    time_duration = cls._transform_time_str2obj(scrapy_data_time_unit, csv_data[0])
                    if time_duration > web2csv_time_duration_update.NewWebEnd:
                        break
                    sub_csv_data_list.append(csv_data)
            elif web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_AFTER:
                for csv_data in reserved(csv_data_list):
                    time_duration = cls.__transform_time_str2obj(scrapy_data_time_unit, csv_data[0])
                    if time_duration < web2csv_time_duration_update.NewWebStart:
                        continue
                    sub_csv_data_list.append(csv_data)
            else:
                raise ValueError("Unsupport AppendDirection: %d" % web2csv_time_duration_update.AppendDirection)

			CMN.FUNC.write_csv_file_data(sub_csv_data_list, csv_filepath)
# Append the old CSV data after the new web data if necessary
            web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath)
# Update the time duration
		CMN_FUNC.write_csv_time_duration_config_file(CMN_FUNC.CSV_DATA_TIME_DURATION_FILENAME, conf_folderpath, new_csv_extension_time_duration)


    @abstractmethod
    def scrape_web(self, scrapy_method, *args, **kwargs):
        raise NotImplementedError


    @abstractmethod
    def scrape_web_to_csv(self, scrapy_method, *args, **kwargs):
        raise NotImplementedError


	@abstractmethod
	def _transform_time_str2obj(cls, time_unit, time_str):
		raise NotImplementedError
    # @abstractmethod
    # def _find_scrapy_data_time_duration(cls, csv_data_list, scrapy_method_index):
    #     raise NotImplementedError


	# @property
	# def CSVTimeDuration(self):
	# 	raise NotImplementedError

	# @CSVTimeDuration.setter
	# 	raise NotImplementedError


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
