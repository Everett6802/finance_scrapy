# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import libs.common as CMN
import dataset_definition as DS_DEF
import dataset_variable as DS_VAR


def load_raw(method_index, company_code_number=None, company_group_number=None):
# Check method index
	if company_code_number is None:
# Market mode
		CMN.FUNC.check_scrapy_method_index_in_range(method_index, CMN.DEF.FINANCE_MODE_MARKET)
	else:
# Stock mode
		CMN.FUNC.check_scrapy_method_index_in_range(method_index, CMN.DEF.FINANCE_MODE_STOCK)
		raise NotImplementedError
# Read the column description list
	conf_filename = CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index] + DS_DEF.DATASET_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
	column_description_list = CMN.FUNC.unicode_read_config_file_lines(conf_filename, DS_VAR.DatasetVar.DATASET_FOLDER_PATH)
	column_description_list_len = len(column_description_list)
# Define the column name
	column_name_list = [DS_DEF.DATESET_DATE_COLUMN_NAME,]
	for index in range(1, column_description_list_len):
		column_name_list.append((DS_DEF.DATESET_COLUMN_NAME_FORMAT % index))
	# import pdb; pdb.set_trace()
# Read the data in dataset
	filepath = None
	if company_code_number is None:
		filepath = "%s/%s/%s.csv" % (DS_VAR.DatasetVar.DATASET_FOLDER_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index])
	else:
		filepath = "%s/%s%02d/%s/%s.csv" % (DS_VAR.DatasetVar.DATASET_FOLDER_PATH, CMN.DEF.CSV_STOCK_FOLDERNAME, company_group_number, company_code_number, CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index])
	# print DS_VAR.DatasetVar.DATASET_FOLDER_PATH
	df = pd.read_csv(filepath, names=column_name_list, parse_dates=[0,])
	df = df.set_index(DS_DEF.DATESET_DATE_COLUMN_NAME)
	return df, column_description_list
