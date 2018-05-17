# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import libs.common as CMN
import libs.base as BASE
import dataset_definition as DS_DEF
import dataset_variable as DS_VAR

g_profile_lookup = BASE.CP.CompanyProfile.Instance()


def load_raw(method_index, company_code_number=None, field_index_list=None, company_group_number=None):
	'''
	CAUTION: 
	The start field index must be 1
	since the 0 column in the dataset must be date
	'''

# Check method index
	if company_code_number is None:
# Market mode
		CMN.FUNC.check_scrapy_method_index_in_range(method_index, CMN.DEF.FINANCE_MODE_MARKET)
	else:
# Stock mode
		CMN.FUNC.check_scrapy_method_index_in_range(method_index, CMN.DEF.FINANCE_MODE_STOCK)
		if company_group_number is None:
			profile_lookup = BASE.CP.CompanyProfile.Instance()
			company_group_number = profile_lookup.lookup_company_group_number(company_code_number)
# Read the column description list
	conf_filename = CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index] + DS_DEF.DATASET_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
# Define the column name
	# import pdb; pdb.set_trace()
	column_description_list = CMN.FUNC.unicode_read_config_file_lines(conf_filename, DS_VAR.DatasetVar.DATASET_FOLDER_PATH)
	column_name_list = [DS_DEF.DATESET_DATE_COLUMN_NAME,]
	column_index_list = [DS_DEF.DATESET_DATE_COLUMN_INDEX,]
	if field_index_list is not None:
		full_column_description_list = column_description_list
		column_description_list = [full_column_description_list[DS_DEF.DATESET_DATE_COLUMN_INDEX],]
		start_index = (1 if (field_index_list[0] == DS_DEF.DATESET_DATE_COLUMN_INDEX) else 0)
		for field_index in field_index_list[start_index:]:
			column_description_list.append(full_column_description_list[field_index])
			column_name_list.append((DS_DEF.DATESET_COLUMN_NAME_FORMAT % (method_index, field_index)))	
			column_index_list.append(field_index)
	else:
		column_description_list_len = len(column_description_list)
		for index in range(1, column_description_list_len):
			column_name_list.append((DS_DEF.DATESET_COLUMN_NAME_FORMAT % (method_index, index)))
			column_index_list.append(index)
	# import pdb; pdb.set_trace()
# Read the data in dataset
	filepath = None
	if company_code_number is None:
		filepath = "%s/%s/%s.csv" % (DS_VAR.DatasetVar.DATASET_FOLDER_PATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index])
	else:
		company_group_number = int(company_group_number)
		filepath = "%s/%s%02d/%s/%s.csv" % (DS_VAR.DatasetVar.DATASET_FOLDER_PATH, CMN.DEF.CSV_STOCK_FOLDERNAME, company_group_number, company_code_number, CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index])
	# print DS_VAR.DatasetVar.DATASET_FOLDER_PATH
	df = None
	# import pdb; pdb.set_trace()
	kwargs = {
		"names": column_name_list, 
		"parse_dates": [DS_DEF.DATESET_DATE_COLUMN_INDEX,]
	}
	if field_index_list is not None:
		kwargs["usecols"] = column_index_list
	df = pd.read_csv(filepath, **kwargs)
# Set the date column as index
	df = df.set_index(DS_DEF.DATESET_DATE_COLUMN_NAME)
	return df, column_description_list


def load_hybrid(method_index_list, company_code_number=None, field_index_dict=None, company_group_number=None):
	'''
	# field_index_dict: The field index list in each method
	# Format: 
	{
		method_index1: [field_index1, field_index2,...]
		method_index2: [field_index1, field_index2,...]
		...
	}
	Caution: If the method_index is NOT in the method_index_list, 
	         the field_index_list of this method is useless
	'''
	df = None
	column_description_list = []
	# import pdb; pdb.set_trace()
	for method_index in method_index_list:
		field_index_list = (field_index_dict.get(method_index, None) if (field_index_dict is not None) else None)
		df_new, column_description_list_new = load_raw(method_index, company_code_number, field_index_list, company_group_number)
		if df is None:
			df = df_new
			column_description_list.extend(column_description_list_new)
		else:
			df = pd.merge(df, df_new, right_index=True, left_index=True)
			column_description_list.extend(column_description_list_new[1:])
	return df, column_description_list


def load_market_hybrid(method_index_list, field_index_dict=None):
	return load_hybrid(method_index_list, field_index_dict=field_index_dict)


def load_stock_hybrid(method_index_list, company_code_number, field_index_dict=None, company_group_number=None):
	return load_hybrid(method_index_list, company_code_number, field_index_dict=field_index_dict, company_group_number=company_group_number)
