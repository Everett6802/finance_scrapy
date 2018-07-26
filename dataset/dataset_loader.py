# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
import libs.base as BASE
import common_definition as DS_CMN_DEF
import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
# import common_class as DS_CMN_CLS

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
	conf_filename = CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index] + DS_CMN_DEF.DATASET_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
# Define the column name
	# import pdb; pdb.set_trace()
	column_description_list = CMN.FUNC.unicode_read_config_file_lines(conf_filename, GV.FINANCE_DATASET_DATA_FOLDERPATH)
	column_name_list = [DS_CMN_DEF.DATESET_DATE_COLUMN_NAME,]
	column_index_list = [DS_CMN_DEF.DATESET_DATE_COLUMN_INDEX,]
	if field_index_list is not None:
		full_column_description_list = column_description_list
		column_description_list = [full_column_description_list[DS_CMN_DEF.DATESET_DATE_COLUMN_INDEX],]
		start_index = (1 if (field_index_list[0] == DS_CMN_DEF.DATESET_DATE_COLUMN_INDEX) else 0)
		for field_index in field_index_list[start_index:]:
			column_description_list.append(full_column_description_list[field_index])
			column_name_list.append((DS_CMN_DEF.DATESET_COLUMN_NAME_FORMAT % (method_index, field_index)))	
			column_index_list.append(field_index)
	else:
		column_description_list_len = len(column_description_list)
		for index in range(1, column_description_list_len):
			column_name_list.append((DS_CMN_DEF.DATESET_COLUMN_NAME_FORMAT % (method_index, index)))
			column_index_list.append(index)
	# import pdb; pdb.set_trace()
# Read the data in dataset
	filepath = None
	if company_code_number is None:
		filepath = "%s/%s/%s.csv" % (GV.FINANCE_DATASET_DATA_FOLDERPATH, CMN.DEF.CSV_MARKET_FOLDERNAME, CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index])
	else:
		company_group_number = int(company_group_number)
		filepath = "%s/%s%02d/%s/%s.csv" % (GV.FINANCE_DATASET_DATA_FOLDERPATH, CMN.DEF.CSV_STOCK_FOLDERNAME, company_group_number, company_code_number, CMN.DEF.SCRAPY_MODULE_NAME_BY_METHOD_MAPPING[method_index])
	# print DS_CMN_VAR.DatasetVar.DATASET_FOLDER_PATH
	df = None
	# import pdb; pdb.set_trace()
	kwargs = {
		"names": column_name_list, 
		"parse_dates": [DS_CMN_DEF.DATESET_DATE_COLUMN_INDEX,]
	}
	if field_index_list is not None:
		kwargs["usecols"] = column_index_list
	df = pd.read_csv(filepath, **kwargs)
# Set the date column as index
	df = df.set_index(DS_CMN_DEF.DATESET_DATE_COLUMN_NAME)
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


def load_stock_price_history(company_number, overwrite_stock_price_list=None):
# overwrite_stock_price_list
# Format: Date O:OpenPrice H:HighPrice L:LowPrice C:ClosePrice; Ex. 180613 O:98.6 H: 100.5 L: 97.9 C 99.9
    df, column_description_list = load_stock_hybrid([9,], company_number)
    df.rename(columns={'0903': 'open', '0904': 'high', '0905': 'low', '0906': 'close', '0907': 'change', '0908': 'volume'}, inplace=True)
    # import pdb; pdb.set_trace()
    if overwrite_stock_price_list is not None:
		new_entry_list = []
		for line in overwrite_stock_price_list:
			line_split = line.split(' ')
			overwrite_date = line_split[0]
			overwrite_date_index = DS_CMN_FUNC.date2Date(overwrite_date)
			entry = None
			try:
# The data already exists
				loc = df.index.get_loc(overwrite_date_index)
				entry = df.iloc[loc]
			except KeyError as e:
				entry = pd.DataFrame(index=[overwrite_date_index,])
				entry.index = pd.to_datetime(entry.index)
				entry.index.name = 'date'
				new_entry_list.append(entry)
# The data does NOT exist
			for price_entry in line_split[1:]:
				price_entry_split = price_entry.split(":")
				if len(price_entry_split) != 2:
					raise ValueError("Incorrect config format: %s" % price_entry)
				if price_entry_split[0] == DS_CMN_DEF.SR_PRICE_TYPE_OPEN:
					entry['open'] = float(price_entry_split[1])
				elif price_entry_split[0] == DS_CMN_DEF.SR_PRICE_TYPE_HIGH:
					entry['high'] = float(price_entry_split[1])
				elif price_entry_split[0] == DS_CMN_DEF.SR_PRICE_TYPE_LOW:
					entry['low'] = float(price_entry_split[1])
				elif price_entry_split[0] == DS_CMN_DEF.SR_PRICE_TYPE_CLOSE:
					entry['close'] = float(price_entry_split[1])
				elif price_entry_split[0] == DS_CMN_DEF.SR_VOLUME:
					entry['volume'] = int(price_entry_split[1])
				else:
					raise ValueError("Unkown mark type: %s" % price_entry_split[0])
		# import pdb; pdb.set_trace()
# Append the new entry
		if len(new_entry_list) != 0:
			data_list = [df,]
			data_list.extend(new_entry_list)
			df = pd.concat(data_list)
			df.sort_index(ascending=True)

    return df, column_description_list
