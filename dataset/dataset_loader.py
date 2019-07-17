# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
from scrapy import common as CMN
from scrapy.common.common_variable import GlobalVar as GV
from scrapy import libs as LIBS
# import libs.selenium as SL
import common_definition as DS_CMN_DEF
import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
# import common_class as DS_CMN_CLS

g_profile_lookup = LIBS.CP.CompanyProfile.Instance()


def load_raw(method_index, company_number=None, field_index_list=None, company_group_number=None, check_data_duplicate=True):
	'''
	CAUTION: 
	The start field index must be 1
	since the 0 column in the dataset must be date
	'''

	conf_filename = None
	data_time_unit = None
	# import pdb; pdb.set_trace()
# Check method index
	# CMN.FUNC.check_scrapy_method_index_in_range(method_index, (CMN.DEF.FINANCE_MODE_MARKET if (company_number is None) else CMN.DEF.FINANCE_MODE_STOCK))
	conf_filename = CMN.DEF.SCRAPY_CSV_FILENAME[method_index] + CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
	data_time_unit = CMN.DEF.SCRAPY_METHOD_DATA_TIME_UNIT[method_index]
	if company_number is not None:
		if company_group_number is None:
			profile_lookup = LIBS.CP.CompanyProfile.Instance()
			company_group_number = profile_lookup.lookup_company_group_number(company_number)
# Define the column name
# Read the column description list
	# import pdb; pdb.set_trace()
	field_description_folder = "%s/%s" % (GV.FINANCE_DATASET_DATA_FOLDERPATH, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME)
	column_description_list = CMN.FUNC.unicode_read_config_file_lines(conf_filename, field_description_folder)
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
	filepath = CMN.FUNC.get_finance_data_csv_filepath(method_index, GV.FINANCE_DATASET_DATA_FOLDERPATH, company_group_number, company_number)
	df = None
	# import pdb; pdb.set_trace()
# Set the parameters for reading data from CSV
	kwargs = {
		"names": column_name_list, 
		"parse_dates": [DS_CMN_DEF.DATESET_DATE_COLUMN_INDEX,]
	}
	if data_time_unit != CMN.DEF.DATA_TIME_UNIT_DAY:
		if data_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
			# kwargs["date_parser"] = lambda x: pd.datetime.strptime(CMN.FUNC.transform_month2date_str(x), '%Y-%m-%d')
			kwargs["date_parser"] = lambda x: pd.datetime.strptime(CMN.FUNC.transform_month2date_str(x), '%Y-%m-%d')
		elif data_time_unit == CMN.DEF.DATA_TIME_UNIT_QUARTER:
			kwargs["date_parser"] = lambda x: pd.datetime.strptime(CMN.FUNC.transform_quarter2date_str(x), '%Y-%m-%d')
		elif data_time_unit == CMN.DEF.DATA_TIME_UNIT_YEAR:
			kwargs["date_parser"] = lambda x: pd.datetime.strptime(CMN.FUNC.transform_year2date_str(x), '%Y-%m-%d')
		else:
			raise RuntimeError("Unsupport URL time unit: %d" % data_time_unit)
	if field_index_list is not None:
		kwargs["usecols"] = column_index_list
	df = pd.read_csv(filepath, **kwargs)
# Set the date column as index
	df = df.set_index(DS_CMN_DEF.DATESET_DATE_COLUMN_NAME)
	if check_data_duplicate:
		if len(df) != 0 and type(df.index.get_loc(df.index[0])) != int:
			raise ValueError("Duplicate date occurs in %s" % filepath)
	return df, column_description_list


def load_hybrid(method_index_list, company_number=None, field_index_dict=None, company_group_number=None):
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
	if type(method_index_list) is not list:
		if type(method_index_list) is int:
			method_index_list = [method_index_list,]
		else:
			raise ValueError("Unsupport data type of method_index_list: %s" % type(method_index_list))

# Check the time units of the methods are identical
	SCRAPY_METHOD_DATA_TIME_UNIT = CMN.DEF.SCRAPY_METHOD_DATA_TIME_UNIT
	data_time_unit = [SCRAPY_METHOD_DATA_TIME_UNIT[method_index] for method_index in method_index_list]
	if len(filter(lambda time_unit: time_unit != data_time_unit[0], data_time_unit)) != 0:
		raise ValueError("The time unit[%s] are NOT identical" % data_time_unit)

	df = None
	column_description_list = []
	data_time_unit = None
	# import pdb; pdb.set_trace()
	for method_index in method_index_list:
		if data_time_unit is None:
			data_time_unit
		field_index_list = (field_index_dict.get(method_index, None) if (field_index_dict is not None) else None)
		df_new, column_description_list_new = load_raw(method_index, company_number, field_index_list, company_group_number)
		if df is None:
			df = df_new
			column_description_list.extend(column_description_list_new)
		else:
			df = pd.merge(df, df_new, right_index=True, left_index=True)
			column_description_list.extend(column_description_list_new[1:])
	return df, column_description_list


def load_market_hybrid(method_index_list, field_index_dict=None):
	return load_hybrid(method_index_list, field_index_dict=field_index_dict)


def load_stock_hybrid(method_index_list, company_number, field_index_dict=None, company_group_number=None):
	return load_hybrid(method_index_list, company_number, field_index_dict=field_index_dict, company_group_number=company_group_number)


# def load_selenium_market_hybrid(method_index_list, field_index_dict=None):
# 	return load_hybrid(method_index_list, field_index_dict=field_index_dict)


# def load_selenium_stock_hybrid(method_index_list, company_number, field_index_dict=None, company_group_number=None):
# 	return load_hybrid(method_index_list, company_number, field_index_dict=field_index_dict, company_group_number=company_group_number)


def transfrom_stock_price_time_unit(df, data_time_unit=CMN.DEF.DATA_TIME_UNIT_DAY):
    def day_to_new_timeunit(df, groupby_func_ptr):
        df_group_new_timeunit = df.groupby(df.index.map(groupby_func_ptr))
        # import pdb; pdb.set_trace()
        from dataset.common_class import StockPrice as PRICE
        df_entry_new_timeunit_list = []
        # import pdb; pdb.set_trace()
        for df_time, df_data in df_group_new_timeunit:
            first_date = df_data.index[0]
            open_price = df_data.ix[0]['open']
            close_price = df_data.ix[-1]['close']
            high_price = 0.0
            low_price = 100000.0
            volume = 0
            data_0902 = 0
            data_0908 = 0
            change = 0.0
            for index, row in df_data.iterrows():
                if row['high'] > high_price:
                    high_price = row['high']
                if row['low'] < low_price:
                    low_price = row['low']
                volume += row['volume']
                data_0902 += row['0902']
                data_0908 += row['0908']
            print "%s O:%s H:%s L:%s C:%s V:%d" % (first_date, PRICE(open_price), PRICE(high_price), PRICE(low_price), PRICE(close_price), int(volume/1000))
# Entry in new time unit
            data_list = [volume, data_0902, open_price, high_price, low_price, close_price, change, data_0908,]
            data_row = {element[0]: element[1] for element in zip(df.columns.tolist(), data_list)}
            entry = pd.DataFrame(data_row, index=[first_date,], columns=df.columns.tolist())
            df_entry_new_timeunit_list.append(entry)
        # import pdb; pdb.set_trace()
        df_new_timeunit = pd.concat(df_entry_new_timeunit_list)
        df_new_timeunit.sort_index(ascending=True)
        df_new_timeunit.index.name = 'date'
        close_diff = df_new_timeunit['close'].diff()
        close_diff.ix[0] = 0.0
        df_new_timeunit['change'] = close_diff
        return df_new_timeunit

    TIMEUNIT_GROUPBY_FUNC_PTR = {
        CMN.DEF.DATA_TIME_UNIT_WEEK: lambda t: t.year * 100 + t.weekofyear,
        CMN.DEF.DATA_TIME_UNIT_MONTH: lambda t: t.year * 100 + t.month,
        CMN.DEF.DATA_TIME_UNIT_QUARTER: lambda t: t.year * 10 + (t.month - 1) / 3,
        CMN.DEF.DATA_TIME_UNIT_YEAR: lambda t: t.year,
    }
# Chnage the time unit
    if data_time_unit != CMN.DEF.DATA_TIME_UNIT_DAY:
        df = day_to_new_timeunit(df, TIMEUNIT_GROUPBY_FUNC_PTR[data_time_unit])
    return df


def load_future_index_history():
    # import pdb; pdb.set_trace()
    df, column_description_list = load_market_hybrid(CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX)
# "開盤價", "最高價", "最低價", "最後成交價", "漲跌價", "漲跌%", "盤後交易時段成交量", "一般交易時段成交量", "合計成交量"
    new_columns={
    	('%02d01' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'open', 
    	('%02d02' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'high', 
    	('%02d03' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'low', 
    	('%02d04' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'close', 
    	('%02d05' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'change', 
    	('%02d06' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'change%', 
    	('%02d07' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'volume (after hours trading)',
    	('%02d08' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'volume (general hours trading)',
    	('%02d09' % CMN.DEF.SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX): 'total volume',  
    }
    df.rename(columns=new_columns, inplace=True)
    # import pdb; pdb.set_trace()
    return df, column_description_list


def load_stock_price_history(company_number, overwrite_stock_price_list=None, data_time_unit=CMN.DEF.DATA_TIME_UNIT_DAY):
    # import pdb; pdb.set_trace()
# overwrite_stock_price_list
# Format: Date O:OpenPrice H:HighPrice L:LowPrice C:ClosePrice; Ex. 180613 O:98.6 H: 100.5 L: 97.9 C 99.9
    df, column_description_list = load_stock_hybrid(CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX, company_number)
    # df.rename(columns={'0903': 'open', '0904': 'high', '0905': 'low', '0906': 'close', '0907': 'change', '0908': 'volume'}, inplace=True)
# "成交張數", "開盤價", "最高價", "最低價", "收盤價", "漲跌價差",
    new_columns={
    	('%02d01' % CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX): 'volume', 
    	('%02d03' % CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX): 'open', 
    	('%02d04' % CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX): 'high', 
    	('%02d05' % CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX): 'low', 
    	('%02d06' % CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX): 'close', 
    	('%02d07' % CMN.DEF.SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX): 'change',
    }
    df.rename(columns=new_columns, inplace=True)
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
					entry['volume'] = int(price_entry_split[1]/1000)
				else:
					raise ValueError("Unkown mark type: %s" % price_entry_split[0])
		# import pdb; pdb.set_trace()
# Append the new entry
		if len(new_entry_list) != 0:
			data_list = [df,]
			data_list.extend(new_entry_list)
			df = pd.concat(data_list)
			df.sort_index(ascending=True)

# Chnage the time unit if necessary
    if data_time_unit != CMN.DEF.DATA_TIME_UNIT_DAY:
        # df = day_to_new_timeunit(df, TIMEUNIT_GROUPBY_FUNC_PTR[data_time_unit])
        df = transfrom_stock_price_time_unit(df, data_time_unit)
    return df, column_description_list


def load_revenue_history(company_number):
    df, column_description_list = load_selenium_stock_hybrid(SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX, company_number)
# "單月營收", "單月年增率",
    new_columns={
    	('%02d01' % SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX): 'monthly revenue', 
    	('%02d03' % SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX): 'monthly YOY growth', 
    }
    df.rename(columns=new_columns, inplace=True)
    return df, column_description_list


def load_profitability_history(company_number):
    df, column_description_list = load_selenium_stock_hybrid(SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX, company_number)
# "毛利率", "營業利益率", '稅後純益率', "股東權益報酬率", '每股淨值', '每股稅後盈餘'
    new_columns={
        ('%02d01' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'gross profit margin', 
        ('%02d02' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'operating profit margin', 
        # ('%02d03' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'net profit margin', 
        ('%02d04' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'net profit margin', 
        ('%02d05' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'return on equity', 
        ('%02d08' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'net asset value of each share', 
        ('%02d09' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'earnings per share', 
    }
    df.rename(columns=new_columns, inplace=True)
    return df, column_description_list


def load_cashflow_statement_history(company_number):
    df, column_description_list = load_selenium_stock_hybrid(SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX, company_number)
# "營業活動現金流量", "投資活動現金流量", '理財活動現金流量', "自由現金流量'
    new_columns={
        # ('%02d01' % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX): 'gross profit margin', 
        ('%02d02' % SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX): 'cash flow from operating activities', 
        ('%02d03' % SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX): 'cash flow from investing activities', 
        ('%02d04' % SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX): 'cash flow from financing activities', 
        ('%02d07' % SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX): 'free cash flow', 
    }
    df.rename(columns=new_columns, inplace=True)
    return df, column_description_list


def load_dividend_history(company_number):
    df, column_description_list = load_selenium_stock_hybrid(SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX, company_number)
# "現金股利", "股票股利合計'
    new_columns={
        ('%02d01' % SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX): 'cash dividend', 
        ('%02d04' % SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX): 'stock dividend', 
    }
    df.rename(columns=new_columns, inplace=True)
    return df, column_description_list


def load_top10_oi_history():
    # import pdb; pdb.set_trace()
    df, column_description_list = load_market_hybrid(CMN.DEF.SCRAPY_METHOD_TFE_OPEN_INTEREST_INDEX)
# 前十大"
    new_columns={
    	('%02d02' % CMN.DEF.SCRAPY_METHOD_TFE_OPEN_INTEREST_INDEX): 'top ten', 
    }
    df.rename(columns=new_columns, inplace=True)
    # import pdb; pdb.set_trace()
    return df, column_description_list
