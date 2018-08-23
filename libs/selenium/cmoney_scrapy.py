#! /usr/bin/python
# -*- coding: utf8 -*-

import time
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
# available since 2.4.0
from selenium.webdriver.support.ui import WebDriverWait
# available since 2.26.0
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

import common_definition as CMN_DEF
import GUIWebScrapyBase as ScrapyBase
# import libs.common as CMN
# g_logger = CMN.LOG.get_logger()


PRINT_SCRAPY = True


def __parse_data_from_table0_element(table_element, table_data_count=CMN_DEF.CMONEY_TABLE_DEF_DATA_COUNT):
# Parse the data in the table
	tr_elements = table_element.find_elements_by_tag_name("tr")
# Parse the data name
	th_elements = tr_elements[0].find_elements_by_tag_name("th")
	data_name_list = []
	for th_element in th_elements[1:]:
		data_name_list.append(th_element.text)
# Parse the data time and data
	# data_name_list_len = len(data_name_list)
# 	data_time_list = []
# 	data_list = []
# 	tr_elements_len = len(tr_elements)
# # Restrict to the max data
# 	if table_data_count > tr_elements_len:
# 		# g_logger.warn("There are just %d data !!!" % tr_elements_len)
# 		table_data_count = tr_elements_len
# 	table_column_end_index = table_data_count + 1
# 	for tr_element in tr_elements[1:table_column_end_index]:
# 		td_elements = tr_element.find_elements_by_tag_name("td")
# 		data_time_list.append(td_elements[0].text)
# 		data_element_list = []
# 		for td_element in td_elements[1:]:
# 			data_element_list.append(td_element.text)
# 		data_list.append(data_element_list)

# 	return (data_list, data_time_list, data_name_list)
	data_list = []
	tr_elements_len = len(tr_elements)
# Restrict to the max data
	if table_data_count > tr_elements_len:
		# g_logger.warn("There are just %d data !!!" % tr_elements_len)
		table_data_count = tr_elements_len
	table_column_end_index = table_data_count + 1
	for index, tr_element in enumerate(tr_elements[1:table_column_end_index]):
		td_elements = tr_element.find_elements_by_tag_name("td")
		data_element_list = [td_elements[0].text,]
		for td_element in td_elements[1:]:
			data_element_list.append(td_element.text)
		data_list.append(data_element_list)

	return (data_list, data_name_list)


def __parse_data_from_table1_element(table_element, table_data_count=CMN_DEF.CMONEY_TABLE_DEF_DATA_COUNT):
	# import pdb; pdb.set_trace()
	tr_elements = table_element.find_elements_by_tag_name("tr")
	data_name_list = None
	data_list = None
	# data_time_list = None
	# # import pdb; pdb.set_trace()
	table_column_end_index = None
# 	for tr_element in tr_elements:
# 		# print tr_element.text
# 		row_list = tr_element.text.split(' ')
# 		row_list_len = len(row_list)
# # Restrict to the max data
# 		if table_data_count > row_list_len:
# 			# g_logger.warn("There are just %d data !!!" % row_list_len)
# 			table_data_count = row_list_len

# 		if data_time_list is None:
# 			table_column_end_index = table_data_count + 1
# 			data_time_list = []
# 			data_time_list.extend(row_list[1:table_column_end_index])
# 			data_name_list = []
# # CAUTION: Can't write in this way
# 			# data_list = [[],] * data_time_list_len
# 			data_list = []
# 			for i in range(len(data_time_list)):
# 				data_list.append([])
# 		else:
# 			data_name_list.append(row_list[0])
# 			for index, data in enumerate(row_list[1:table_column_end_index]):
# 				data_list[index].append(data)

# 	return (data_list, data_time_list, data_name_list)
	for tr_element in tr_elements:
		# print tr_element.text
		row_list = tr_element.text.split(' ')
		row_list_len = len(row_list)
# Restrict to the max data
		if table_data_count > row_list_len:
			# g_logger.warn("There are just %d data !!!" % row_list_len)
			table_data_count = row_list_len

		if data_list is None:
			table_column_end_index = table_data_count + 1
			# data_time_list = []
			# data_time_list.extend(row_list[1:table_column_end_index])
			data_list = [[data_time,] for data_time in row_list[1:table_column_end_index]]
# CAUTION: Can't write in this way
			# data_list = [[],] * data_time_list_len
			data_name_list = []
		else:
			data_name_list.append(row_list[0])
			for index, data in enumerate(row_list[1:table_column_end_index]):
				data_list[index].append(data)

	return (data_list, data_name_list)


def __parse_data_from_table(driver, table_element_parse_func, *args, **kwargs):
	assert kwargs.get("table_xpath", None) is not None, "The kwargs::table_xpath is NOT found"
	table_xpath = kwargs["table_xpath"]

# Wait for the table
	wait = WebDriverWait(driver, 10)
	table_element = wait.until(
		EC.presence_of_element_located((By.XPATH, table_xpath))
	)
	# table_element = driver.find_element_by_xpath(table_xpath)
# Argument setting
	# import pdb; pdb.set_trace()
# table_data_count
	table_data_count = None
	args_len = len(args)
	if args_len >= 1:
		table_data_count = args[0]
	if kwargs.get("table_data_count", None) is not None:
		if table_data_count is not None:
			assert table_data_count == kwargs["table_data_count"], "Duplicate table_data_count arguments"
			pass
		else:
			table_data_count = kwargs["table_data_count"]
	if table_data_count is None:
		table_data_count = CMN_DEF.CMONEY_TABLE_DEF_DATA_COUNT
	assert table_data_count >= 1, "The table_data_count[%d] should be greater than 1" % table_data_count

	(data_list, data_name_list) = table_element_parse_func(table_element, table_data_count)
	if PRINT_SCRAPY: __print_table_scrapy_result(data_list, data_name_list)
	return (data_list, data_name_list)


def __print_table_scrapy_result(data_list, data_name_list):
	# import pdb; pdb.set_trace()
	data_time_list_len = len(data_time_list)
	print "  ".join(data_name_list)
	for index in range(data_time_list_len):
		print "%s: %s" % (data_time_list[index], "  ".join(data_list[index]))


def _scrape_dividend_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table0_element, *args, **kwargs)


def _scrape_revenue_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table0_element, *args, **kwargs)


def _scrape_income_statement_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_balance_sheet_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_cashflow_statement_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_profitability_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_business_performance_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_management_capacity_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_financial_structure_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


def _scrape_solvency_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)


class CMoneyWebScrapyMeta(type):

	__ATTRS = {
# market start
# market end
# stock start
		"_scrape_dividend_": _scrape_dividend_,
		"_scrape_revenue_": _scrape_revenue_,
		"_scrape_income_statement_": _scrape_income_statement_,
		"_scrape_balance_sheet_": _scrape_balance_sheet_,
		"_scrape_cashflow_statement_": _scrape_cashflow_statement_,
		"_scrape_profitability_": _scrape_profitability_,
		"_scrape_business_performance_": _scrape_business_performance_,
		"_scrape_management_capacity_": _scrape_management_capacity_,
		"_scrape_financial_structure_": _scrape_financial_structure_,
		"_scrape_solvency_": _scrape_solvency_,
# stock end
	}

	def __new__(mcs, name, bases, attrs):
		attrs.update(mcs.__ATTRS)
		return type.__new__(mcs, name, bases, attrs)


class CMoneyWebScrapy(ScrapyBase.GUIWebScrapyBase):

	__metaclass__ = CMoneyWebScrapyMeta

	__CMONEY_ULR_PREFIX = "https://www.cmoney.tw/finance/"

	__MARKET_SCRAPY_CFG = {
	}

	__STOCK_SCRAPY_CFG = {
		"dividend": { # 股利政策
			"url_format": __CMONEY_ULR_PREFIX + "f00027.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div/div/div[2]/table",
			"table_time_unit_list": ["&o=1",], # Uselesss, only for compatibility
			"table_time_unit_description_list": [u"Dummy",], # Uselesss, only for compatibility
		},
		"revenue": {
			"url_format": __CMONEY_ULR_PREFIX + "f00029.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li[4]/article/div/div/div/table",
			"table_time_unit_list": ["&o=1", "&o=2",],
			"table_time_unit_description_list": [u"月營收", u"合併月營收",],
		},
		"income statement": {
			"url_format": __CMONEY_ULR_PREFIX + "f00040.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div[2]/div/table",
			"table_time_unit_list": ["&o=4", "&o=3",],
			"table_time_unit_description_list": [u"季合併財務比率", u"年合併財務比率",],
		},
		"balance sheet": {
			"url_format": __CMONEY_ULR_PREFIX + "f00041.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div[2]/div/table",
			"table_time_unit_list": ["&o=5", "&o=4", "&o=6",],
			"table_time_unit_description_list": [u"季合併損益表(單季)", u"年合併損益表",],
		},
		"cashflow statement": {
			"url_format": __CMONEY_ULR_PREFIX + "f00042.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div[2]/div/table",
			"table_time_unit_list": ["&o=5", "&o=4", "&o=6",],
			"table_time_unit_description_list": [u"季合併現金流量表(單季)", u"年合併現金流量表", u"季合併現金流量表(累計)",],
		},
		"profitability": { # 獲利能力
			"url_format": __CMONEY_ULR_PREFIX + "f00043.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li[2]/article/div/div/div/table",
			"table_time_unit_list": ["&o=4", "&o=3",],
			"table_time_unit_description_list": [u"季合併財務比率", u"年合併財務比率",],
		},
		"business performance": { # 經營績效
			"url_format": __CMONEY_ULR_PREFIX + "f00043.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li[3]/article/div/div/div/table",
			"table_time_unit_list": ["&o=4", "&o=3",],
			"table_time_unit_description_list": [u"季合併財務比率", u"年合併財務比率",],
		},
		"management capacity": { # 經營能力
			"url_format": __CMONEY_ULR_PREFIX + "f00043.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li[4]/article/div/div/div/table",
			"table_time_unit_list": ["&o=4", "&o=3",],
			"table_time_unit_description_list": [u"季合併財務比率", u"年合併財務比率",],
		},
		"financial structure": { # 財務結構
			"url_format": __CMONEY_ULR_PREFIX + "f00043.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li[5]/article/div/div/div/table",
			"table_time_unit_list": ["&o=4", "&o=3",],
			"table_time_unit_description_list": [u"季合併財務比率", u"年合併財務比率",],
		},
		"solvency": { # 償債能力
			"url_format": __CMONEY_ULR_PREFIX + "f00043.aspx?s=%s",
			"table_xpath": "//*[@id=\"MainContent\"]/ul/li[6]/article/div/div/div/table",
			"table_time_unit_list": ["&o=4", "&o=3",],
			"table_time_unit_description_list": [u"季合併財務比率", u"年合併財務比率",],
			"table_time_order_reserved": True,
		},
	}

	__MARKET_URL = {}

	__MARKET_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
	__MARKET_TIME_UNIT_LIST = {key: value["table_time_unit_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
	__MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

	__STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	__STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	__STOCK_TIME_UNIT_LIST = {key: value["table_time_unit_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	__STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

	__TABLE_XPATH = {}
	__TABLE_XPATH.update(__MARKET_TABLE_XPATH)
	__TABLE_XPATH.update(__STOCK_TABLE_XPATH)

	__TIME_UNIT_LIST = {}
	__TIME_UNIT_LIST.update(__MARKET_TIME_UNIT_LIST)
	__TIME_UNIT_LIST.update(__STOCK_TIME_UNIT_LIST)

	__TIME_UNIT_DESCRIPTION_LIST = {}
	__TIME_UNIT_DESCRIPTION_LIST.update(__MARKET_TIME_UNIT_DESCRIPTION_LIST)
	__TIME_UNIT_DESCRIPTION_LIST.update(__STOCK_TIME_UNIT_DESCRIPTION_LIST)

	__FUNC_PTR = {
# market start
# market end
# stock start
		"dividend": _scrape_dividend_,
		"revenue": _scrape_revenue_,
		"income statement": _scrape_income_statement_,
		"balance sheet": _scrape_balance_sheet_,
		"cashflow statement": _scrape_cashflow_statement_,
		"profitability": _scrape_profitability_,
		"business performance": _scrape_business_performance_,
		"management capacity": _scrape_management_capacity_,
		"financial structure": _scrape_financial_structure_,
		"solvency": _scrape_solvency_,
# stock end
	}


	@classmethod
	def _transform_time_str2obj(cls, time_unit, time_str):
        time_obj = None
        if time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
            year_str = time_str[0:4]
            month_str = time_str[4:]
            time_obj = CMN.CLS.FinanceMonth.from_string("%s-%s" % (year_str, month_str))
        elif time_unit == CMN.DEF.DATA_TIME_UNIT_QUARTER:
            time_obj = CMN.CLS.FinanceQuarter.from_string(time_str)
        else:
            raise ValueError("Unsupport time unit[%d] for transform" % scrapy_data_time_unit)
        return time_obj


	@classmethod
	def get_scrapy_method_list(cls):
		return cls.__FUNC_PTR.keys()


	@classmethod
	def print_scrapy_method(cls):
		print ", ".join(cls.get_scrapy_method_list())


	@classmethod
	def print_scrapy_method_time_unit_description(cls, scrapy_method):
		print ", ".join(cls.__TIME_UNIT_DESCRIPTION_LIST[scrapy_method])


	def __init__(self):
		# self.url = url
		self.webdriver = None
		# self.csv_time_duration = None
		self.company_number = None
		self.company_group_number = None
		self.is_annual = True


	def __enter__(self):
		self.webdriver = webdriver.Chrome()
		return self


	def __exit__(self, type, msg, traceback):
		if self.webdriver is not None:
			self.webdriver.quit()
		return False


	def scrape_web(self, scrapy_method, *args, **kwargs):
		url = None
		# import pdb; pdb.set_trace()
		if self.__MARKET_URL.get(scrapy_method, None) is not None:
			url = self.__MARKET_URL[scrapy_method]
		elif self.__STOCK_URL_FORMAT.get(scrapy_method, None) is not None:
			url_format = self.__STOCK_URL_FORMAT[scrapy_method]
# stock_time_unit
			stock_time_unit = None
			if kwargs.get("stock_time_unit", None) is not None:
				stock_time_unit = kwargs["stock_time_unit"]
			if stock_time_unit is None:
				stock_time_unit = CMN_DEF.CMONEY_STOCK_TABLE_DEF_TIME_UNIT
			url_format += self.__STOCK_TIME_UNIT_LIST[scrapy_method][stock_time_unit]
			url = url_format % self.company_number
		else:
			raise ValueError("Unknown scrapy method: %s" % scrapy_method)
		self.webdriver.get(url)
		kwargs['table_xpath'] = self.__TABLE_XPATH[scrapy_method]
		data_list, data_name_list = (self.__FUNC_PTR[scrapy_method])(self.webdriver, *args, **kwargs)
		return data_list.reverse(), data_name_list


    def scrape_web_to_csv(self, scrapy_method_index, *args, **kwargs):
    	scrapy_method = CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]["scrapy_class_method"]
    	csv_data_list, _ = self.scrape_web(scrapy_method, *args, **kwargs)
    	self._write_scrapy_data_to_csv(csv_data_list, scrapy_method_index, self.company_number, self.company_group_number)


	# @property
	# def CSVTimeDuration(self):
	# 	return self.csv_time_duration

	# @CSVTimeDuration.setter
	# def CSVTimeDurationCSVTimeDuration(self, csv_time_duration):
	# 	self.csv_time_duration = csv_time_duration


	@property
	def CompanyNumber(self):
		return self.company_number

	@CompanyNumber.setter
	def CompanyNumber(self, company_number):
		self.company_number = company_number


	@property
	def CompanyGroupNumber(self):
		return self.company_group_number

	@CompanyGroupNumber.setter
	def CompanyGroupNumber(self, company_group_number):
		self.company_group_number = company_group_number


if __name__ == '__main__':
	with CMoneyWebScrapy() as cmoney:
		cmoney.CompanyNumber = "2367"
		kwargs = {}
		kwargs["table_data_count"] = 2
		for scrapy_method in CMoneyWebScrapy.get_scrapy_method_list():
			cmoney.scrape(scrapy_method, **kwargs)
			print "\n"
		# cmoney.scrape("dividend", **kwargs)
		# import pdb; pdb.set_trace()
		# (scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("income statement", 2)
		# import pdb; pdb.set_trace()
	# 	print "Done"
