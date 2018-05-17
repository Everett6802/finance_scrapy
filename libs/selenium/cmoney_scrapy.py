#! /usr/bin/python
# -*- coding: utf8 -*-

import time
from selenium import webdriver
import common_definition as CMN_DEF
# import libs.common as CMN
# g_logger = CMN.LOG.get_logger()


###################################################


def __parse_data_from_table_element(table_element, table_column_count=CMN_DEF.CMONEY_TABLE_DEF_COLUMN_COUNT):
	assert table_column_count >= 1 and table_column_count <= table_column_count, "The table_column_count[%d] is Out-of-Range; [1, %d]" % (table_column_count, CMN_DEF.CMONEY_TABLE_MAX_COLUMN_COUNT)
	table_column_end_index = table_column_count + 1
	tr_elements = table_element.find_elements_by_tag_name("tr")
	data_time_list = None
	data_name_list = None
	data_list = None
	# import pdb; pdb.set_trace()
	for tr_element in tr_elements:
		# print tr_element.text
		row_list = tr_element.text.split(' ')
		if data_time_list is None:
			data_time_list = []
			data_time_list.extend(row_list[1:table_column_end_index])
			data_name_list = []
# CAUTION: Can't write in this way
			# data_list = [[],] * data_time_list_len
			data_list = []
			for i in range(len(data_time_list)):
				data_list.append([])
		else:
			data_name_list.append(row_list[0])
			for index, data in enumerate(row_list[1:table_column_end_index]):
				data_list[index].append(data)
	return (data_list, data_time_list, data_name_list)


def __parse_data_from_table(driver, *args, **kwargs):
	assert kwargs.get("table_xpath", None) is not None, "The kwargs::table_xpath is NOT found"
	table_xpath = kwargs["table_xpath"]
	table_element = driver.find_element_by_xpath(table_xpath)
# Argument setting
# table_column_count
	table_column_count = None
	args_len = len(args)
	if args_len >= 1:
		table_column_count = args[0]
	if kwargs.get("table_column_count", None) is not None:
		if table_column_count is not None:
			assert table_column_count == kwargs["table_column_count"], "Duplicate table_column_count arguments"
			pass
		else:
			table_column_count = kwargs["table_column_count"]
	if table_column_count is None:
		table_column_count = CMN_DEF.CMONEY_TABLE_DEF_COLUMN_COUNT

	return __parse_data_from_table_element(table_element, table_column_count)


def _scrape_income_statement_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_balance_sheet_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_cashflow_statement_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_profitability_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_business_performance_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_management_capacity_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_financial_structure_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


def _scrape_solvency_(driver, *args, **kwargs):
	return __parse_data_from_table(driver, *args, **kwargs)


class CMoneyWebScrapyMeta(type):

	__ATTRS = {
# market start
# market end
# stock start
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


class CMoneyWebScrapy(object):

	__metaclass__ = CMoneyWebScrapyMeta

	__CMONEY_ULR_PREFIX = "https://www.cmoney.tw/finance/"

	__STOCK_SCRAPY_CFG = {
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
		},
	}

	__MARKET_URL = {}

	__STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	__STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	__STOCK_TIME_UNIT_LIST = {key: value["table_time_unit_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	__STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}


	__FUNC_PTR = {
# market start
# market end
# stock start
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
	def list_scrapy_method(cls):
		print ", ".join(cls.__FUNC_PTR.keys())


	@classmethod
	def list_scrapy_stock_method_time_unit_description(cls, scrapy_method):
		print ", ".join(cls.__STOCK_TIME_UNIT_DESCRIPTION_LIST[scrapy_method])


	def __init__(self):
		# self.url = url
		self.webdriver = None
		self.company_number = None
		self.is_annual = True


	def __enter__(self):
		self.webdriver = webdriver.Chrome()
		return self


	def __exit__(self, type, msg, traceback):
		if self.webdriver is not None:
			self.webdriver.quit()
		return False


	def scrape(self, scrapy_method, *args, **kwargs):
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
		kwargs['table_xpath'] = self.__STOCK_TABLE_XPATH[scrapy_method]
		return (self.__FUNC_PTR[scrapy_method])(self.webdriver, *args, **kwargs)


	@property
	def CompanyNumber(self):
		return self.company_number

	@CompanyNumber.setter
	def CompanyNumber(self, company_number):
		self.company_number = company_number


if __name__ == '__main__':
	with CMoneyWebScrapy() as cmoney:
		cmoney.CompanyNumber = "2367"
		# import pdb; pdb.set_trace()
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("income statement", 2)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("balance sheet", 3, stock_time_unit=0)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("cashflow statement", 4, stock_time_unit=1)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("profitability", 5, stock_time_unit=1)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("business performance", 6)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("management capacity", 7, stock_time_unit=0)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("financial structure", 8, stock_time_unit=1)
		(scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("solvency", 1)
		# import pdb; pdb.set_trace()
	# 	print scrapy_res
