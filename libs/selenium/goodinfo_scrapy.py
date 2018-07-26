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
# import libs.common as CMN
# g_logger = CMN.LOG.get_logger()


PRINT_SCRAPY = True

def _scrape_legal_persion_buy_sell_(driver, *args, **kwargs):
	import pdb; pdb.set_trace()
	table_element = driver.find_element_by_class_name("solid_1_padding_3_2_tbl")
	tr_elements = table_element.find_elements_by_tag_name("tr")
	for tr_element in tr_elements[2:]:
		td_elements = tr_element.find_elements_by_tag_name("td")
		for td_element in td_elements[1:4]:
			print td_element.text
	# 	data_name_list.append(th_element.text)
	# # return __parse_data_from_table(driver, __parse_data_from_table0_element, *args, **kwargs)



def _scrape_legal_persion_buy_sell_accumulate_(driver, *args, **kwargs):
	import pdb; pdb.set_trace()
	table_element = driver.find_element_by_class_name("solid_1_padding_3_1_tbl")
	tr_elements = table_element.find_elements_by_tag_name("tr")
	for tr_element in tr_elements[2:]:
		td_elements = tr_element.find_elements_by_tag_name("td")
		for td_element in td_elements[1:4]:
			print td_element.text


class GoodInfoWebScrapyMeta(type):

	__ATTRS = {
# market start
# market end
# stock start
		"_scrape_legal_persion_buy_sell_": _scrape_legal_persion_buy_sell_,
		"_scrape_legal_persion_buy_sell_accumulate_": _scrape_legal_persion_buy_sell_accumulate_,
# stock end
	}

	def __new__(mcs, name, bases, attrs):
		attrs.update(mcs.__ATTRS)
		return type.__new__(mcs, name, bases, attrs)


class GoodInfoWebScrapy(object):

	__metaclass__ = GoodInfoWebScrapyMeta

	__CMONEY_ULR_PREFIX = "https://goodinfo.tw/StockInfo/"

	__STOCK_SCRAPY_CFG = {
		"legal_persion_buy_sell": { # 三大法人買賣超
			"url_format": __CMONEY_ULR_PREFIX + "ShowBuySaleChart.asp?STOCK_ID=%s&CHT_CAT=DATE",
		},
		"legal_persion_buy_sell_accumulate": { # 三大法人買賣超累計
			"url_format": __CMONEY_ULR_PREFIX + "ShowBuySaleChart.asp?STOCK_ID=%s&CHT_CAT=DATE",
		},
	}

	__MARKET_URL = {}

	__STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	# __STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	# __STOCK_TIME_UNIT_LIST = {key: value["table_time_unit_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
	# __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

	__FUNC_PTR = {
# market start
# market end
# stock start
		"legal_persion_buy_sell": _scrape_legal_persion_buy_sell_,
		"legal_persion_buy_sell_accumulate": _scrape_legal_persion_buy_sell_accumulate_,
# stock end
	}


	@classmethod
	def get_scrapy_method_list(cls):
		return cls.__FUNC_PTR.keys()


	@classmethod
	def print_scrapy_method(cls):
		print ", ".join(cls.get_scrapy_method_list())


	# @classmethod
	# def print_scrapy_stock_method_time_unit_description(cls, scrapy_method):
	# 	print ", ".join(cls.__STOCK_TIME_UNIT_DESCRIPTION_LIST[scrapy_method])


	def __init__(self):
		# self.url = url
		self.webdriver = None
		self.company_number = None
		# self.is_annual = True


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
# # stock_time_unit
# 			stock_time_unit = None
# 			if kwargs.get("stock_time_unit", None) is not None:
# 				stock_time_unit = kwargs["stock_time_unit"]
# 			if stock_time_unit is None:
# 				stock_time_unit = CMN_DEF.CMONEY_STOCK_TABLE_DEF_TIME_UNIT
# 			url_format += self.__STOCK_TIME_UNIT_LIST[scrapy_method][stock_time_unit]
			url = url_format % self.company_number
		else:
			raise ValueError("Unknown scrapy method: %s" % scrapy_method)
		self.webdriver.get(url)
		# kwargs['table_xpath'] = self.__STOCK_TABLE_XPATH[scrapy_method]
		return (self.__FUNC_PTR[scrapy_method])(self.webdriver, *args, **kwargs)


	@property
	def CompanyNumber(self):
		return self.company_number

	@CompanyNumber.setter
	def CompanyNumber(self, company_number):
		self.company_number = company_number


if __name__ == '__main__':
	with GoodInfoWebScrapy() as goodinfo:
		goodinfo.CompanyNumber = "6274"
		kwargs = {}
		goodinfo.scrape("legal_persion_buy_sell", **kwargs)
		# kwargs["table_data_count"] = 2
		# for scrapy_method in GoodInfoWebScrapy.get_scrapy_method_list():
		# 	goodinfo.scrape(scrapy_method, **kwargs)
		# 	print "\n"
		# goodinfo.scrape("legal_persion_buy_sell", **kwargs)
		# import pdb; pdb.set_trace()
		# (scrapy_list, scrapy_time_list, scrapy_name_list) = goodinfo.scrape("income statement", 2)
		# import pdb; pdb.set_trace()
	# 	print "Done"
