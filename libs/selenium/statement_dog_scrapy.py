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
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.keys import Keys


def __statementdog_stock_search(driver, company_search_string):
	element = driver.find_element_by_id("stockid")
	element.send_keys(company_search_string)
	element = driver.find_element_by_xpath("//*[@id=\"navi-wrapper\"]/div[2]/div[4]/a")
	element.click()


def __statementdog_menu_title(driver, menu_title_index):
	elements = driver.find_elements_by_class_name("menu-title")
	elements[menu_title_index].click()


def __statementdog_menu_list(driver, list_index):
	elements = driver.find_elements_by_class_name("menu_wrapper")
	items = elements[1].find_elements_by_tag_name("li")
	items[list_index].click()


def __goto_statementdog_menu_list(driver, company_search_string, menu_title_index, list_index):
	__statementdog_stock_search(driver, company_search_string)
	time.sleep(2)
	__statementdog_menu_title(driver, menu_title_index)
	time.sleep(2)
	__statementdog_menu_list(driver, list_index)
	time.sleep(2)


def _scrape_income_statement_(driver, *args, **kwargs):
	pass

def _scrape_balance_sheet_asset_(driver, *args, **kwargs):
	pass


def _scrape_balance_sheet_liability_equity_(driver, *args, **kwargs):
	pass


def _scrape_cashflow_statement_(driver, *args, **kwargs):
	pass


def _scrape_dividend_(driver, *args, **kwargs):
# args[0]: company_search_string
# parse the argument
	# company_search_string = args[0]
	import pdb; pdb.set_trace()
# scrape web
	element = driver.find_element_by_id("stockid")
	element.send_keys(company_search_string)
	element1 = driver.find_element_by_xpath("//*[@id=\"navi-wrapper\"]/div[2]/div[4]/a")
	element1.click()

	elements = driver.find_elements_by_class_name("menu-title")
	elements[1].click()
	time.sleep(2)

	elements = driver.find_elements_by_class_name("menu_wrapper")
	items = elements[1].find_elements_by_tag_name("li")
	items[7].click()
	time.sleep(2)
	# __goto_statementdog_menu_list(driver, company_search_string, 1, 7)


	element = driver.find_element_by_id("datasheet")
	items = element.find_elements_by_tag_name("li")
# Find the table
	rows = items[1].find_elements(By.TAG_NAME, "tr")
# # Table Header
# 	cols = rows[0].find_elements(By.TAG_NAME, "th")
# 	for col in cols:
# 		print col.text #prints text from the element
	table_data_list = []
# Table Data
	for row in rows[1:]:
		cols = row.find_elements(By.TAG_NAME, "td") #note: index start from 0, 1 is col 2
		for col in cols:
			# print col.text #prints text from the element
			table_data_list.append(col.text)
	return table_data_list


class StatementDogWebScrapyMeta(type):

	__ATTRS = {
		"_scrape_income_statement_": _scrape_income_statement_,
		"_scrape_balance_sheet_asset_": _scrape_balance_sheet_asset_,
		"_scrape_balance_sheet_liability_equity_": _scrape_balance_sheet_liability_equity_,
		"_scrape_cashflow_statement_": _scrape_cashflow_statement_,
		"_scrape_dividend_": _scrape_dividend_,
	}

	def __new__(mcs, name, bases, attrs):
		attrs.update(mcs.__ATTRS)
		return type.__new__(mcs, name, bases, attrs)


class StatementDogWebScrapy(object):

	__metaclass__ = StatementDogWebScrapyMeta

	__HOME_URL = 'https://statementdog.com/analysis'

	__FUNC_PTR = {
		"income statement": _scrape_income_statement_,
		"balance sheet: asset": _scrape_balance_sheet_asset_,
		"balance sheet: liability & equity": _scrape_balance_sheet_liability_equity_,
		"cashflow statement": _scrape_cashflow_statement_,
		"dividend": _scrape_dividend_,
	}


	@classmethod
	def list_scrapy_method(cls):
		print ", ".join(cls.__FUNC_PTR.keys())


	def __init__(self, url):
		self.url = url
		self.webdriver = None
		self.company_number = None
		self.company_number_changed = True
		# self.compnay_name = None
		# self.company_search_string = None


	def __enter__(self):
		self.webdriver = webdriver.Chrome()
		self.webdriver.get(self.__HOME_URL)
		return self


	def __exit__(self, type, msg, traceback):
		if self.webdriver is not None:
			self.webdriver.quit()
		return False


	# def set_company(self, company_number, company_name=None):
	# 	self.company_search_string = u"%s %s" % (company_number, company_name)


	def scrape(self, scrapy_method, *args, **kwargs):
		if self.company_number is None:
			raise ValueError("Unknown company number !!!")
		if self.__FUNC_PTR.get(scrapy_method, None) is None:
			raise ValueError("The scrapy method[%s] is NOT defined" % scrapy_method)

		# print self.webdriver.title
# Switch to certain a company
		if self.company_number_changed:
			element = driver.find_element_by_id("stockid")
			element.clear()
			element.send_keys(self.company_number)
			element.send_keys(Keys.RETURN)
		return (self.__FUNC_PTR[scrapy_method])(self.webdriver, self.company_search_string, *args, **kwargs)


	@property
	def CompanyNumber(self):
		if self.company_number is None:
			raise ValueError("self.company_number is NOT set")
		return self.company_number

	@CompanyNumber.setter
	def CompanyNumber(self, company_number):
		if self.company_number != company_number:
			self.company_number_changed = True
			self.company_number = company_number


if __name__ == '__main__':
	# with StatementDogWebScrapy('https://statementdog.com/analysis') as statement_dog:
	# 	# statement_dog.list_scrapy_method()
	# 	statement_dog.set_company(u"2317", u"鴻海")
	# 	# scrapy_res = statement_dog.scrape("income statement")
	# 	scrapy_res = statement_dog.scrape("dividend")
	# 	import pdb; pdb.set_trace()
	# 	print scrapy_res

	driver = webdriver.Chrome()
	driver.get('https://statementdog.com/analysis')
	print driver.title

	import pdb; pdb.set_trace()
	# element = driver.find_element_by_class_name("navi-search")
	element = driver.find_element_by_id("stockid")
	# element.send_keys(u"2317 鴻海")
	element.clear()
	element.send_keys("2317")
	element.send_keys(Keys.RETURN)
	import pdb; pdb.set_trace()
	element.clear()	
	element.send_keys("2367")
	element.send_keys(Keys.RETURN)
	# # 提交
	# # element = driver.find_element_by_class_name("navi-search")
	# element.submit()
	# element1 = driver.find_element_by_class_name("stock_search_btn")
	element1 = driver.find_element_by_xpath("//*[@id=\"navi-wrapper\"]/div[2]/div[4]/a")
	element1.click()

	elements = driver.find_elements_by_class_name("menu-title")
	elements[1].click()
	time.sleep(2)

	elements = driver.find_elements_by_class_name("menu_wrapper")
	items = elements[1].find_elements_by_tag_name("li")
	items[7].click()
	time.sleep(2)

	element = driver.find_element_by_id("datasheet")
	items = element.find_elements_by_tag_name("li")
	# for item in items:
	#     print item.text
	# Find the table
	rows = items[1].find_elements(By.TAG_NAME, "tr")
	# Table Header
	cols = rows[0].find_elements(By.TAG_NAME, "th")
	for col in cols:
	   	print col.text #prints text from the element
	# Table Data
	for row in rows[1:]:
	    cols = row.find_elements(By.TAG_NAME, "td") #note: index start from 0, 1 is col 2
	    for col in cols:
	    	print col.text #prints text from the element

	driver.quit()
