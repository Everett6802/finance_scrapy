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

# import scrapy_definition as SC_DEF
import scrapy_class_base.ScrapyBase as ScrapyBase

STATEMENT_DOG_SHEET_INTERVAL_LAST_3_YEARS = 0
STATEMENT_DOG_SHEET_INTERVAL_LAST_5_YEARS = 1
STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS = 2
STATEMENT_DOG_SHEET_INTERVAL_SELF_DEFINE = 3
PRINT_SCRAPY = True


def __statementdog_menu_title(driver, menu_title_index):
	elements = driver.find_elements_by_class_name("menu-title")
	elements[menu_title_index].click()


def __statementdog_menu_list(driver, list_index):
	# elements = driver.find_elements_by_class_name("menu_wrapper")
	# items = elements[1].find_elements_by_tag_name("li")
# menu-list, a ordered list
	element = driver.find_element_by_xpath("//*[@id=\"content\"]/div[1]/div[1]/ol")
	items = element.find_elements_by_tag_name("li")
	items[list_index].click()


def __goto_statementdog_menu_list(driver, menu_title_index, list_index):
	# __statementdog_stock_search(driver, company_search_string)
	__statementdog_menu_title(driver, menu_title_index)
	time.sleep(1)
	__statementdog_menu_list(driver, list_index)
	time.sleep(1)


def __switch_sheet_interval(driver):
	# element = driver.find_element_by_xpath("//*[@id=\"report_title\"]/table/tbody/tr/td[1]/div[1]")
	# element = driver.find_element_by_xpath("//*[@id=\"report_title\"]/table/tbody/tr/td[1]/ul")
	# import pdb; pdb.set_trace()
	element_sheet_interval = driver.find_element_by_xpath("//*[@id=\"report_title\"]/table/tbody/tr/td[1]/div[1]")
	try:
		time.sleep(1)
		# element_sheet_interval.text.encode('utf-8')
		element_sheet_interval.click()
	except Exception as e:
# *** UnicodeEncodeError: 'ascii' codec can't encode characters in position 138-145: ordinal not in range(128)
		print str(e)
	element_sheet_interval_options = driver.find_element_by_xpath("//*[@id=\"report_title\"]/table/tbody/tr/td[1]/ul")
# CAUTION: Can' explot 'Select', since the tag of the component is NOT 'Select'
	# select = Select(element)
	# select.selectByIndex(SC_DEF.STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS);
	items = element_sheet_interval_options.find_elements_by_tag_name("li")
	items[SC_DEF.STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS].click()
	# for item in items:
	# 	print item.text


def __parse_item_table(driver):
	table_element = driver.find_element_by_id("itemTable")
	td_elements = table_element.find_elements(By.TAG_NAME, "td")
	data_name_list = []
	for td_element in td_elements:
		data_name_list.append(td_element.text)
	return data_name_list


def __parse_data_table(driver):
	table_element = driver.find_element_by_id("dataTable")
	tr_elements = table_element.find_elements(By.TAG_NAME, "tr")
# # Time
# 	data_time_list = []
# 	th_elements = tr_elements[0].find_elements(By.TAG_NAME, "th")
# 	for th_element in th_elements:
# 		data_time_list.append(th_element.text)
# # Data
# 	data_list = []
# 	for i in range(len(data_time_list)):
# 		data_list.append([])
# 	for tr_element in tr_elements[1:]:
# 		td_elements = tr_element.find_elements(By.TAG_NAME, "td")
# 		for index, td_element in enumerate(td_elements):
# 			data_list[index].append(td_element.text)

# 	return (data_list, data_time_list)
# Time
	data_list = []
	th_elements = tr_elements[0].find_elements(By.TAG_NAME, "th")
	for th_element in th_elements:
		data_list.append([th_element.text,])
# Data
	for tr_element in tr_elements[1:]:
		td_elements = tr_element.find_elements(By.TAG_NAME, "td")
		for index, td_element in enumerate(td_elements):
			data_list[index].append(td_element.text)

	return data_list


def __print_table_scrapy_result(data_list, data_name_list):
	# import pdb; pdb.set_trace()
	data_time_list_len = len(data_time_list)
	print "  ".join(data_name_list)
	for index in range(data_time_list_len):
		print "%s: %s" % (data_time_list[index], "  ".join(data_list[index]))


def __scrape_table_data(driver, menu_title_index, list_index, *args, **kwargs):
# Switch the menu page
	__goto_statementdog_menu_list(driver, menu_title_index, list_index)
	import pdb; pdb.set_trace()
	# __switch_sheet_interval(driver)
# Wait for the table
	wait = WebDriverWait(driver, 10)
	element_table = wait.until(
		EC.presence_of_element_located((By.ID, "datasheet"))
	)

	element = driver.find_element_by_class_name("sheet-ctrl-option-btn-group")
	li = element.find_elements_by_tag_name("li")
	try:
		li[1].click()
	except Exception as e:
# *** UnicodeEncodeError: 'ascii' codec can't encode characters in position 138-145: ordinal not in range(128)
		print str(e)

# Parse Table
	data_name_list = __parse_item_table(driver)
	data_list = __parse_data_table(driver)
	if PRINT_SCRAPY: __print_table_scrapy_result(data_list, data_name_list)
	return (data_list, data_name_list)


def _scrape_income_statement_(driver, *args, **kwargs):
	return __scrape_table_data(driver, 1, 3, *args, **kwargs)


def _scrape_balance_sheet_asset_(driver, *args, **kwargs):
	return __scrape_table_data(driver, 1, 4, *args, **kwargs)


def _scrape_balance_sheet_liability_equity_(driver, *args, **kwargs):
	return __scrape_table_data(driver, 1, 5, *args, **kwargs)


def _scrape_cashflow_statement_(driver, *args, **kwargs):
	return __scrape_table_data(driver, 1, 6, *args, **kwargs)


def _scrape_dividend_(driver, *args, **kwargs):
	return __scrape_table_data(driver, 1, 7, *args, **kwargs)


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


class StatementDogWebScrapy(ScrapyBase):

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
	def get_scrapy_method_list(cls):
		return cls.__FUNC_PTR.keys()


	@classmethod
	def print_scrapy_method(cls):
		print ", ".join(cls.get_scrapy_method_list())


	def __init__(self):
		self.webdriver = None
		# self.csv_time_duration = None
		self.company_number = None
		self.company_group_number = None
		self.company_number_changed = True


	def __enter__(self):
		self.webdriver = webdriver.Chrome()
		self.webdriver.get(self.__HOME_URL)
		return self


	def __exit__(self, type, msg, traceback):
		if self.webdriver is not None:
			self.webdriver.quit()
		return False


	def scrape_web(self, *args, **kwargs):
		if self.company_number is None:
			raise ValueError("Unknown company number !!!")
		if self.__FUNC_PTR.get(self.scrapy_method, None) is None:
			raise ValueError("The scrapy method[%s] is NOT defined" % self.scrapy_method)

		# print self.webdriver.title
# Switch to certain a company
		if self.company_number_changed:
# Wait until the web element shows-up
			# element = driver.find_element_by_id("stockid")
			wait = WebDriverWait(self.webdriver, 10)
			element_stockid = wait.until(
			    EC.presence_of_element_located((By.ID, "stockid"))
			)

			element_stockid.clear()
			time.sleep(1)
			element_stockid.send_keys(self.company_number)
			time.sleep(1)
			element_stockid.send_keys(Keys.RETURN)
			self.company_number_changed = False
		self.webdriver.implicitly_wait(5) # seconds
		return (self.__FUNC_PTR[self.scrapy_method])(self.webdriver, *args, **kwargs)


	# @property
	# def CSVTimeDuration(self):
	# 	return self.csv_time_duration

	# @CSVTimeDuration.setter
	# def CSVTimeDurationCSVTimeDuration(self, csv_time_duration):
	# 	self.csv_time_duration = csv_time_duration


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


	@property
	def CompanyGroupNumber(self):
		return self.company_group_number

	@CompanyGroupNumber.setter
	def CompanyGroupNumber(self, company_group_number):
		self.company_group_number = company_group_number


if __name__ == '__main__':
	with StatementDogWebScrapy() as statement_dog:
		statement_dog.CompanyNumber = "2367"
		# import pdb; pdb.set_trace()
		for scrapy_method in StatementDogWebScrapy.get_scrapy_method_list():
			statement_dog.ScrapyMethod = scrapy_method
			statement_dog.scrape()
			print "\n"
		# statement_dog.scrape("cashflow statement")
		print "Done"
