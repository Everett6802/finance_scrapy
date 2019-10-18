#! /usr/bin/python
# -*- coding: utf8 -*-

import time
import copy
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

import scrapy.common as CMN
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()

STATEMENT_DOG_SHEET_INTERVAL_LAST_3_YEARS = 0
STATEMENT_DOG_SHEET_INTERVAL_LAST_5_YEARS = 1
STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS = 2
STATEMENT_DOG_SHEET_INTERVAL_SELF_DEFINE = 3
STATEMENT_DOG_USERNAME = "everett6802@hotmail.com"
STATEMENT_DOG_PASSWORD = "ntza00010"
PRINT_SCRAPY = True


def _statementdog_login(driver):
    # driver.get("http://www.statementdog.com/analysis")
    # time.sleep(3)
# Wait for button to login
    wait = WebDriverWait(driver, 10)
    element_table = wait.until(
        EC.presence_of_element_located((By.ID, "member-login"))
    )
    login_btn = driver.find_element_by_id("member-login")
    login_btn.click()
    # time.sleep(5)
# Wait for login dialog
    wait = WebDriverWait(driver, 10)
    element_table = wait.until(
        EC.presence_of_element_located((By.ID, "user_email"))
    )
    email_input = driver.find_element_by_id("user_email")
    email_input.send_keys(STATEMENT_DOG_USERNAME)
    password_input = driver.find_element_by_id("user_password")
    password_input.send_keys(STATEMENT_DOG_PASSWORD)
    time.sleep(1)
    password_input.submit()
    time.sleep(3)


def __swtich_report_menu(driver, report_menu_index):
    report_menu = driver.find_element_by_id("report-menu")
# Does NOT work
    # report_menu_select = Select(report_menu)
    # report_menu_select.selectByIndex(1)
    report_menu_items = report_menu.find_elements_by_tag_name("li")
    report_menu_items[report_menu_index].click()


def __swtich_sheet_interval(driver, sheet_interval_index):
    sheet_interval = driver.find_element_by_class_name("sheet-ctrl-option-btn-group")
    sheet_interval_items = sheet_interval.find_elements_by_tag_name("li")
    sheet_interval_items[sheet_interval_index].click()


def __parse_item_table(driver):
    table_element = driver.find_element_by_id("itemTable")
    td_elements = table_element.find_elements(By.TAG_NAME, "td")
    data_name_list = []
    for td_element in td_elements:
        data_name_list.append(td_element.text)
    return data_name_list


def __parse_data_table(driver, max_data_count=None):
    table_element = driver.find_element_by_id("dataTable")
    tr_elements = table_element.find_elements(By.TAG_NAME, "tr")
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
# Check if the latest data is empty
    data_list_len = len(data_list)
    if data_list_len > 0:
        assert data_list_len >= 2, "Incorrect latest data: %s" % data_list
        try:
            float(data_list[-1][1])
        except ValueError as e:
            del data_list[-1]
    start_data_index = 0
    if max_data_count is not None:
        max_data_count = min(max_data_count, data_list_len)
    	start_data_index = -1 * max_data_count
    return data_list[start_data_index:]


def __scrape_table_data(driver, report_menu_index, sheet_interval_index):
    if report_menu_index is not None:
        __swtich_report_menu(driver, report_menu_index)
        time.sleep(1)
    if sheet_interval_index is not None:
        __sheet_interval_menu(driver, sheet_interval_index)
        time.sleep(1)

    wait = WebDriverWait(driver, 10)
    element_table = wait.until(
        EC.presence_of_element_located((By.ID, "itemTable"))
    )
# Parse Table
    data_name_list = __parse_item_table(driver)
    data_list = __parse_data_table(driver)
    # if PRINT_SCRAPY: __print_table_scrapy_result(data_list, data_name_list)
    return (data_list, data_name_list)


def _scrape_monthly_revenue_growth_rate_(driver, report_menu_index, sheet_interval_index, *args, **kwargs):
    # import pdb; pdb.set_trace()
    data_list, data_name_list = __scrape_table_data(driver, report_menu_index, sheet_interval_index)
    for data in data_list:
        time_str = str(data[0])
        assert len(time_str) == 6, "The length of the time string[%s] should be 6, not %d" % (time_str, len(time_str))
        data[0] = CMN.DEF.MONTH_STRING_FORMAT % (int(time_str[0:4]), int(time_str[4:]))
    return (data_list, data_name_list)


def _scrape_eps_growth_rate_(driver, report_menu_index, sheet_interval_index, *args, **kwargs):
    # import pdb; pdb.set_trace()
    data_list, data_name_list = __scrape_table_data(driver, report_menu_index, sheet_interval_index)
    for data in data_list:
        time_str = str(data[0])
        assert len(time_str) == 5, "The length of the time string[%s] should be 5, not %d" % (time_str, len(time_str))
        data[0] = CMN.DEF.QUARTER_STRING_FORMAT % (int(time_str[0:4]), int(time_str[4:]))
    return (data_list, data_name_list)


def _scrape_net_asset_value_(driver, report_menu_index, sheet_interval_index, *args, **kwargs):
    # import pdb; pdb.set_trace()
    data_list, data_name_list = __scrape_table_data(driver, report_menu_index, sheet_interval_index)
    for data in data_list:
        time_str = str(data[0])
        assert len(time_str) == 5, "The length of the time string[%s] should be 5, not %d" % (time_str, len(time_str))
        data[0] = CMN.DEF.QUARTER_STRING_FORMAT % (int(time_str[0:4]), int(time_str[4:]))
    return (data_list, data_name_list)


class StatementDogWebScrapyMeta(type):

    __ATTRS = {
        "_scrape_monthly_revenue_growth_rate_": _scrape_monthly_revenue_growth_rate_,
        "_scrape_eps_growth_rate_": _scrape_eps_growth_rate_,
        "_scrape_net_asset_value_": _scrape_net_asset_value_,
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class StatementDogScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = StatementDogWebScrapyMeta

    __STATEMENT_DOG_HOME_URL = 'http://www.statementdog.com/analysis'


    __MARKET_SCRAPY_CFG = {
    }

    __STOCK_SCRAPY_CFG = {
        "monthly revenue growth rate": { # 月營收成長率
            "url_format": __STATEMENT_DOG_HOME_URL + "/tpe/%s/monthly-revenue-growth-rate",
            "report_menu_description_list": [u"單月營收年增率", u"單月每股營收年增率", u"單月營收月增率",],
            # "report_menu_default_index": 0,
            "sheet_interval_description_list": None, # Uselesss, only for compatibility
            # "sheet_interval_default_index": None, # Uselesss, only for compatibility
        },
        "eps growth rate": { # 每股盈餘成長率
            "url_format": __STATEMENT_DOG_HOME_URL + "/tpe/%s/eps-growth-rate",
            "report_menu_description_list": [u"EPS年增率", u"EPS季增率",],
            # "report_menu_default_index": 0,
            "sheet_interval_description_list": [u"季報", u"年報",]
            # "sheet_interval_default_index": None, # Uselesss, only for compatibility
        },
        "net asset value": { # 每股淨值
            "url_format": __STATEMENT_DOG_HOME_URL + "/tpe/%s/nav",
            "report_menu_description_list": None, # Uselesss, only for compatibility,
            # "report_menu_default_index": 0,
            "sheet_interval_description_list": [u"季報", u"年報",]
            # "sheet_interval_default_index": None, # Uselesss, only for compatibility
        },
    }

    SCRAPY_TRANSFROM_CFG = {
    }

    __MARKET_URL = {key: value["url"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_REPORT_MENU_DESCRIPTION = {key: value["report_menu_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_REPORT_MENU_DEFAULT_INDEX = {key: value["report_menu_default_index"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_SHEET_INTERVAL_DESCRIPTION = {key: value["sheet_interval_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_SHEET_INTERVAL_DEFAULT_INDEX = {key: value["sheet_interval_default_index"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_REPORT_MENU_DESCRIPTION = {key: value["report_menu_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_REPORT_MENU_DEFAULT_INDEX = {key: value["report_menu_default_index"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_SHEET_INTERVAL_DESCRIPTION = {key: value["sheet_interval_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_SHEET_INTERVAL_DEFAULT_INDEX = {key: value["sheet_interval_default_index"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    __REPORT_MENU_DESCRIPTION = {}
    __REPORT_MENU_DESCRIPTION.update(__MARKET_REPORT_MENU_DESCRIPTION)
    __REPORT_MENU_DESCRIPTION.update(__STOCK_REPORT_MENU_DESCRIPTION)

    # __REPORT_MENU_DEFAULT_INDEX = {}
    # __REPORT_MENU_DEFAULT_INDEX.update(__MARKET_REPORT_MENU_DEFAULT_INDEX)
    # __REPORT_MENU_DEFAULT_INDEX.update(__STOCK_REPORT_MENU_DEFAULT_INDEX)

    __SHEET_INTERVAL_DESCRIPTION = {}
    __SHEET_INTERVAL_DESCRIPTION.update(__MARKET_SHEET_INTERVAL_DESCRIPTION)
    __SHEET_INTERVAL_DESCRIPTION.update(__STOCK_SHEET_INTERVAL_DESCRIPTION)

    # __SHEET_INTERVAL_DEFAULT_INDEX = {}
    # __SHEET_INTERVAL_DEFAULT_INDEX.update(__MARKET_SHEET_INTERVAL_DEFAULT_INDEX)
    # __SHEET_INTERVAL_DEFAULT_INDEX.update(__STOCK_SHEET_INTERVAL_DEFAULT_INDEX)

    SCRAPY_NEED_TRANSFROM_METHOD_LIST = SCRAPY_TRANSFROM_CFG.keys()
    SCRAPY_TRANSFROM_METHOD_LIST = [value[0] for value in SCRAPY_TRANSFROM_CFG.values()]
    SCRAPY_TRANSFROM_METHOD_CFG_LIST = [value[1] for value in SCRAPY_TRANSFROM_CFG.values()]

    __FUNC_PTR = {
        "monthly revenue growth rate": _scrape_monthly_revenue_growth_rate_,
        "eps growth rate": _scrape_eps_growth_rate_,
        "net asset value": _scrape_net_asset_value_,
    }


    def __init__(self, **cfg):
        super(StatementDogScrapy, self).__init__()
# For the variables which are NOT changed during scraping
        self.xcfg = self._update_cfg_dict(cfg)

        self.webdriver = None


    def __enter__(self):
        self.webdriver = webdriver.Chrome()
        return self


    def __exit__(self, type, msg, traceback):
        if self.webdriver is not None:
            self.webdriver.quit()
        return False


    def scrape_web(self, *args, **kwargs):
# Login
        self.webdriver.get(self.__STATEMENT_DOG_HOME_URL)
        _statementdog_login(self.webdriver)

        url = None
        # import pdb; pdb.set_trace()
        scrapy_method = self.scrapy_method
        scrapy_kwargs = copy.deepcopy(kwargs)
        try:
            scrapy_method_index = self.SCRAPY_NEED_TRANSFROM_METHOD_LIST.index(scrapy_method)
            scrapy_method = self.SCRAPY_TRANSFROM_METHOD_LIST[scrapy_method_index]
            scrapy_kwargs.update(self.SCRAPY_TRANSFROM_METHOD_CFG_LIST[scrapy_method_index])
        except ValueError:
            pass
        finally:
            if self.__MARKET_URL.get(scrapy_method, None) is not None:
                url = self.__MARKET_URL[scrapy_method]
            elif self.__STOCK_URL_FORMAT.get(scrapy_method, None) is not None:
                url_format = self.__STOCK_URL_FORMAT[scrapy_method]
                url = url_format % self.company_number
            else:
                raise ValueError("Unknown scrapy method: %s" % scrapy_method)
        self.webdriver.get(url)
        # scrapy_kwargs['table_xpath'] = self.__TABLE_XPATH[scrapy_method]
        scrapy_kwargs['max_data_count'] = self.xcfg['max_data_count']
        # import pdb; pdb.set_trace()
        report_menu_index = None
        if scrapy_kwargs.has_key("report_menu_index"):
           report_menu_index = scrapy_kwargs.pop("report_menu_index")
        sheet_interval_index = None
        if scrapy_kwargs.has_key("sheet_interval_index"):
           sheet_interval_index = scrapy_kwargs.pop("sheet_interval_index ")
        return (self.__FUNC_PTR[scrapy_method])(self.webdriver, report_menu_index, sheet_interval_index, *args, **scrapy_kwargs)


	@CompanyNumber.setter
	def CompanyNumber(self, company_number):
		if self.company_number != company_number:
			self.company_number_changed = True
		super(StatementDogScrapy, self).CompanyNumber = company_number


if __name__ == '__main__':
	with StatementDogScrapy() as statement_dog:
		statement_dog.CompanyNumber = "2367"
		# import pdb; pdb.set_trace()
		for scrapy_method in StatementDogWebScrapy.get_scrapy_method_list():
			statement_dog.ScrapyMethod = scrapy_method
			statement_dog.scrape()
			print "\n"
		# statement_dog.scrape("cashflow statement")
		print "Done"
