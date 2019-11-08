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

import scrapy.common as CMN
# from libs.common.common_variable import GlobalVar as GV
# import scrapy_definition as CMN.DEF
# import scrapy_function as SC_FUNC
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()

CMONEY_TABLE0_DIVIDEND_MAX_DATA_COUNT = 8
CMONEY_TABLE1_MAX_DATA_COUNT = 8
# CMONEY_TABLE_DEF_DATA_COUNT = 1
CMONEY_STOCK_TABLE_DEF_TIME_UNIT = 0
PRINT_SCRAPY = False


def __parse_data_from_table0_element(table_element, max_data_count=None):
    # import pdb; pdb.set_trace()
# Parse the data in the table
    tr_elements = table_element.find_elements_by_tag_name("tr")
# Parse the data name
    th_elements = tr_elements[0].find_elements_by_tag_name("th")
    data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
    for th_element in th_elements[1:]:
        data_name_list.append(th_element.text)
# Parse the data time and data
    # data_name_list_len = len(data_name_list)
# 	data_time_list = []
# 	data_list = []
# 	tr_elements_len = len(tr_elements)
# # Restrict to the max data
# 	if max_data_count > tr_elements_len:
# 		# g_logger.warn("There are just %d data !!!" % tr_elements_len)
# 		max_data_count = tr_elements_len
# 	table_column_end_index = max_data_count + 1
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
    # if max_data_count > tr_elements_len:
    #     # g_logger.warn("There are just %d data !!!" % tr_elements_len)
    #     max_data_count = tr_elements_len
    # table_column_end_index = max_data_count + 1
    if max_data_count is not None:
        max_data_count = min(max_data_count, tr_elements_len)
    sub_tr_elements = tr_elements[1:(max_data_count + 1)] if max_data_count is not None else tr_elements[1:]
    # import pdb; pdb.set_trace()
    is_year_time_str = None
    time_str = None
    for index, tr_element in enumerate(sub_tr_elements):
        td_elements = tr_element.find_elements_by_tag_name("td")
        if is_year_time_str is None:
            time_str_len = len(td_elements[0].text)
            if time_str_len == 4:
                is_year_time_str = True
            elif time_str_len == 6:
                is_year_time_str = False
            else:
                raise ValueError("UnSupport time string format: %s" % td_elements[0].text)
        if is_year_time_str:
            time_str = "%s" % td_elements[0].text
        else:
            time_str = "%s-%s" % (td_elements[0].text[0:4], td_elements[0].text[4:])
        data_element_list = [time_str,]
        for td_element in td_elements[1:]:
            if td_element.text.startswith("--"):
                data_element_list.append(0.0)
            else:
                data_element_list.append(td_element.text.replace(",",""))
        data_list.append(data_element_list)
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


def __parse_data_from_table1_element(table_element, max_data_count=None):
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
# 		if max_data_count > row_list_len:
# 			# g_logger.warn("There are just %d data !!!" % row_list_len)
# 			max_data_count = row_list_len

# 		if data_time_list is None:
# 			table_column_end_index = max_data_count + 1
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
    # import pdb; pdb.set_trace()
    for tr_element in tr_elements:
        # print tr_element.text
        row_list = tr_element.text.split(' ')
        row_list_len = len(row_list)
# Restrict to the max data
        # if max_data_count > row_list_len:
        #     # g_logger.warn("There are just %d data !!!" % row_list_len)
        #     max_data_count = row_list_len
        if max_data_count is not None:
            max_data_count = min(max_data_count, tr_elements_len)

        if data_list is None:
            if max_data_count is not None:
                table_column_end_index = max_data_count + 1
            # # data_time_list = []
            # # data_time_list.extend(row_list[1:table_column_end_index])
            # data_list = [[data_time,] for data_time in row_list[1:table_column_end_index]]
            sub_row_list = row_list[1:table_column_end_index] if max_data_count is not None else row_list[1:]
            data_list = [[data_time,] for data_time in sub_row_list]
# CAUTION: Can't write in this way
            # data_list = [[],] * data_time_list_len
            # raise RuntimeError("Need to check if the date column is in the list")
            data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
        else:
            data_name_list.append(row_list[0])
            sub_row_list = row_list[1:table_column_end_index] if max_data_count is not None else row_list[1:]
            for index, data in enumerate(sub_row_list):
                if data.startswith("--"):
                    data_list[index].append(0.0)
                else:
                    data_list[index].append(data.replace(",",""))
    # import pdb; pdb.set_trace()
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
# max_data_count
    max_data_count = None
    # args_len = len(args)
    # if args_len >= 1:
    #     max_data_count = args[0]
    if kwargs.get("max_data_count", None) is not None:
        # if max_data_count is not None:
        #     assert max_data_count == kwargs["max_data_count"], "Duplicate max_data_count arguments"
        #     pass
        # else:
        max_data_count = kwargs["max_data_count"]
    # if max_data_count is None:
    #     max_data_count = CMN.DEF.CMONEY_TABLE_DEF_DATA_COUNT
    # assert max_data_count >= 1, "The max_data_count[%d] should be greater than 1" % max_data_count

    (data_list, data_name_list) = table_element_parse_func(table_element, max_data_count)
# Re-Order the data time: from old to new
    data_list.reverse()
    if PRINT_SCRAPY: __print_table_scrapy_result(data_list, data_name_list)
    return (data_list, data_name_list)


def __print_table_scrapy_result(data_list, data_name_list):
    # import pdb; pdb.set_trace()
    data_list_len = len(data_list)
    print "  ".join(data_name_list)
    for index in range(data_list_len):
        print "%s: %s" % (data_list[index][0], "  ".join(data_list[index]))


def _scrape_dividend_(driver, *args, **kwargs):
    return __parse_data_from_table(driver, __parse_data_from_table0_element, *args, **kwargs)


def _scrape_revenue_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
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


def _scrape_financial_ratio_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
    assert type(kwargs['table_xpath']) is list, "The 'table_xpath' argument type should be a list, not %s" % type(kwargs['table_xpath'])
    table_xpath_list = kwargs['table_xpath']
    total_data_list = None
    total_data_list_len = 0
    total_data_name_list = None
    for table_xpath in table_xpath_list:
        kwargs['table_xpath'] = table_xpath
        data_list, data_name_list = __parse_data_from_table(driver, __parse_data_from_table1_element, *args, **kwargs)
        if total_data_list is None:
            total_data_list = data_list
            total_data_name_list = data_name_list
            total_data_list_len = len(total_data_list)
        else:
            assert total_data_list_len == len(data_list), "The data lengthes are NOT identical: %d, %d" % (total_data_list_len, len(data_list))
            for index in range(total_data_list_len):
                total_data_list[index].extend(data_list[index][1:])
            total_data_name_list.extend(data_name_list[1:])
    # import pdb; pdb.set_trace()
    return total_data_list, total_data_name_list


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
        "_scrape_financial_ratio_": _scrape_financial_ratio_,
# stock end
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class CMoneyScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = CMoneyWebScrapyMeta

    __CMONEY_HOME_ULR = "https://www.cmoney.tw/finance/"

    __MARKET_SCRAPY_CFG = {
    }

    __STOCK_SCRAPY_CFG = {
        "dividend": { # 股利政策
            "url_format": __CMONEY_HOME_ULR + "f00027.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div/div/div[2]/table",
            "table_time_unit_url_list": ["&o=1",], # Uselesss, only for compatibility
            "table_time_unit_description_list": [u"Dummy",], # Uselesss, only for compatibility
        },
        "revenue": { # 營收盈餘
            "url_format": __CMONEY_HOME_ULR + "f00029.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li[4]/article/div/div/div/table",
            "table_time_unit_url_list": ["&o=1", "&o=2",],
            "table_time_unit_description_list": [u"月營收", u"合併月營收",],
        },
        "income statement": {
            "url_format": __CMONEY_HOME_ULR + "f00040.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div[2]/div/table",
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
        },
        "balance sheet": {
            "url_format": __CMONEY_HOME_ULR + "f00041.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div[2]/div/table",
            "table_time_unit_url_list": ["&o=5", "&o=4", "&o=6",],
            "table_time_unit_description_list": [u"季合併損益表(單季)", u"年合併損益表",],
        },
        "cashflow statement": { # 現金流量表
            "url_format": __CMONEY_HOME_ULR + "f00042.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li/article/div[2]/div/table",
            "table_time_unit_url_list": ["&o=5", "&o=4", "&o=6",],
            "table_time_unit_description_list": [u"季(單季)", u"年", u"季(累計)",],
        },
        "profitability": { # 獲利能力
            "url_format": __CMONEY_HOME_ULR + "f00043.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li[2]/article/div/div/div/table",
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
        },
        "business performance": { # 經營績效
            "url_format": __CMONEY_HOME_ULR + "f00043.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li[3]/article/div/div/div/table",
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
        },
        "management capacity": { # 經營能力
            "url_format": __CMONEY_HOME_ULR + "f00043.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li[4]/article/div/div/div/table",
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
        },
        "financial structure": { # 財務結構
            "url_format": __CMONEY_HOME_ULR + "f00043.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li[5]/article/div/div/div/table",
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
        },
        "solvency": { # 償債能力
            "url_format": __CMONEY_HOME_ULR + "f00043.aspx?s=%s",
            "table_xpath": "//*[@id=\"MainContent\"]/ul/li[6]/article/div/div/div/table",
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
            "table_time_order_reserved": True,
        },
# Multiple tables
        "financial ratio": { # 財務比率: 獲利能力,經營績效,經營能力,財務結構,償債能力
            "url_format": __CMONEY_HOME_ULR + "f00043.aspx?s=%s",
            "table_xpath": [
                "//*[@id=\"MainContent\"]/ul/li[2]/article/div/div/div/table",
                "//*[@id=\"MainContent\"]/ul/li[3]/article/div/div/div/table",
                "//*[@id=\"MainContent\"]/ul/li[4]/article/div/div/div/table",
                "//*[@id=\"MainContent\"]/ul/li[5]/article/div/div/div/table",
                "//*[@id=\"MainContent\"]/ul/li[6]/article/div/div/div/table",
            ],
            "table_time_unit_url_list": ["&o=4", "&o=3",],
            "table_time_unit_description_list": [u"季", u"年",],
            "table_time_order_reserved": True,
        },
    }

    SCRAPY_TRANSFROM_CFG = {
        "quarterly cashflow statement": ["cashflow statement", {"stock_time_unit": 0,}],
        "yearly cashflow statement": ["cashflow statement", {"stock_time_unit": 1,}],
        "quarterly financial ratio": ["financial ratio", {"stock_time_unit": 0,}],
        "yearly financial ratio": ["financial ratio", {"stock_time_unit": 1,}],
    }

    __MARKET_URL = {key: value["url"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    __TABLE_XPATH = {}
    __TABLE_XPATH.update(__MARKET_TABLE_XPATH)
    __TABLE_XPATH.update(__STOCK_TABLE_XPATH)

    __TIME_UNIT_URL_LIST = {}
    __TIME_UNIT_URL_LIST.update(__MARKET_TIME_UNIT_URL_LIST)
    __TIME_UNIT_URL_LIST.update(__STOCK_TIME_UNIT_URL_LIST)

    __TIME_UNIT_DESCRIPTION_LIST = {}
    __TIME_UNIT_DESCRIPTION_LIST.update(__MARKET_TIME_UNIT_DESCRIPTION_LIST)
    __TIME_UNIT_DESCRIPTION_LIST.update(__STOCK_TIME_UNIT_DESCRIPTION_LIST)


    SCRAPY_NEED_TRANSFROM_METHOD_LIST = SCRAPY_TRANSFROM_CFG.keys()
    SCRAPY_TRANSFROM_METHOD_LIST = [value[0] for value in SCRAPY_TRANSFROM_CFG.values()]
    SCRAPY_TRANSFROM_METHOD_CFG_LIST = [value[1] for value in SCRAPY_TRANSFROM_CFG.values()]

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
        "financial ratio": _scrape_financial_ratio_,
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    def __init__(self, **cfg):
        super(CMoneyScrapy, self).__init__()
# For the variables which are NOT changed during scraping
        # self.xcfg = {
        #     "dry_run_only": False,
        #     "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
        #     "max_data_count": None,
        # }
        # self.xcfg.update(cfg)
        self.xcfg = self._update_cfg_dict(cfg)

        # self.url = url
        self.webdriver = None
        # self.csv_time_duration = None
        # self.company_number = None
        # self.company_group_number = None
        self.is_annual = True
        # self.method_list = None


    def __enter__(self):
        self.webdriver = webdriver.Chrome()
        return self


    def __exit__(self, type, msg, traceback):
        if self.webdriver is not None:
            self.webdriver.quit()
        return False


    def scrape_web(self, *args, **kwargs):
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
# stock_time_unit
                stock_time_unit = None
                if scrapy_kwargs.get("stock_time_unit", None) is not None:
                    stock_time_unit = scrapy_kwargs["stock_time_unit"]
                if stock_time_unit is None:
                    stock_time_unit = CMONEY_STOCK_TABLE_DEF_TIME_UNIT
                url_format += self.__STOCK_TIME_UNIT_URL_LIST[scrapy_method][stock_time_unit]
                url = url_format % self.company_number
            else:
                raise ValueError("Unknown scrapy method: %s" % scrapy_method)
        self.webdriver.get(url)
        scrapy_kwargs['table_xpath'] = self.__TABLE_XPATH[scrapy_method]
        scrapy_kwargs['max_data_count'] = self.xcfg['max_data_count']
        # import pdb; pdb.set_trace()
        return (self.__FUNC_PTR[scrapy_method])(self.webdriver, *args, **scrapy_kwargs)


if __name__ == '__main__':
    import pdb; pdb.set_trace()
    with CMoneyScrapy() as cmoney:
        cmoney.CompanyNumber = "2367"
        kwargs = {}
        kwargs["max_data_count"] = 2
        for scrapy_method in CMoneyWebScrapy.get_scrapy_method_list():
            cmoney.ScrapyMethod = scrapy_method
            cmoney.scrape(**kwargs)
            print "\n"
		# cmoney.scrape("dividend", **kwargs)
		# import pdb; pdb.set_trace()
		# (scrapy_list, scrapy_time_list, scrapy_name_list) = cmoney.scrape("income statement", 2)
		# import pdb; pdb.set_trace()
	# 	print "Done"
