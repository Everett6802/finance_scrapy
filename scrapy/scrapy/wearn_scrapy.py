#! /usr/bin/python
# -*- coding: utf8 -*-

import re
import time
# Install selenium in Anaconda
# conda install -c conda-forge selenium
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
# available since 2.4.0
from selenium.webdriver.support.ui import WebDriverWait
# available since 2.26.0
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

import scrapy.common as CMN
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()

PRINT_SCRAPY = True


def _scrape_tfe_open_interest_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
# Wait for the table
    wait = WebDriverWait(driver, 10)
    table_element = wait.until(
        EC.presence_of_element_located((By.XPATH, "/html/body/div[4]/table"))
    )

    tr_elements = table_element.find_elements_by_tag_name("tr")
# data name
    data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
    th_elements = tr_elements[1].find_elements_by_tag_name("th")
    for th_element in th_elements[1:]:
        data_name_list.append(th_element.text)
# data
    data_list = []
    for tr_element in tr_elements[2:]:
        td_elements = tr_element.find_elements_by_tag_name("td")
        mobj = re.match("([\d]{2,3})/([\d]{2})/([\d]{2})", td_elements[0].text)
        if mobj is None:
            raise ValueError("Unknown time unit string: %s" % td_elements[0].text)
        year = int(mobj.group(1)) + CMN.DEF.REPUBLIC_ERA_YEAR_OFFSET
        time_str = "%d-%s-%s" % (year, mobj.group(2), mobj.group(3))
        data_element_list = [time_str,]
        for td_element in td_elements[1:]:
            data_element_list.append(td_element.text)
        data_list.append(data_element_list)
    # import pdb; pdb.set_trace()
# Re-Order the data time: from old to new
    data_list.reverse()
    return (data_list, data_name_list)


class WEarnWebScrapyMeta(type):

    __ATTRS = {
# market start
# market end
# stock start
        "_scrape_tfe_open_interest_": _scrape_tfe_open_interest_,
# stock end
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class WEarnScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = WEarnWebScrapyMeta

    __WEARN_ULR_PREFIX = "https://stock.wearn.com/"

    __MARKET_SCRAPY_CFG = {
        "TFE open interest": { # 台指期未平倉(大額近月、法人所有月)
            "url": __WEARN_ULR_PREFIX + "taifexphoto.asp",
            # "table_time_unit_description_list": [u"日",],
       },
    }

    __STOCK_SCRAPY_CFG = {
    }

    __MARKET_URL = {key: value["url"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_URL_LIST = {key: value["table_time_unit_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_URL_LIST = {key: value["table_time_unit_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    # __TABLE_XPATH = {}
    # __TABLE_XPATH.update(__MARKET_TABLE_XPATH)
    # __TABLE_XPATH.update(__STOCK_TABLE_XPATH)

    # __TIME_UNIT_URL_LIST = {}
    # __TIME_UNIT_URL_LIST.update(__MARKET_TIME_UNIT_URL_LIST)
    # __TIME_UNIT_URL_LIST.update(__STOCK_TIME_UNIT_URL_LIST)

    # __TIME_UNIT_DESCRIPTION_LIST = {}
    # __TIME_UNIT_DESCRIPTION_LIST.update(__MARKET_TIME_UNIT_DESCRIPTION_LIST)
    # __TIME_UNIT_DESCRIPTION_LIST.update(__STOCK_TIME_UNIT_DESCRIPTION_LIST)

    __FUNC_PTR = {
# market start
        "TFE open interest": _scrape_tfe_open_interest_,
# market end
# stock start
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    # @classmethod
    # def get_scrapy_method_list(cls):
    #     return cls.__METHOD_NAME_LIST


    # @classmethod
    # def print_scrapy_method(cls):
    #     print ", ".join(cls.__METHOD_NAME_LIST)


    # @classmethod
    # def print_scrapy_method_time_unit_description(cls, scrapy_method):
    #     print ", ".join(cls.__TIME_UNIT_DESCRIPTION_LIST[scrapy_method])


    def __init__(self, **cfg):
# For the variables which are NOT changed during scraping
        self.xcfg = {
            "dry_run_only": False,
            "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "max_data_count": None,
        }
        self.xcfg.update(cfg)
        # self.url = url
        self.webdriver = None
        # self.company_number = None
        # self.company_group_number = None


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
        if self.__MARKET_URL.get(self.scrapy_method, None) is not None:
            url = self.__MARKET_URL[self.scrapy_method]
#         elif self.__STOCK_URL_FORMAT.get(self.scrapy_method, None) is not None:
#             url_format = self.__STOCK_URL_FORMAT[self.scrapy_method]
# # stock_time_unit
#             stock_time_unit = None
#             if kwargs.get("stock_time_unit", None) is not None:
#                 stock_time_unit = kwargs["stock_time_unit"]
#             if stock_time_unit is None:
#                 stock_time_unit = SC_DEF.GOODINFO_STOCK_TABLE_DEF_TIME_UNIT
#             url_format += self.__STOCK_TIME_UNIT_URL_LIST[self.scrapy_method][stock_time_unit]
#             url = url_format % self.company_number
        else:
            raise ValueError("Unknown scrapy method: %s" % self.scrapy_method)
        self.webdriver.get(url)
        # kwargs['table_xpath'] = self.__STOCK_TABLE_XPATH[self.scrapy_method]
        return (self.__FUNC_PTR[self.scrapy_method])(self.webdriver, *args, **kwargs)


    @property
    def ScrapyMethod(self):
        return self.scrapy_method

    @ScrapyMethod.setter
    def ScrapyMethod(self, value):
        self._set_scrapy_method(self, value)


    @property
    def ScrapyMethodIndex(self):
        return self.scrapy_method_index

    @ScrapyMethodIndex.setter
    def ScrapyMethodIndex(self, value):
        self._set_scrapy_method_index(self, value)


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
    with WEarnScrapy() as goodinfo:
        goodinfo.CompanyNumber = "6274"
        kwargs = {}
        goodinfo.scrape("institutional investor net buy sell", **kwargs)
    	# kwargs["table_data_count"] = 2
		# for scrapy_method in WEarnWebScrapy.get_scrapy_method_list():
		# 	goodinfo.scrape(scrapy_method, **kwargs)
		# 	print "\n"
		# goodinfo.scrape("legal_persion_buy_sell", **kwargs)
		# import pdb; pdb.set_trace()
		# (scrapy_list, scrapy_time_list, scrapy_name_list) = goodinfo.scrape("income statement", 2)
		# import pdb; pdb.set_trace()
	# 	print "Done"
