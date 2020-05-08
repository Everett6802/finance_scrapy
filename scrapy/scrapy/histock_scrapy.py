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
from selenium.webdriver.support.ui import Select

import scrapy.common as CMN
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()

PRINT_SCRAPY = True
WEEKLY_STRIKE_PRICE_RANGE = 1000
STRIKE_PRICE_RANGE = 1500
MAX_DATA_DAY_NUMBER = 6
DEF_SCRAPY_DAY_COUNT = 1


def __parse_table(driver, date_index, date_string, tr_elements, *args, **kwargs):
    strike_price_range = kwargs.get("strike_price_range", 1000)

    OPTION_DATA_COLUMN_NUM = 7
# data
    # date_label = driver.find_element_by_id("CPHB1_Options1_rdoDates_5")
    data_list = []
    data_list.append(date_string)
# Find the strike price
    at_the_money_strike_price = None
    at_the_money_strike_price_labels = driver.find_elements_by_class_name("tg")
    for at_the_money_strike_price_label in at_the_money_strike_price_labels:
        try:
            at_the_money_strike_price = int(at_the_money_strike_price_label.text)
        except ValueError:
            pass
    assert at_the_money_strike_price is not None, "Fails to find the strike price"

    PREFIX_CHECK_COLUMN_NUM_LIST = [3, 10,]
    STRICE_PRICE_COLUMN_NUM = 7
    row_list = []
    # import pdb; pdb.set_trace()
    for tr_element in tr_elements[2:]:
        row_element_list = []
# Add the strike price in the first column
        th_element = tr_element.find_element_by_tag_name("th")
        row_element_list.append(th_element.text)
# Parse the data in each row
        td_elements = tr_element.find_elements_by_tag_name("td")
        for index, td_element in enumerate(td_elements):
            element_text = td_element.text.replace(",", "")
            if index in PREFIX_CHECK_COLUMN_NUM_LIST:
                # data_element = element_text
                if element_text != u'-':
                    if element_text[0] == u'\u25b2':
                        element_text = "+%s" % element_text[1:]
                    elif element_text[0] == u'\u25bc':
                        element_text = "-%s" % element_text[1:]
                    else:
                        element_text = "%s" % element_text[1:]
                row_element_list.append(element_text)
            else:
                row_element_list.append(element_text)
        row_list.append(row_element_list)
    # import pdb; pdb.set_trace()
# Filter the rows which are NOT in the range of strike price
    strike_price_range_upper = at_the_money_strike_price + strike_price_range
    strike_price_range_lower = at_the_money_strike_price - strike_price_range
    row_list = filter(lambda x: strike_price_range_upper >= int(x[0]) >= strike_price_range_lower, row_list)
    row_num = len(row_list)
# Convert multi-dimensional list to a 1D list
# It's just two nested loops: 
# exactly as if within an outer statement for row_element_list in row_list: 
# you indented a further statement for x in row_element_list: 
# and then inside it just took the leading expression x of the list comprehension 
# to be appended as the next item of the new list you're building
    data_list.extend([x for row_element_list in row_list for x in row_element_list])

    data_name_list = None
# data name
    if date_index == 0:
        row_name_list = []
        th_elements0 = tr_elements[0].find_elements_by_tag_name("th")
        buy_put_option_name_list = []
        buy_put_option_name_list.extend(([th_elements0[0].text,] * OPTION_DATA_COLUMN_NUM))
        buy_put_option_name_list.extend(([th_elements0[2].text,] * OPTION_DATA_COLUMN_NUM))
        # import pdb; pdb.set_trace()
# Insert strike price
        row_name_list.append(th_elements0[1].text)
        th_elements1 = tr_elements[1].find_elements_by_tag_name("th")
        for index, th_element in enumerate(th_elements1):
            row_name_list.append("%s%s" % (buy_put_option_name_list[index], th_element.text))
        # import pdb; pdb.set_trace()

        data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
        for i in range(row_num):
            data_name_list.extend(row_name_list)
        assert len(data_list) == len(data_name_list), "The length of data_list[%d] and data_name_list[%d] is NOT identical" % (len(data_list), len(data_name_list))
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


def __parse_option_open_interest_table(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
    scrapy_day_count = kwargs.get("scrapy_day_count", DEF_SCRAPY_DAY_COUNT)
    assert 1 <= scrapy_day_count <= MAX_DATA_DAY_NUMBER, "scrapy_day_count[%d] should be in the range: [1, %d]" % (scrapy_day_count, MAX_DATA_DAY_NUMBER)
    wait = WebDriverWait(driver, 15)

    data_list = [] 
    data_name_list = None
    for count in range(scrapy_day_count):
        index = MAX_DATA_DAY_NUMBER - 1 - count
# Select the date
        if count != 1:
            xpath = "//*[@id='CPHB1_Options1_rdoDates_%d']" % index
            date_input = driver.find_element_by_xpath(xpath)
            # print date_input.get_attribute("value") // Can get the date string
            date_input.click()
# Wait for the select 
        option_select = wait.until(
            EC.presence_of_element_located((By.ID, "CPHB1_Options1_ddlEndDates"))
        )
        # option_select = driver.find_element_by_id("CPHB1_Options1_ddlEndDates")
        option_item_list = [x.get_attribute("value") for x in option_select.find_elements_by_tag_name("option")]
        # print option_item_list
# Count weekly option
        weekly_option_count = len(filter(lambda x : re.match("[\d]+W[\d]", x), option_item_list))
        assert (weekly_option_count >= 0 and weekly_option_count <= 2), "Incorrect weekly option count, option_item_list: %s" % option_item_list
        # import pdb; pdb.set_trace()
        is_weekly = kwargs.get("is_weekly", False)
        option_select_index = 0
        if is_weekly:
            option_select_index = weekly_option_count - 1
        else:
            option_select_index = weekly_option_count
# Select the expiry date
        table_element = None
        if option_select_index != 0:
            option_select = Select(option_select)
            option_select.select_by_index(option_select_index)
# Wait for the table
            wait = WebDriverWait(driver, 10)
            table_element = wait.until(
                EC.presence_of_element_located((By.XPATH, "/html/body/form/div[4]/div[5]/div/div/div[2]/div[2]/div/table"))
            )
        else:
            table_element = driver.find_element_by_xpath("/html/body/form/div[4]/div[5]/div/div/div[2]/div[2]/div/table")
        tr_elements = table_element.find_elements_by_tag_name("tr")
# Parse the date string
        xpath = "//*[@id='CPHB1_Options1_rdoDates']/tbody/tr/td[%d]/label" % (index + 1)
        date_label = driver.find_element_by_xpath(xpath)
        date_element_list = map(int, date_label.text.split("/"))
        assert len(date_element_list) == 3, "date_element_list[%s] length should be 3" % date_element_list
        date_string = CMN.DEF.DATE_STRING_FORMAT % (date_element_list[0], date_element_list[1], date_element_list[2])
        single_date_data_list, data_name_list_tmp = __parse_table(driver, count, date_string, tr_elements, **kwargs)
        data_list.append(single_date_data_list)
        if data_name_list is None:
            data_name_list = data_name_list_tmp
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


def _scrape_option_open_interest_(driver, *args, **kwargs):
    kwargs["strike_price_range"] = STRIKE_PRICE_RANGE
    return __parse_option_open_interest_table(driver, *args, **kwargs)


def _scrape_weekly_option_open_interest_(driver, *args, **kwargs):
    kwargs["is_weekly"] = True
    kwargs["strike_price_range"] = WEEKLY_STRIKE_PRICE_RANGE
    return __parse_option_open_interest_table(driver, *args, **kwargs)


class HiStockScrapyMeta(type):

    __ATTRS = {
# market start
        "_scrape_option_open_interest_": _scrape_option_open_interest_,
        "_scrape_weekly_option_open_interest_": _scrape_weekly_option_open_interest_,
# market end
# stock start
# stock end
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class HiStockScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = HiStockScrapyMeta

    __HISTOCK_ULR_PREFIX = "https://histock.tw/"

    __MARKET_SCRAPY_CFG = {
        "option open interest": { # 台指選擇權未平倉
            "url": __HISTOCK_ULR_PREFIX + "stock/option.aspx?m=week",
        },
        "weekly option open interest": { # 台指周選擇權未平倉
            "url": __HISTOCK_ULR_PREFIX + "stock/option.aspx?m=week",
        },
    }

    __STOCK_SCRAPY_CFG = {
    }

    __MARKET_URL = {key: value["url"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
 
    __FUNC_PTR = {
# market start
        "option open interest": _scrape_option_open_interest_,
        "weekly option open interest": _scrape_weekly_option_open_interest_,
# market end
# stock start
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    def __init__(self, **cfg):
        super(HiStockScrapy, self).__init__()
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
        url = None
        # import pdb; pdb.set_trace()
        if self.__MARKET_URL.get(self.scrapy_method, None) is not None:
            url = self.__MARKET_URL[self.scrapy_method]
        else:
            raise ValueError("Unknown scrapy method: %s" % self.scrapy_method)
        self.webdriver.get(url)
        # kwargs['table_xpath'] = self.__STOCK_TABLE_XPATH[self.scrapy_method]
        return (self.__FUNC_PTR[self.scrapy_method])(self.webdriver, *args, **kwargs)


if __name__ == '__main__':
    with HiStockScrapy() as histock:
        # histock.CompanyNumber = "6274"
        kwargs = {
            "scrapy_day_count": 2
        }
        data_list, data_name_list = histock.scrape("option open interest", **kwargs)
        # kwargs["table_data_count"] = 2
        # for scrapy_method in HiStockScrapy.get_scrapy_method_list():
        #   histock.scrape(scrapy_method, **kwargs)
        #   print "\n"
        # histock.scrape("legal_persion_buy_sell", **kwargs)
        # import pdb; pdb.set_trace()
        # (scrapy_list, scrapy_time_list, scrapy_name_list) = histock.scrape("income statement", 2)
        # import pdb; pdb.set_trace()
    #   print "Done"
