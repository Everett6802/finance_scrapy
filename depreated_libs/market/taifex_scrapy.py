# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import market_scrapy_base as MarketScrapyBase
g_logger = CMN.LOG.get_logger()


def _scrape_option_put_call_ratio_(web_data, *args, **kwargs):
    # import pdb; pdb.set_trace()
    data_name_list = []
    data_list = []
    web_data_len = len(web_data)
    # print "len: %d" % data_len
    for tr in web_data[web_data_len - 1 : 0 : -1]:
        td = tr.select('td')
        entry = [str(td[0].text.replace("/", "-")),]
        for index in range(1, 7):
            entry.append(str(td[index].text).replace(',', ''))
        data_list.append(entry)
    return (data_list, data_name_list)


class TaifexScrapyMeta(type):

    __ATTRS = {
        "_scrape_option_put_call_ratio_": _scrape_option_put_call_ratio_,
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class TaifexScrapy(MarketScrapyBase.MarketScrapyBase):

	__metaclass__ = TaifexScrapyMeta
    __GOODINFO_ULR_PREFIX = "https://goodinfo.tw/StockInfo/"

    __MARKET_SCRAPY_CFG = {
        "scrape option put call ratio": { # 臺指選擇權賣權買權比
            "url_format": __GOODINFO_ULR_PREFIX + "cht/3/pcRatio?down_type=&queryStartDate=%d%2F%02d%2F%02d&queryEndDate=%d%2F%02d%2F%02d",
            # "table_time_unit_url_list": ["&CHT_CAT=DATE", "&CHT_CAT=WEEK", "&CHT_CAT=MONTH",],
            # "table_time_unit_description_list": [u"日", u"週", u"月",],
        },
    }

    __STOCK_SCRAPY_CFG = {
    }

    __MARKET_URL = {key: value["url_format"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    # __TABLE_XPATH = {}
    # __TABLE_XPATH.update(__MARKET_TABLE_XPATH)
    # __TABLE_XPATH.update(__STOCK_TABLE_XPATH)

    __TIME_UNIT_URL_LIST = {}
    __TIME_UNIT_URL_LIST.update(__MARKET_TIME_UNIT_URL_LIST)
    __TIME_UNIT_URL_LIST.update(__STOCK_TIME_UNIT_URL_LIST)

    # __TIME_UNIT_DESCRIPTION_LIST = {}
    # __TIME_UNIT_DESCRIPTION_LIST.update(__MARKET_TIME_UNIT_DESCRIPTION_LIST)
    # __TIME_UNIT_DESCRIPTION_LIST.update(__STOCK_TIME_UNIT_DESCRIPTION_LIST)

    __FUNC_PTR = {
# market start
        "institutional investor net buy sell": _scrape_institutional_investor_net_buy_sell_,
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
        # self.csv_time_duration = None
        self.company_number = None
        self.company_group_number = None
        # self.is_annual = True


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
        elif self.__STOCK_URL_FORMAT.get(self.scrapy_method, None) is not None:
            url_format = self.__STOCK_URL_FORMAT[self.scrapy_method]
# stock_time_unit
            stock_time_unit = None
            if kwargs.get("stock_time_unit", None) is not None:
                stock_time_unit = kwargs["stock_time_unit"]
            if stock_time_unit is None:
                stock_time_unit = CMN_DEF.GOODINFO_STOCK_TABLE_DEF_TIME_UNIT
            url_format += self.__STOCK_TIME_UNIT_URL_LIST[self.scrapy_method][stock_time_unit]
            url = url_format % self.company_number
        else:
            raise ValueError("Unknown scrapy method: %s" % self.scrapy_method)
        self.webdriver.get(url)
        # kwargs['table_xpath'] = self.__STOCK_TABLE_XPATH[self.scrapy_method]
        return (self.__FUNC_PTR[self.scrapy_method])(self.webdriver, *args, **kwargs)


    # def update_csv_field(self):
    #     _, csv_data_field_list = self.scrape_web()
    #     self._write_scrapy_field_data_to_config(csv_data_field_list, self.scrapy_method_index, self.xcfg['finance_root_folderpath'])


    # @property
    # def CSVTimeDuration(self):
    #     return self.csv_time_duration

    # @CSVTimeDuration.setter
    # def CSVTimeDurationCSVTimeDuration(self, csv_time_duration):
    # 	self.csv_time_duration = csv_time_duration


    @property
    def ScrapyMethod(self):
        return self.scrapy_method

    @ScrapyMethod.setter
    def ScrapyMethod(self, value):
        # try:
        #     self.method_list.index(value)
        # except ValueError:
        #     errmsg = "The method[%s] is NOT support in %s" % (value, CMN.FUNC.get_instance_class_name(self))
        #     g_logger.error(errmsg)
        #     raise ValueError(errmsg)
        # self.scrapy_method = value
        # if self.scrapy_method_index is not None:
        #     g_logger.warn("The {0}::scrapy_method_index is reset since the {0}::scrapy_method is set ONLY".format(CMN.FUNC.get_instance_class_name(self)))
        #     self.scrapy_method_index = None
        # raise NotImplementedError
        self._set_scrapy_method(self, value)


    @property
    def ScrapyMethodIndex(self):
        return self.scrapy_method_index

    @ScrapyMethodIndex.setter
    def ScrapyMethodIndex(self, value):
        # if CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[value]['class_name'] != CMN.FUNC.get_instance_class_name(self):
        #     raise ValueError("The scrapy index[%d] is NOT supported by the Scrapy class: %s" % (value, CMN.FUNC.get_instance_class_name(self)))
        # self.scrapy_method_index = value
        # self.scrapy_method = CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[self.scrapy_method_index]['scrapy_class_method']
        self._set_scrapy_method_index(self, value)



if __name__ == '__main__':
