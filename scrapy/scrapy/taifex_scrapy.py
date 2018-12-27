# -*- coding: utf8 -*-

import re
# import requests
# import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
# import scrapy.common as CMN
# import scrapy_function as SC_FUNC
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()


def _scrape_taiwan_weighted_index_and_volume_(scrapy_cfg, *args, **kwargs):
    # import pdb; pdb.set_trace()
    url = scrapy_cfg['url']
    if kwargs.has_key("month") is not None:
        finance_month = kwargs["month"]
        year = finance_month.year
        month = finance_month.month
        start_date = 1
        end_date = finance_month.get_last_date_of_month()
        url_time_range = scrapy_cfg["url_time_range_format"].format(year, month, start_date, end_date)  
        url += url_time_range

    def parse_url(url_data):
        pass

    web_data = ScrapyBase.try_get_web_data(url, parse_url)

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
        "_scrape_taiwan_weighted_index_and_volume_": _scrape_taiwan_weighted_index_and_volume_,
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class TaifexScrapy(ScrapyBase.ScrapyBase):

	__metaclass__ = TaifexScrapyMeta
    __TAIFEX_ULR_PREFIX = "http://www.taifex.com.tw/"

    __MARKET_SCRAPY_CFG = {
        "scrape taiwan weighted index and volume": { # 臺指選擇權賣權買權比
            "url": __TAIFEX_ULR_PREFIX + "cht/3/pcRatio"
            "url_time_range_format" = "?queryStartDate={0}%2F{1:02d}}%2F{2:02d}&queryEndDate={0}%2F{1:02d}%2F{3:02d}",
            "url_encoding": URL_ENCODING_UTF8,
        },
    }

    __STOCK_SCRAPY_CFG = {
    }

    # __MARKET_URL = {key: value["url_format"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    # __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    # __TIME_UNIT_URL_LIST = {}
    # __TIME_UNIT_URL_LIST.update(__MARKET_TIME_UNIT_URL_LIST)
    # __TIME_UNIT_URL_LIST.update(__STOCK_TIME_UNIT_URL_LIST)

    # __TIME_UNIT_DESCRIPTION_LIST = {}
    # __TIME_UNIT_DESCRIPTION_LIST.update(__MARKET_TIME_UNIT_DESCRIPTION_LIST)
    # __TIME_UNIT_DESCRIPTION_LIST.update(__STOCK_TIME_UNIT_DESCRIPTION_LIST)

    __SCRAPY_CFG = {}
    __SCRAPY_CFG.update(__MARKET_SCRAPY_CFG)
    __SCRAPY_CFG.update(__STOCK_SCRAPY_CFG)

    __FUNC_PTR = {
# market start
        "taiwan weighted index and volume": _scrape_taiwan_weighted_index_and_volume_,
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
            # "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "max_data_count": None,
        }
        self.xcfg.update(cfg)
        # self.url = url
        # self.csv_time_duration = None


    def scrape_web(self, *args, **kwargs):
        url = None
        # import pdb; pdb.set_trace()
        scrapy_method_name = None
        try:
            scrapy_method_name = self.__METHOD_NAME_LIST[self.scrapy_method]
        except:
            raise ValueError("Unknown scrapy method: %d" % self.scrapy_method)
        scrapy_cfg = self.__SCRAPY_CFG[scrapy_method_name]
        return (self.__FUNC_PTR[self.scrapy_method])(scrapy_cfg, *args, **kwargs)


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
    with TaifexScrapy() as taifex:
        kwargs = {}
        import pdb; pdb.set_trace()
        taifex.scrape("option put call ratio", **kwargs)
