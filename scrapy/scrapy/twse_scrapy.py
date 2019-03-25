# -*- coding: utf8 -*-

import re
# from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import scrapy.common as CMN
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()


def _scrape_taiwan_weighted_index_and_volume_(scrapy_cfg, *args, **kwargs):
    # import pdb; pdb.set_trace()
    url = scrapy_cfg['url']
    if kwargs.has_key("time"):
        time_cfg = kwargs["time"]
        # time_start = time_cfg['start']
        # time_end = time_cfg['end']
        # assert time_start == time_end, "The start[%s] and end[%s] months are NOT identical" % (time_start, time_end)
        url_time = scrapy_cfg["url_time_format"].format(time_cfg.year, time_cfg.month)  
        url += url_time

    parse_data_name = kwargs.get("parse_data_name", True)
    parse_data = kwargs.get("parse_data", True)
    def parse_url_data(req):
        # import pdb; pdb.set_trace()
        scrapy_res = json.loads(req.text)
        data_name_list = None
        if parse_data_name:
            data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
            scrapy_res_field = scrapy_res['fields']
            data_name_list.extend(scrapy_res_field[1:])
        data_list = None
        if parse_data:
            data_list = []
            scrapy_res_data = scrapy_res['data']
            for entry in scrapy_res_data:
                date_list = str(entry[0]).split('/')
                if len(date_list) != 3:
                    raise RuntimeError("The date format is NOT as expected: %s", date_list)
                date_str = CMN.FUNC.transform_date_str((int(date_list[0]) + CMN.DEF.REPUBLIC_ERA_YEAR_OFFSET), int(date_list[1]), int(date_list[2]))
                data_element_list = [date_str,]
                data_element_list.extend([str(data_element).replace(',', '') for data_element in entry[1:]])
                data_list.append(data_element_list)
        # import pdb; pdb.set_trace()
        return (data_list, data_name_list)
    data_list, data_name_list = ScrapyBase.ScrapyBase.try_request_web_data(url, parse_url_data)
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


class TwseScrapyMeta(type):

    __ATTRS = {
        "_scrape_taiwan_weighted_index_and_volume_": _scrape_taiwan_weighted_index_and_volume_,
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class TwseScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = TwseScrapyMeta

    _CAN_SET_TIME_RANGE = True

    __TWSE_ULR_PREFIX = "http://www.twse.com.tw/"

    __MARKET_SCRAPY_CFG = {
        "taiwan weighted index and volume": { # 臺股指數及成交量
            "url": __TWSE_ULR_PREFIX + "exchangeReport/FMTQIK?response=json",
            "url_time_format": "&date={0}{1:02d}01",
            "url_encoding": CMN.DEF.URL_ENCODING_UTF8,
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
        # url = None
        # import pdb; pdb.set_trace()
        # scrapy_method_name = None
        # try:
        #     scrapy_method_name = self.__METHOD_NAME_LIST[self.scrapy_method]
        # except:
        #     raise ValueError("Unknown scrapy method: %s" % self.scrapy_method)
        # scrapy_cfg = self.__SCRAPY_CFG[scrapy_method_name]
        # return (self.__FUNC_PTR[self.scrapy_method])(scrapy_cfg, *args, **kwargs)
        return (self.__FUNC_PTR[self.scrapy_method])(self.__SCRAPY_CFG[self.scrapy_method], *args, **kwargs)


    # def update_csv_field(self):
    #     _, csv_data_field_list = self.scrape_web()
    #     self._write_scrapy_field_data_to_config(csv_data_field_list, self.scrapy_method_index, self.xcfg['finance_root_folderpath'])


    # @property
    # def CSVTimeDuration(self):
    #     return self.csv_time_duration

    # @CSVTimeDuration.setter
    # def CSVTimeDurationCSVTimeDuration(self, csv_time_duration):
    # 	self.csv_time_duration = csv_time_duration


    # @property
    # def ScrapyMethod(self):
    #     return self.scrapy_method

    # @ScrapyMethod.setter
    # def ScrapyMethod(self, value):
    #     # try:
    #     #     self.method_list.index(value)
    #     # except ValueError:
    #     #     errmsg = "The method[%s] is NOT support in %s" % (value, CMN.FUNC.get_instance_class_name(self))
    #     #     g_logger.error(errmsg)
    #     #     raise ValueError(errmsg)
    #     # self.scrapy_method = value
    #     # if self.scrapy_method_index is not None:
    #     #     g_logger.warn("The {0}::scrapy_method_index is reset since the {0}::scrapy_method is set ONLY".format(CMN.FUNC.get_instance_class_name(self)))
    #     #     self.scrapy_method_index = None
    #     # raise NotImplementedError
    #     self._set_scrapy_method(self, value)


    # @property
    # def ScrapyMethodIndex(self):
    #     return self.scrapy_method_index

    # @ScrapyMethodIndex.setter
    # def ScrapyMethodIndex(self, value):
    #     # if CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[value]['class_name'] != CMN.FUNC.get_instance_class_name(self):
    #     #     raise ValueError("The scrapy index[%d] is NOT supported by the Scrapy class: %s" % (value, CMN.FUNC.get_instance_class_name(self)))
    #     # self.scrapy_method_index = value
    #     # self.scrapy_method = CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[self.scrapy_method_index]['scrapy_class_method']
    #     self._set_scrapy_method_index(self, value)


if __name__ == '__main__':
    with TwseScrapy() as taifex:
        kwargs = {}
        import pdb; pdb.set_trace()
        taifex.scrape("option put call ratio", **kwargs)
