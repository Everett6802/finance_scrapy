# -*- coding: utf8 -*-

import re
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
# data field
        data_name_list = None
        if parse_data_name:
            data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
            scrapy_res_field = scrapy_res['fields']
            data_name_list.extend(scrapy_res_field[1:])
# data
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


def _scrape_stock_price_and_volume_(scrapy_cfg, *args, **kwargs):
    # import pdb; pdb.set_trace()
    company_number = kwargs.get("company_number", None)
    if company_number is None:
        raise ValueError("The company number should NOT be NONE")
    url = scrapy_cfg['url'] % company_number
    if kwargs.has_key("time"):
        time_cfg = kwargs["time"]
        url_time = scrapy_cfg["url_time_format"].format(time_cfg.year, time_cfg.month)  
        url += url_time

    parse_data_name = kwargs.get("parse_data_name", True)
    parse_data = kwargs.get("parse_data", True)
    def parse_url_data(req):
        # import pdb; pdb.set_trace()
        scrapy_res = json.loads(req.text)
# data field
        data_name_list = None
        if parse_data_name:
            data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
            scrapy_res_field = scrapy_res['fields']
            data_name_list.extend(scrapy_res_field[1:])
# data
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
                data_element_list.extend([str(data_element).replace(",", "") for data_element in entry[1:]])
                # data_element_list[1] = str(int(data_element_list[1]) * 1000)
                # data_element_list[2] = str(int(data_element_list[2]) * 1000)
                data_element_list[7] = data_element_list[7].lstrip("X").lstrip(" ") # The 'change' column
                data_list.append(data_element_list)
        # import pdb; pdb.set_trace()
        return (data_list, data_name_list)
    data_list, data_name_list = ScrapyBase.ScrapyBase.try_request_web_data(url, parse_url_data)
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


class TwseScrapyMeta(type):

    __ATTRS = {
        "_scrape_taiwan_weighted_index_and_volume_": _scrape_taiwan_weighted_index_and_volume_,
        "_scrape_stock_price_and_volume_": _scrape_stock_price_and_volume_,
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
        "stock price and volume": { # 個股股價及成交量
            "url": __TWSE_ULR_PREFIX + "exchangeReport/STOCK_DAY?response=json&stockNo=%s",
            "url_time_format": "&date={0}{1:02d}01",
            "url_encoding": CMN.DEF.URL_ENCODING_UTF8,
        },
    }

    __SCRAPY_CFG = {}
    __SCRAPY_CFG.update(__MARKET_SCRAPY_CFG)
    __SCRAPY_CFG.update(__STOCK_SCRAPY_CFG)

    __FUNC_PTR = {
# market start
        "taiwan weighted index and volume": _scrape_taiwan_weighted_index_and_volume_,
# market end
# stock start
        "stock price and volume": _scrape_stock_price_and_volume_,
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    def __init__(self, **cfg):
        super(TwseScrapy, self).__init__()
# # For the variables which are NOT changed during scraping
#         self.xcfg = {
#             "dry_run_only": False,
#             # "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
#             "max_data_count": None,
#         }
#         self.xcfg.update(cfg)
        self.xcfg = self._update_cfg_dict(cfg)


    def scrape_web(self, *args, **kwargs):
        if self.company_number is not None:
            kwargs["company_number"] = self.company_number
        return (self.__FUNC_PTR[self.scrapy_method])(self.__SCRAPY_CFG[self.scrapy_method], *args, **kwargs)


if __name__ == '__main__':
    with TwseScrapy() as taifex:
        kwargs = {}
        # import pdb; pdb.set_trace()
        taifex.scrape("taiwan weighted index and volume", **kwargs)
