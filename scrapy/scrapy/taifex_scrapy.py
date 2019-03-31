# -*- coding: utf8 -*-

import re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import scrapy.common as CMN
import scrapy_class_base as ScrapyBase# as ScrapyBase
g_logger = CMN.LOG.get_logger()


def _scrape_taiwan_future_index_and_lot_(scrapy_cfg, *args, **kwargs):
    # import pdb; pdb.set_trace()
    url = scrapy_cfg['url']
    if kwargs.has_key("time"):
        time_cfg = kwargs["time"]
        # time_start = time_range_cfg['start']
        # time_end = time_range_cfg['end']
        # assert time_start == time_end, "The start[%s] and end[%s] days are NOT identical" % (time_start, time_end)
        url_time_range = scrapy_cfg["url_time_format"].format(time_cfg.year, time_cfg.month, time_cfg.day)  
        url += url_time_range

    parse_data_name = kwargs.get("parse_data_name", True)
    parse_data = kwargs.get("parse_data", True)
    def parse_url_data(req):
        # import pdb; pdb.set_trace()
        soup = BeautifulSoup(req.text)
        # print (soup.prettify())
        table_tags = soup.find_all('table', class_='table_f')
        if table_tags is None:
            g_logger.error("Fail to find the table of taiwan future index and lot")
            return None
        tr_tags = table_tags[0].find_all('tr')
# parse data name
        data_name_list = None
        if parse_data_name:
            data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
            th_tags = tr_tags[0].find_all("th")
            for th_tag in th_tags[2:]:
                data_name_list.append(th_tag.text)
# parse data
        data_list = None
        if parse_data:
            # import pdb; pdb.set_trace()
# Find the date of the data
            regex = re.compile("[\d]{4}/[\d]{2}/[\d]{2}")
            date_tag = soup.find('h3', text=regex)
            if date_tag is None:
                raise RuntimeError("Fail to find the date of taiwan future index and lot")
            mobj = re.search(r"[\d]{4}/[\d]{2}/[\d]{2}", date_tag.string, re.U)
            date_str = mobj.group(0)
            date_element_list = date_str.split("/")
            assert len(date_element_list) == 3, "The length[%d] of date_element_list should be 3" % len(date_element_list)
            data_element_list = [CMN.DEF.DATE_STRING_FORMAT % (int(date_element_list[0]), int(date_element_list[1]), int(date_element_list[2])),]
# Parse the table of the data
            td_tags = tr_tags[1].find_all("td")
            for td_tag in td_tags[2:]:
                # print (td_tag.text)
                if td_tag.find("font"):
                    data_element_list.append(td_tag.font.string[1:].strip("%"))
                else:
                    data_element_list.append(td_tag.string.strip("\r\n\t"))
            data_list = [data_element_list,]
        # import pdb; pdb.set_trace()
        return (data_list, data_name_list)
    # import pdb; pdb.set_trace()
    data_list, data_name_list = ScrapyBase.ScrapyBase.try_request_web_data(url, parse_url_data)
    return (data_list, data_name_list)


def _scrape_option_put_call_ratio_(scrapy_cfg, *args, **kwargs):
    # import pdb; pdb.set_trace()
    url = scrapy_cfg['url']
    if kwargs.has_key("time"):
        # finance_month = kwargs["month"]
        # year = finance_month.year
        # month = finance_month.month
        # start_date = 1
        # end_date = finance_month.get_last_date_of_month()
        time_range_cfg = finance_month = kwargs["time"]
        time_start = time_range_cfg['start']
        time_end = time_range_cfg['end']
        url_time_range = scrapy_cfg["url_time_format"].format(time_start.year, time_start.month, time_start.day, time_end.year, time_end.month, time_end.day)  
        url += url_time_range

    parse_data_name = kwargs.get("parse_data_name", True)
    parse_data = kwargs.get("parse_data", True)
    def parse_url_data(req):
        # import pdb; pdb.set_trace()
        soup = BeautifulSoup(req.text)
        # print (soup.prettify())
        table_tag = soup.find('table', class_='table_a')
        if table_tag is None:
            # g_logger.error("Fail to find the table of option put call ratio")
            # return None
            raise RuntimeError("Fail to find the table of option put call ratio")
        tr_tags = table_tag.find_all('tr')
# parse data name
        data_name_list = None
        if parse_data_name:
            data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
            th_tags = tr_tags[0].find_all("th")
            for th_tag in th_tags[1:]:
                data_name_list.append(th_tag.text)
# parse data
        data_list = None
        if parse_data:
            data_list = []
            for tr_tag in tr_tags[1:]:
                # print (tr_tag.text)
                td_tags = tr_tag.find_all("td")
                date_element_list = td_tags[0].string.split("/")
                assert len(date_element_list) == 3, "The length[%d] of date_element_list should be 3" % len(date_element_list)
                data_element_list = [CMN.DEF.DATE_STRING_FORMAT % (int(date_element_list[0]), int(date_element_list[1]), int(date_element_list[2])),]
                for td_tag in td_tags[1:]:
                    # print (td_tag.text)
                    data_element_list.append(td_tag.string.strip("\r\n\t").replace(',', ''))
                data_list.append(data_element_list)
        # import pdb; pdb.set_trace()
        return (data_list, data_name_list)
    # import pdb; pdb.set_trace()
    data_list, data_name_list = ScrapyBase.ScrapyBase.try_request_web_data(url, parse_url_data)
    if data_list is not None:
        data_list.reverse()
    return (data_list, data_name_list)


class TaifexScrapyMeta(type):

    __ATTRS = {
        "_scrape_taiwan_future_index_and_lot_": _scrape_taiwan_future_index_and_lot_,
        "_scrape_option_put_call_ratio_": _scrape_option_put_call_ratio_,
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class TaifexScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = TaifexScrapyMeta

    _CAN_SET_TIME_RANGE = True

    __TAIFEX_ULR_PREFIX = "http://www.taifex.com.tw/"

    __MARKET_SCRAPY_CFG = {
        "taiwan future index and lot": {  # 臺股期貨指數(近月)及成交口數
            "url": __TAIFEX_ULR_PREFIX + "cht/3/futDailyMarketReport?queryType=2&marketCode=0&commodity_id=TX",
            "url_time_format": "&queryDate={0}%2F{1:02d}%2F{2:02d}",
            "url_encoding": CMN.DEF.URL_ENCODING_UTF8,
        },
        "option put call ratio": { # 臺指選擇權賣權買權比
            "url": __TAIFEX_ULR_PREFIX + "cht/3/pcRatio",
            "url_time_format": "?queryStartDate={0}%2F{1:02d}%2F{2:02d}&queryEndDate={3}%2F{4:02d}%2F{5:02d}",
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
        "taiwan future index and lot": _scrape_taiwan_future_index_and_lot_,
        "option put call ratio": _scrape_option_put_call_ratio_,
# market end
# stock start
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    def __init__(self, **cfg):
# For the variables which are NOT changed during scraping
        self.xcfg = {
            "dry_run_only": False,
            # "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
            "max_data_count": None,
        }
        self.xcfg.update(cfg)


    def scrape_web(self, *args, **kwargs):
        return (self.__FUNC_PTR[self.scrapy_method])(self.__SCRAPY_CFG[self.scrapy_method], *args, **kwargs)


if __name__ == '__main__':
    with TaifexScrapy() as taifex:
        kwargs = {}
        # import pdb; pdb.set_trace()
        taifex.scrape("option put call ratio", **kwargs)
