#! /usr/bin/python
# -*- coding: utf8 -*-

import re
import time
import copy
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
import scrapy_class_base as ScrapyBase
g_logger = CMN.LOG.get_logger()

GOODINFO_STOCK_TABLE_DEF_TIME_UNIT = 0
PRINT_SCRAPY = True


def _scrape_financial_ratio_growth_rate_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
    table_element = driver.find_element_by_class_name("solid_1_padding_4_4_tbl")
    # thead_element = table_elements[3].find_element_by_tag_name("thead")
    tbody_element = table_element.find_element_by_tag_name("tbody")
    tr_elements = tbody_element.find_elements_by_tag_name("tr")

# data time
    data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
    td_elements0 = tr_elements[0].find_elements_by_tag_name("td")
    data_time_len = len(td_elements0) - 1
    data_list = []
    for index in range(data_time_len):
        data_list.append([])
        data_list[index].append(td_elements0[index + 1].text)
# data name
    # import pdb; pdb.set_trace()
# search for the ranges which are interested in
    search_range_title_name_list = [u"獲利季成長率", u"獲利年成長率", u"各項資產佔總資產比重",]
    search_range_title_name_list_len = len(search_range_title_name_list)
    search_range_title_index_list = []
    for index, tr_element in enumerate(tr_elements):
        td_elements = tr_element.find_elements_by_tag_name("td")
        if td_elements[0].text in search_range_title_name_list:
            search_range_title_index_list.append(index)
            if len(search_range_title_index_list) >= search_range_title_name_list_len:
                break
    title_index_list = []
    search_range_title_index_list_len = len(search_range_title_index_list)
    for times in range(search_range_title_index_list_len - 1):
        title_index_list.extend(range(search_range_title_index_list[times] + 1, search_range_title_index_list[times + 1]))
    # for tr_element in tr_elements[1:]:
    for title_index in title_index_list:
        tr_element = tr_elements[title_index]
        td_elements = tr_element.find_elements_by_tag_name("td")
        data_name_list.append(td_elements[0].text)
        for index, td_element in enumerate(td_elements[1:]):
            data_list[index].append(td_element.text)
        # data_element_list = []
        # for td_element in td_elements[1:]:
    data_list.reverse()
    return (data_list, data_name_list)


def _scrape_institutional_investor_net_buy_sell_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
    data_list = []
    # table_elements = driver.find_elements_by_class_name("solid_1_padding_4_0_tbl")
    # thead_element = table_elements[3].find_element_by_tag_name("thead")
    # tbody_elements = table_elements[3].find_elements_by_tag_name("tbody")

    table_element = driver.find_element_by_class_name("solid_1_padding_4_1_tbl")
    thead_element = table_element.find_element_by_tag_name("thead")
    tbody_elements = table_element.find_elements_by_tag_name("tbody")
    # th_elements = tr_elements[0].find_elements_by_tag_name("th")
    # data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
    # for th_element in th_elements[1:]:
    #     data_name_list.append(th_element.text)
# data name
    tr_elements = thead_element.find_elements_by_tag_name("tr")
    data_name_list = [CMN.DEF.DATE_IN_CHINESE,]
    td_elements0 = tr_elements[0].find_elements_by_tag_name("td")
    for td_element in td_elements0[1:5]:
    	td_element_text = re.sub('\n', '', td_element.text)
        data_name_list.append(td_element_text)
    DATA_NAME_TMP_COLSPAN_COUNT_LIST = [5, 3, 3, 3]
    data_name_tmp_list = []
    for index, td_element in enumerate(td_elements0[5:]):
    	td_element_text = re.sub('\n', '', td_element.text)
        data_name_tmp_list.extend([td_element_text,] * DATA_NAME_TMP_COLSPAN_COUNT_LIST[index])
    td_elements1 = tr_elements[1].find_elements_by_tag_name("td")
    assert len(data_name_tmp_list) == len(td_elements1), "The length[%d, %d] is NOT identical" % (len(data_name_tmp_list), len(td_elements1))
    for index, td_element in enumerate(td_elements1):
        td_element_text = re.sub('\n', '', td_element.text)
        data_name_list.append(u"%s:%s" % (data_name_tmp_list[index], td_element_text))
    # import pdb; pdb.set_trace()
# data
    data_time_unit = None
    for tbody_element in tbody_elements:
        tr_elements = tbody_element.find_elements_by_tag_name("tr")
        for tr_element in tr_elements:
            td_elements = tr_element.find_elements_by_tag_name("td")
            if data_time_unit is None:
                if re.match("[\d]{4}M[\d]{2}", td_elements[0].text) is not None:
                    data_time_unit = CMN.DEF.DATA_TIME_UNIT_MONTH
                elif re.match("W[\d]{4}", td_elements[0].text) is not None:
                    data_time_unit = CMN.DEF.DATA_TIME_UNIT_WEEK
                elif re.match("[\d]{2}'[\d]{2}/[\d]{1,2}", td_elements[0].text) is not None:
                    data_time_unit = CMN.DEF.DATA_TIME_UNIT_DAY
            	else:
            		raise ValueError("Unknown time unit string: %s" % td_elements[0].text)
            if data_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
                mobj = re.match("([\d]{4})M([\d]{2})", td_elements[0].text)
                time_str = "%s-%s-01" % (mobj.group(1), mobj.group(2))
            elif data_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
                mobj = re.match("W([\d]{2})([\d]{1,2})", td_elements[0].text)
                year = int(mobj.group(1))
                weekofyear = int(mobj.group(2))
                data_date = CMN.CLS.FinanceWeek.weekofyear_to_date(year, weekofyear) 
                time_str = "%d-%02d-%02d" % (data_date.year, data_date.month, data_date.day)
            else:
            	mobj = re.match("([\d]{2})'([\d]{2})/([\d]{2})", td_elements[0].text)
                time_str = "20%s-%s-%s" % (mobj.group(1), mobj.group(2), mobj.group(3))
            data_element_list = [time_str,]
            for td_element in td_elements[1:]:
                data_element_list.append(td_element.text)
            data_list.append(data_element_list)
    data_list.reverse()
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


def _scrape_legal_persion_buy_sell_accumulate_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
    table_element = driver.find_element_by_class_name("solid_1_padding_3_1_tbl")
    tr_elements = table_element.find_elements_by_tag_name("tr")
    for tr_element in tr_elements[2:]:
        td_elements = tr_element.find_elements_by_tag_name("td")
        for td_element in td_elements[1:4]:
            print td_element.text


class GoodInfoWebScrapyMeta(type):

    __ATTRS = {
# market start
# market end
# stock start
        "_scrape_financial_ratio_growth_rate_": _scrape_financial_ratio_growth_rate_,
        "_scrape_institutional_investor_net_buy_sell_": _scrape_institutional_investor_net_buy_sell_,
        "_scrape_legal_persion_buy_sell_accumulate_": _scrape_legal_persion_buy_sell_accumulate_,
# stock end
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class GoodInfoScrapy(ScrapyBase.ScrapyBase):

    __metaclass__ = GoodInfoWebScrapyMeta

    __GOODINFO_HOME_ULR = "https://goodinfo.tw/StockInfo/"

    __MARKET_SCRAPY_CFG = {
    }

    __STOCK_SCRAPY_CFG = {
        "financial ratio growth rate": { # 財務比率成長率
            "url_format": __GOODINFO_HOME_ULR + "StockFinDetail.asp?STOCK_ID=%s",
            "table_time_unit_url_list": ["&RPT_CAT=XX_M_QUAR", "&RPT_CAT=XX_M_QUAR_ACC", "&RPT_CAT=XX_M_YEAR",],
            "table_time_unit_description_list": [u"季", u"累季", u"年",],
        },
        "institutional investor net buy sell": { # 三大法人買賣超
            "url_format": __GOODINFO_HOME_ULR + "ShowBuySaleChart.asp?STOCK_ID=%s",
            "table_time_unit_url_list": ["&CHT_CAT=DATE", "&CHT_CAT=WEEK", "&CHT_CAT=MONTH",],
            "table_time_unit_description_list": [u"日", u"週", u"月",],
        },
        "institutional investor net buy sell accumulate": { # 三大法人買賣超累計
            "url_format": __GOODINFO_HOME_ULR + "ShowBuySaleChart.asp?STOCK_ID=%s",
            "table_time_unit_url_list": ["&CHT_CAT=DATE", "&CHT_CAT=WEEK", "&CHT_CAT=MONTH",],
            "table_time_unit_description_list": [u"日", u"週", u"月",],
        },
    }

    SCRAPY_TRANSFROM_CFG = {
        "quarterly financial ratio growth rate": ["financial ratio growth rate", {"stock_time_unit": 0,}],
        "yearly financial ratio growth rate": ["financial ratio growth rate", {"stock_time_unit": 2,}],
    }

    __MARKET_URL = {key: value["url"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_TIME_UNIT_URL_LIST = {key: value["table_time_unit_url_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    # __TABLE_XPATH = {}
    # __TABLE_XPATH.update(__MARKET_TABLE_XPATH)
    # __TABLE_XPATH.update(__STOCK_TABLE_XPATH)

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
        "financial ratio growth rate": _scrape_financial_ratio_growth_rate_,
        "institutional investor net buy sell": _scrape_institutional_investor_net_buy_sell_,
        "institutional investor net buy sell accumulate": _scrape_legal_persion_buy_sell_accumulate_,
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    def __init__(self, **cfg):
        super(GoodInfoScrapy, self).__init__()
# For the variables which are NOT changed during scraping
        # self.xcfg = {
        #     "dry_run_only": False,
        #     "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
        #     "max_data_count": None,
        # }
        # self.xcfg.update(cfg)
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
                    stock_time_unit = GOODINFO_STOCK_TABLE_DEF_TIME_UNIT
                url_format += self.__STOCK_TIME_UNIT_URL_LIST[scrapy_method][stock_time_unit]
                url = url_format % self.company_number
            else:
                raise ValueError("Unknown scrapy method: %s" % self.scrapy_method)
        self.webdriver.get(url)
        # kwargs['table_xpath'] = self.__STOCK_TABLE_XPATH[self.scrapy_method]
        return (self.__FUNC_PTR[scrapy_method])(self.webdriver, *args, **scrapy_kwargs)


if __name__ == '__main__':
    with GoodInfoScrapy() as goodinfo:
        goodinfo.CompanyNumber = "6274"
        kwargs = {}
        goodinfo.scrape("institutional investor net buy sell", **kwargs)
    	# kwargs["table_data_count"] = 2
		# for scrapy_method in GoodInfoWebScrapy.get_scrapy_method_list():
		# 	goodinfo.scrape(scrapy_method, **kwargs)
		# 	print "\n"
		# goodinfo.scrape("legal_persion_buy_sell", **kwargs)
		# import pdb; pdb.set_trace()
		# (scrapy_list, scrapy_time_list, scrapy_name_list) = goodinfo.scrape("income statement", 2)
		# import pdb; pdb.set_trace()
	# 	print "Done"
