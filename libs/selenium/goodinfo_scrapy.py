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

import libs.common as CMN
import common_definition as CMN_DEF
import gui_scrapy_base as ScrapyBase
g_logger = CMN.LOG.get_logger()

PRINT_SCRAPY = True


def _scrape_institutional_investor_net_buy_sell_(driver, *args, **kwargs):
    # import pdb; pdb.set_trace()
    data_list = []
    table_elements = driver.find_elements_by_class_name("solid_1_padding_4_0_tbl")
    thead_element = table_elements[3].find_element_by_tag_name("thead")
    tbody_elements = table_elements[3].find_elements_by_tag_name("tbody")

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
    # import pdb; pdb.set_trace()
    return (data_list, data_name_list)


def _scrape_legal_persion_buy_sell_accumulate_(driver, *args, **kwargs):
    import pdb; pdb.set_trace()
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
        "_scrape_institutional_investor_net_buy_sell_": _scrape_institutional_investor_net_buy_sell_,
        "_scrape_legal_persion_buy_sell_accumulate_": _scrape_legal_persion_buy_sell_accumulate_,
# stock end
    }

    def __new__(mcs, name, bases, attrs):
        attrs.update(mcs.__ATTRS)
        return type.__new__(mcs, name, bases, attrs)


class GoodInfoWebScrapy(ScrapyBase.GUIWebScrapyBase):

    __metaclass__ = GoodInfoWebScrapyMeta

    __GOODINFO_ULR_PREFIX = "https://goodinfo.tw/StockInfo/"

    __MARKET_SCRAPY_CFG = {
    }

    __STOCK_SCRAPY_CFG = {
        "institutional investor net buy sell": { # 三大法人買賣超
            "url_format": __GOODINFO_ULR_PREFIX + "ShowBuySaleChart.asp?STOCK_ID=%s",
            "table_time_unit_list": ["&CHT_CAT=DATE", "&CHT_CAT=WEEK", "&CHT_CAT=MONTH",],
        },
        "institutional investor net buy sell accumulate": { # 三大法人買賣超累計
            "url_format": __GOODINFO_ULR_PREFIX + "ShowBuySaleChart.asp?STOCK_ID=%s",
            "table_time_unit_list": ["&CHT_CAT=DATE", "&CHT_CAT=WEEK", "&CHT_CAT=MONTH",],
        },
    }

    __MARKET_URL = {key: value["url"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    __MARKET_TIME_UNIT_LIST = {key: value["table_time_unit_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}
    # __MARKET_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __MARKET_SCRAPY_CFG.items()}

    __STOCK_URL_FORMAT = {key: value["url_format"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TABLE_XPATH = {key: value["table_xpath"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    __STOCK_TIME_UNIT_LIST = {key: value["table_time_unit_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}
    # __STOCK_TIME_UNIT_DESCRIPTION_LIST = {key: value["table_time_unit_description_list"] for (key, value) in __STOCK_SCRAPY_CFG.items()}

    # __TABLE_XPATH = {}
    # __TABLE_XPATH.update(__MARKET_TABLE_XPATH)
    # __TABLE_XPATH.update(__STOCK_TABLE_XPATH)

    __TIME_UNIT_LIST = {}
    __TIME_UNIT_LIST.update(__MARKET_TIME_UNIT_LIST)
    __TIME_UNIT_LIST.update(__STOCK_TIME_UNIT_LIST)

    # __TIME_UNIT_DESCRIPTION_LIST = {}
    # __TIME_UNIT_DESCRIPTION_LIST.update(__MARKET_TIME_UNIT_DESCRIPTION_LIST)
    # __TIME_UNIT_DESCRIPTION_LIST.update(__STOCK_TIME_UNIT_DESCRIPTION_LIST)

    __FUNC_PTR = {
# market start
# market end
# stock start
        "institutional investor net buy sell": _scrape_institutional_investor_net_buy_sell_,
        "legal_persion_buy_sell_accumulate": _scrape_legal_persion_buy_sell_accumulate_,
# stock end
    }
    __METHOD_NAME_LIST = __FUNC_PTR.keys()


    # @classmethod
    # def _transform_time_str2obj(cls, time_unit, time_str):
    #     time_obj = None
    #     # import pdb; pdb.set_trace()
    #     if time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
    #         time_obj = CMN.CLS.FinanceMonth(time_str)
    #     # elif time_unit == CMN.DEF.DATA_TIME_UNIT_QUARTER:
    #     #     time_obj = CMN.CLS.FinanceQuarter(time_str)
    #     # elif time_unit == CMN.DEF.DATA_TIME_UNIT_YEAR:
    #     #     time_obj = CMN.CLS.FinanceYear(time_str)
    #     else:
    #         raise ValueError("Unsupport time unit[%d] for transform" % time_unit)
    #     return time_obj


    @classmethod
    def get_scrapy_method_list(cls):
        return cls.__METHOD_NAME_LIST


    @classmethod
    def print_scrapy_method(cls):
        print ", ".join(cls.__METHOD_NAME_LIST)


    @classmethod
    def print_scrapy_method_time_unit_description(cls, scrapy_method):
        print ", ".join(cls.__TIME_UNIT_DESCRIPTION_LIST[scrapy_method])


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
            url_format += self.__STOCK_TIME_UNIT_LIST[self.scrapy_method][stock_time_unit]
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
    with GoodInfoWebScrapy() as goodinfo:
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
