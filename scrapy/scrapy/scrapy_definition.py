#! /usr/bin/python
# -*- coding: utf8 -*-

from collections import OrderedDict
import scrapy.common as CMN

# CMONEY_TABLE0_DIVIDEND_MAX_DATA_COUNT = 8
# CMONEY_TABLE1_MAX_DATA_COUNT = 8
# # CMONEY_TABLE_DEF_DATA_COUNT = 1
# CMONEY_STOCK_TABLE_DEF_TIME_UNIT = 0
# GOODINFO_STOCK_TABLE_DEF_TIME_UNIT = 0

# STATEMENT_DOG_SHEET_INTERVAL_LAST_3_YEARS = 0
# STATEMENT_DOG_SHEET_INTERVAL_LAST_5_YEARS = 1
# STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS = 2
# STATEMENT_DOG_SHEET_INTERVAL_SELF_DEFINE = 3

# CSV_DATA_TIME_DURATION_FILENAME = ".csv_time_range.conf"

# SCRAPY_METHOD_TYPE_SELENIUM_STOCK = {"need_selenium": True, "need_company_number": True,}
# SCRAPY_METHOD_TYPE_SELENIUM_MARKET = {"need_selenium": True, "need_company_number": False,}
# SCRAPY_METHOD_TYPE_REQUESTS_STOCK = {"need_selenium": False, "need_company_number": True,}
# SCRAPY_METHOD_TYPE_REQUESTS_MARKET = {"need_selenium": False, "need_company_number": False,}

# SCRAPY_METHOD_CONSTANT_CFG = collections.OrderedDict()

# # Market Start
# SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_NAME = "option put call ratio"
# SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_CFG = {# 臺指選擇權賣權買權比
#     "description": u'臺指選擇權賣權買權比',
#     "module_name": "taifex_scrapy",
#     "class_name": "TaifexScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_MONTH,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_NAME] = SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_CFG.update(SCRAPY_METHOD_TYPE_REQUESTS_MARKET)

# SCRAPY_METHOD_TFE_OPEN_INTEREST_NAME = "TFE open interest"
# SCRAPY_METHOD_TFE_OPEN_INTEREST_CFG = {
#     "description": u'台指期未平倉(大額近月、法人所有月)',
#     "module_name": "wearn_scrapy",
#     "class_name": "WEarnWebScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_DAY,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_TFE_OPEN_INTEREST_NAME] = SCRAPY_METHOD_TFE_OPEN_INTEREST_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_MARKET)
# # Market End

# # Stock Start
# SCRAPY_METHOD_REVENUE_NAME = "revenue"
# SCRAPY_METHOD_REVENUE_CFG = {# 營收盈餘
#     "description": u'營收盈餘',
#     "module_name": "cmoney_scrapy",
#     "class_name": "CMoneyWebScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_MONTH,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_REVENUE_NAME] = SCRAPY_METHOD_REVENUE_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)

# SCRAPY_METHOD_PROFITABILITY_NAME = "profitability"
# SCRAPY_METHOD_PROFITABILITY_CFG = {# 獲利能力
#     "description": u'獲利能力',
#     "module_name": "cmoney_scrapy",
#     "class_name": "CMoneyWebScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_QUARTER,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_PROFITABILITY_NAME] = SCRAPY_METHOD_PROFITABILITY_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)

# SCRAPY_METHOD_CASHFLOW_STATEMENT_NAME = "cashflow statement"
# SCRAPY_METHOD_CASHFLOW_STATEMENT_CFG = {# 現金流量表
#     "description": u'現金流量表',
#     "module_name": "cmoney_scrapy",
#     "class_name": "CMoneyWebScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_QUARTER,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_CASHFLOW_STATEMENT_NAME] = SCRAPY_METHOD_CASHFLOW_STATEMENT_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)

# SCRAPY_METHOD_DIVIDEND_NAME = "dividend"
# SCRAPY_METHOD_DIVIDEND_CFG = {# 股利政策
#     "description": u'股利政策',
#     "module_name": "cmoney_scrapy",
#     "class_name": "CMoneyWebScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_YEAR,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_DIVIDEND_NAME] = SCRAPY_METHOD_DIVIDEND_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)

# SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_NAME = "institutional investor net buy sell"
# SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_CFG = {# 三大法人買賣超
#     "description": u'三大法人買賣超',
#     "module_name": "goodinfo_scrapy",
#     "class_name": "GoodInfoWebScrapy",
#     "url_time_unit": CMN.DEF.DATA_TIME_UNIT_DAY,
# },
# SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_NAME] = SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)
# # Stock End

# SCRAPY_METHOD_NAME = SCRAPY_METHOD_CONSTANT_CFG.keys()
# SCRAPY_METHOD_DESCRIPTION = [cfg["description"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

# SCRAPY_METHOD_LEN = len(SCRAPY_METHOD_DESCRIPTION)

# SCRAPY_METHOD_URL_TIME_UNIT = [cfg['url_time_unit'] for cfg in SCRAPY_CLASS_CONSTANT_CFG.items()]

# SCRAPY_MODULE_NAME_MAPPING = [cfg["module_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

# SCRAPY_MODULE_NAME_BY_METHOD_MAPPING = [cfg['module_name'] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
# SCRAPY_MODULE_NAME_BY_METHOD_MAPPING_LEN = len(SCRAPY_MODULE_NAME_BY_METHOD_MAPPING)

# SCRAPY_CLASS_NAME_MAPPING = [cfg["class_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

# SCRAPY_CLASS_METHOD = [cfg["scrapy_class_method"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
# SCRAPY_CSV_FILENAME = [class_method.replace(" ", "_").lower() for class_method in SCRAPY_CLASS_METHOD]

# SCRAPY_MEMTHOD_TFE_OPEN_INTEREST_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'台指期未平倉')

# SCRAPY_MEMTHOD_REVENUE_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'營收盈餘')
# SCRAPY_MEMTHOD_PROFITABILITY_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'獲利能力')
# SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'現金流量表')
# SCRAPY_MEMTHOD_DIVIDEND_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'股利政策')
# SCRAPY_MEMTHOD_INSTITUTIONAL_INESTOR_NET_BUY_SELL_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'三大法人買賣超')

# __scrapy_class_market_index_list__ = [index for index, cfg in enumerate(SCRAPY_CLASS_CONSTANT_CFG) if cfg['scrapy_type'] == 'market']
# # semi-open interval
# SCRAPY_MARKET_METHOD_START = min(__scrapy_class_market_index_list__)
# SCRAPY_MARKET_METHOD_END = max(__scrapy_class_market_index_list__) + 1

# __scrapy_class_stock_index_list__ = [index for index, cfg in enumerate(SCRAPY_CLASS_CONSTANT_CFG) if cfg['scrapy_type'] == 'stock']
# # semi-open interval
# SCRAPY_STOCK_METHOD_START = min(__scrapy_class_stock_index_list__)
# SCRAPY_STOCK_METHOD_END = max(__scrapy_class_stock_index_list__) + 1

# SCRAPY_METHOD_START = 0
# SCRAPY_METHOD_END = len(SCRAPY_CLASS_CONSTANT_CFG)
