#! /usr/bin/python
# -*- coding: utf8 -*-

import scrapy.common as CMN

CMONEY_TABLE0_DIVIDEND_MAX_DATA_COUNT = 8
CMONEY_TABLE1_MAX_DATA_COUNT = 8
# CMONEY_TABLE_DEF_DATA_COUNT = 1
CMONEY_STOCK_TABLE_DEF_TIME_UNIT = 0
GOODINFO_STOCK_TABLE_DEF_TIME_UNIT = 0

STATEMENT_DOG_SHEET_INTERVAL_LAST_3_YEARS = 0
STATEMENT_DOG_SHEET_INTERVAL_LAST_5_YEARS = 1
STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS = 2
STATEMENT_DOG_SHEET_INTERVAL_SELF_DEFINE = 3

SCRAPY_MODULE_FOLDER = "selenium"

CSV_DATA_TIME_DURATION_FILENAME = ".selenium_csv_time_range.conf"

SCRAPY_CLASS_CONSTANT_CFG = [
# Market Start
    {# 台指期未平倉(大額近月、法人所有月)
        "description": u'台指期未平倉',
        "module_name": "wearn_scrapy",
        "class_name": "WEarnWebScrapy",
        "scrapy_class_method": "TFE open interest",
        "scrapy_type": "market", # for defining the method index
        "url_time_unit": CMN.DEF.DATA_TIME_UNIT_DAY,
    },
# Market End
# Stock Start
    {# 營收盈餘
        "description": u'營收盈餘',
        "module_name": "cmoney_scrapy",
        "class_name": "CMoneyWebScrapy",
        "scrapy_class_method": "revenue",
        "scrapy_type": "stock", # for defining the method index
        "url_time_unit": CMN.DEF.DATA_TIME_UNIT_MONTH,
    },
    {# 獲利能力
        "description": u'獲利能力',
        "module_name": "cmoney_scrapy",
        "class_name": "CMoneyWebScrapy",
        "scrapy_class_method": "profitability",
        "scrapy_type": "stock", # for defining the method index
        "url_time_unit": CMN.DEF.DATA_TIME_UNIT_QUARTER,
    },
    {# 現金流量表
        "description": u'現金流量表',
        "module_name": "cmoney_scrapy",
        "class_name": "CMoneyWebScrapy",
        "scrapy_class_method": "cashflow statement",
        "scrapy_type": "stock", # for defining the method index
        "url_time_unit": CMN.DEF.DATA_TIME_UNIT_QUARTER,
    },
    {# 股利政策
        "description": u'股利政策',
        "module_name": "cmoney_scrapy",
        "class_name": "CMoneyWebScrapy",
        "scrapy_class_method": "dividend",
        "scrapy_type": "stock", # for defining the method index
        "url_time_unit": CMN.DEF.DATA_TIME_UNIT_YEAR,
    },
    {# 三大法人買賣超
        "description": u'三大法人買賣超',
        "module_name": "goodinfo_scrapy",
        "class_name": "GoodInfoWebScrapy",
        "scrapy_class_method": "institutional investor net buy sell",
        "scrapy_type": "stock", # for defining the method index
        "url_time_unit": CMN.DEF.DATA_TIME_UNIT_DAY,
    },
# Stock End
]

SCRAPY_METHOD_DESCRIPTION = [cfg["description"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_METHOD_LEN = len(SCRAPY_METHOD_DESCRIPTION)

SCRAPY_METHOD_URL_TIME_UNIT = [cfg['url_time_unit'] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_MODULE_NAME_MAPPING = [cfg["module_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_MODULE_NAME_BY_METHOD_MAPPING = [cfg['module_name'] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
SCRAPY_MODULE_NAME_BY_METHOD_MAPPING_LEN = len(SCRAPY_MODULE_NAME_BY_METHOD_MAPPING)

SCRAPY_CLASS_NAME_MAPPING = [cfg["class_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_CLASS_METHOD = [cfg["scrapy_class_method"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
SCRAPY_CSV_FILENAME = [class_method.replace(" ", "_").lower() for class_method in SCRAPY_CLASS_METHOD]

SCRAPY_MEMTHOD_TFE_OPEN_INTEREST_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'台指期未平倉')

SCRAPY_MEMTHOD_REVENUE_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'營收盈餘')
SCRAPY_MEMTHOD_PROFITABILITY_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'獲利能力')
SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'現金流量表')
SCRAPY_MEMTHOD_DIVIDEND_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'股利政策')
SCRAPY_MEMTHOD_INSTITUTIONAL_INESTOR_NET_BUY_SELL_INDEX = SCRAPY_METHOD_DESCRIPTION.index(u'三大法人買賣超')

__scrapy_class_market_index_list__ = [index for index, cfg in enumerate(SCRAPY_CLASS_CONSTANT_CFG) if cfg['scrapy_type'] == 'market']
# semi-open interval
SCRAPY_MARKET_METHOD_START = min(__scrapy_class_market_index_list__)
SCRAPY_MARKET_METHOD_END = max(__scrapy_class_market_index_list__) + 1

__scrapy_class_stock_index_list__ = [index for index, cfg in enumerate(SCRAPY_CLASS_CONSTANT_CFG) if cfg['scrapy_type'] == 'stock']
# semi-open interval
SCRAPY_STOCK_METHOD_START = min(__scrapy_class_stock_index_list__)
SCRAPY_STOCK_METHOD_END = max(__scrapy_class_stock_index_list__) + 1

SCRAPY_METHOD_START = 0
SCRAPY_METHOD_END = len(SCRAPY_CLASS_CONSTANT_CFG)
