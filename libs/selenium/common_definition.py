#! /usr/bin/python
# -*- coding: utf8 -*-

import libs.common as CMN

CMONEY_TABLE0_DIVIDEND_MAX_DATA_COUNT = 8
CMONEY_TABLE1_MAX_DATA_COUNT = 8
# CMONEY_TABLE_DEF_DATA_COUNT = 1
CMONEY_STOCK_TABLE_DEF_TIME_UNIT = 0

STATEMENT_DOG_SHEET_INTERVAL_LAST_3_YEARS = 0
STATEMENT_DOG_SHEET_INTERVAL_LAST_5_YEARS = 1
STATEMENT_DOG_SHEET_INTERVAL_LAST_8_YEARS = 2
STATEMENT_DOG_SHEET_INTERVAL_SELF_DEFINE = 3

SCRAPY_MODULE_FOLDER = "selenium"

CSV_DATA_TIME_DURATION_FILENAME = ".selenium_csv_time_range.conf"

SCRAPY_CLASS_CONSTANT_CFG = [
# Market Start
# Market End
# Stock Start
    {# 營收盈餘
        "description": u'營收盈餘',
        "module_name": "cmoney_scrapy",
        "class_name": "CMoneyWebScrapy",
        "scrapy_class_method": "revenue",
        "scrapy_type": "stock", # for defining the method index
        "scrapy_data_time_unit": CMN.DEF.DATA_TIME_UNIT_MONTH,
    },
# Stock End
]

SCRAPY_METHOD_DESCRIPTION = [cfg["description"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_METHOD_LEN = len(SCRAPY_METHOD_DESCRIPTION)

SCRAPY_MODULE_NAME_MAPPING = [cfg["module_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_MODULE_NAME_BY_METHOD_MAPPING = [cfg['module_name'] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
SCRAPY_MODULE_NAME_BY_METHOD_MAPPING_LEN = len(SCRAPY_MODULE_NAME_BY_METHOD_MAPPING)

SCRAPY_CLASS_NAME_MAPPING = [cfg["class_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_CLASS_METHOD = [cfg["scrapy_class_method"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_MEMTHOD_REVENUE_INDEX = SCRAPY_CLASS_METHOD.index("revenue")
# __scrapy_class_market_index_list__ = [index for index, cfg in enumerate(SCRAPY_CLASS_CONSTANT_CFG) if cfg['scrapy_type'] == 'market']
# # semi-open interval
# SCRAPY_MARKET_METHOD_START = min(__scrapy_class_market_index_list__)
# SCRAPY_MARKET_METHOD_END = max(__scrapy_class_market_index_list__) + 1

__scrapy_class_stock_index_list__ = [index for index, cfg in enumerate(SCRAPY_CLASS_CONSTANT_CFG) if cfg['scrapy_type'] == 'stock']
# semi-open interval
SCRAPY_MARKET_METHOD_START = -1
SCRAPY_MARKET_METHOD_END = -1

SCRAPY_STOCK_METHOD_START = min(__scrapy_class_stock_index_list__)
SCRAPY_STOCK_METHOD_END = max(__scrapy_class_stock_index_list__) + 1

SCRAPY_METHOD_START = 0
SCRAPY_METHOD_END = len(SCRAPY_CLASS_CONSTANT_CFG)
