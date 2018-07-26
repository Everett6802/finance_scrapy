#! /usr/bin/python
# -*- coding: utf8 -*-

CMONEY_TABLE0_DIVIDEND_MAX_DATA_COUNT = 8
CMONEY_TABLE1_MAX_DATA_COUNT = 8
CMONEY_TABLE_DEF_DATA_COUNT = 1
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
    },
# Stock End
]

SCRAPY_METHOD_DESCRIPTION = [cfg["description"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_METHOD_LEN = len(SCRAPY_MODULE_DESCRIPTION)

SCRAPY_MODULE_NAME_MAPPING = [cfg["module_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

SCRAPY_CLASS_NAME_MAPPING = [cfg["class_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
