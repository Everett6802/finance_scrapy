# -*- coding: utf8 -*-

import os
import re
import errno
# import logging
import calendar
from datetime import datetime, timedelta
from collections import OrderedDict
# import common_function as CMN_FUNC


FINANCE_MODE_UNKNOWN = -1
FINANCE_MODE_MARKET = 0
FINANCE_MODE_STOCK = 1

# FINANCE_MODE_DESCRIPTION = ["Market", "Stock",]

TIME_OVERLAP_NONE = 0 # The new and original time range does NOT overlap
TIME_OVERLAP_BEFORE = 1 # The new time range overlaps the original one
TIME_OVERLAP_AFTER = 2 # The new time range overlaps the original one
TIME_OVERLAP_COVER = 3 # The new time range covers the original one
TIME_OVERLAP_COVERED = 4 # The new time range is covered by the original one
TIME_OVERLAP_POTENTIAL_BEFORE = 5 # The new time range POTENTIALLY overlaps the original one, need furthur check
TIME_OVERLAP_POTENTIAL_AFTER = 6 # The new time range POTENTIALLY overlaps the original one, need furthur check

#################################################################################
# Return Value
RET_SUCCESS = 0

RET_WARN_BASE = 100
RET_WARN_URL_NOT_EXIST = RET_WARN_BASE + 1

RET_FAILURE_BASE = 100
RET_FAILURE_UNKNOWN = RET_FAILURE_BASE + 1
RET_FAILURE_TIMEOUT = RET_FAILURE_BASE + 2

#################################################################################

CAN_PRINT_CONSOLE = True

RUN_RESULT_FILENAME = "run_result"
TIME_FILENAME_FORMAT = "%04d%02d%02d%02d%02d"
DATE_STRING_FORMAT = "%04d-%02d-%02d"
SNAPSHOT_FILENAME_FORMAT = "snapshot_result%s.tar.gz" % TIME_FILENAME_FORMAT

WRITE2CSV_ONE_MONTH_PER_FILE = 0
WRITE2CSV_ONE_DAY_PER_FILE = 1

PARSE_URL_DATA_BY_BS4 = 0
PARSE_URL_DATA_BY_JSON = 1
PARSE_URL_DATA_BY_CUSTOMIZATION = 2

URL_ENCODING_BIG5 = 'big5'
URL_ENCODING_UTF8 = 'utf-8'

MARKET_TYPE_NONE = -1
MARKET_TYPE_STOCK_EXCHANGE = 0
MARKET_TYPE_OVER_THE_COUNTER = 1

# DATA_TIME_DURATION_UNDEF = -1
# DATA_TIME_DURATION_RANGE = 0
# # DATA_TIME_DURATION_TODAY = 1
# DATA_TIME_DURATION_LAST = 1
# DATA_TIME_DURATION_UNTIL_LAST = 2
# # DATA_TIME_DURATION_TODAY = 3
# # DATA_TIME_DURATION_UNTIL_TODAY = 4
# DATA_TIME_DURATION_RANGE_ALL = 5 # Only support for setting config from file

DATA_TIME_UNIT_DAY = 0
DATA_TIME_UNIT_WEEK = 1
DATA_TIME_UNIT_MONTH = 2
DATA_TIME_UNIT_QUARTER = 3
DATA_TIME_UNIT_YEAR = 4

TIMESLICE_GENERATE_BY_WORKDAY = 0
TIMESLICE_GENERATE_BY_DAY_RANGE = 1
# TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER = 1
TIMESLICE_GENERATE_BY_MONTH = 2
# TIMESLICE_GENERATE_BY_REVENUE = 2
# TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON = 3

TIMESLICE_GENERATE_BY_TIME_RANGE = [
    TIMESLICE_GENERATE_BY_DAY_RANGE,
]

DEF_TIME_RANGE_DAY = 30
DEF_TIME_RANGE_WEEK = 52
DEF_TIME_RANGE_MONTH = 12
DEF_TIME_RANGE_QUARTER = 4
DEF_TIME_RANGE_YEAR = 5

DEF_TIME_RANGE_LIST = [
    DEF_TIME_RANGE_DAY,
    DEF_TIME_RANGE_WEEK,
    DEF_TIME_RANGE_MONTH,
    DEF_TIME_RANGE_QUARTER,
    DEF_TIME_RANGE_YEAR,
]

TIMESLICE_GENERATE_TO_TIME_UNIT_MAPPING = {
    TIMESLICE_GENERATE_BY_WORKDAY: DATA_TIME_UNIT_DAY,
    TIMESLICE_GENERATE_BY_DAY_RANGE: DATA_TIME_UNIT_DAY,
    # TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER: DATA_TIME_UNIT_WEEK,
    TIMESLICE_GENERATE_BY_MONTH: DATA_TIME_UNIT_MONTH,
    # TIMESLICE_GENERATE_BY_REVENUE: DATA_TIME_UNIT_MONTH,
    # TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON: DATA_TIME_UNIT_QUARTER,
}

REPUBLIC_ERA_YEAR_OFFSET = 1911
START_YEAR = 2000
END_YEAR = 2100
START_DATE_STR = "%d-01-01" % START_YEAR
END_DATE_STR = "%d-01-01" % END_YEAR
REPUBLIC_ERA_START_YEAR = START_YEAR - REPUBLIC_ERA_YEAR_OFFSET
REPUBLIC_ERA_END_YEAR = END_YEAR - REPUBLIC_ERA_YEAR_OFFSET
WORKDAY_CANLENDAR_START_DATE_STR = "2000-01-01"
START_QUARTER = 1
END_QUARTER = 4
START_MONTH = 1
END_MONTH = 12
START_DAY = 1
# STATEMENT_START_QUARTER_STR = "2013q1"
# DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR = "2010-01-01"
# TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_START_DATE_STR = "2014-12-01"
TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_DUMMY_COMPANY_CODE_NUMBER = "8888"
# Config filename
# SCRAPY_BEGIN_DATE_STR = START_DATE_STR
# MARKET_LAST_CONFIG_FILENAME = "def_market_last.conf"
# MARKET_ALL_TIME_RANGE_CONFIG_FILENAME = "def_market_all_time_range.conf"
# STOCK_LAST_CONFIG_FILENAME = "def_stock_last.conf"
# STOCK_ALL_TIME_RANGE_CONFIG_FILENAME = "def_stock_all_time_range.conf"
FINANCE_MODE_SWITCH_CONF_FILENAME = "finance_mode_switch.conf"
FINANCE_SCRAPY_CONF_FILENAME = "finance_scrapy.conf"
WORKDAY_CANLENDAR_CONF_FILENAME = ".workday_canlendar.conf"
TFEI_ACCOUNTING_DAY_CONF_FILENAME = ".tfei_accounting_day.conf"
STW_ACCOUNTING_DAY_CONF_FILENAME = ".stw_accounting_day.conf"
TRIPLE_WITCHING_DAY_CONF_FILENAME = ".triple_witching_day.conf"
FINANCIAL_STATEMENT_DAY_CONF_FILENAME = ".financial_statement_day.conf"
COMPANY_PROFILE_CONF_FILENAME = ".company_profile.conf"
COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME = ".company_profile_change_list.conf"
COMPANY_GROUP_CONF_FILENAME = ".company_group.conf"
# BALANCE_SHEET_FIELD_NAME_CONF_FILENAME = "balance_sheet_field_name.conf"
# INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME = "income_statement_field_name.conf"
# CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME = "cash_flow_statement_field_name.conf"
# STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME = "statement_of_changes_in_equity_field_name.conf"
# STATEMENT_FIELD_NAME_CONF_FILENAME = [
#     BALANCE_SHEET_FIELD_NAME_CONF_FILENAME,
#     INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME,
#     CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME,
#     STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME,
# ]
# BALANCE_SHEET_INTEREST_FIELD_METADATA_FILENAME = "balance_sheet_interest_field_metadata"
# INCOME_STATEMENT_INTEREST_FIELD_METADATA_FILENAME = "income_statement_interest_field_metadata"
# CASH_FLOW_STATEMENT_INTEREST_FIELD_METADATA_FILENAME = "cash_flow_statement_interest_field_metadata"
# STATEMENT_OF_CHANGES_IN_EQUITY_INTEREST_FIELD_METADATA_FILENAME = "statement_of_changes_in_equity_interest_field_metadata"
FINANCE_MODE_CONF_FILENAME = ".finance_analysis.conf"
# MARKET_STOCK_SWITCH_CONF_FILENAME = "market_stock_switch.conf"
CSV_DATA_TIME_DURATION_FILENAME = ".csv_time_range.conf"
CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX = "_column_description.conf"
# DATASET_CONF_FILENAME = ".dataset.conf"
COMMAND_EXAMPLE_FILENAME = "command_example"
# Constants
PROJECT_NAME_PREFIX = "finance_"
FINANCE_SCRAPY_PROJECT_NAME = "finance_scrapy"
FINANCE_RECORDER_PROJECT_NAME = "finance_recorder"
FINANCE_ANALYZER_PROJECT_NAME = "finance_analyzer"
FINANCE_DATASET_PROJECT_NAME = "finance_dataset"
TODAY_DATA_EXIST_HOUR = 22
TODAY_DATA_EXIST_MINUTE = 0
# TODAY_STOCK_DATA_EXIST_HOUR = 20
# TODAY_STOCK_DATA_EXIST_MINUTE = 0
SCRAPY_FOLDERNAME = "scrapy"
CONF_FOLDERNAME = "conf"
DATA_FOLDERNAME = "data"
# DATASET_FOLDERNAME = "dataset"
CSV_ROOT_FOLDERPATH = "/var/tmp/finance"
CSV_DST_MERGE_ROOT_FOLDERPATH = "/var/tmp/finance_merge"
CSV_MARKET_FOLDERNAME = "market"
CSV_STOCK_FOLDERNAME = "stock"
CSV_FIELD_DESCRIPTION_FOLDERNAME = "field_description"
MISSING_CSV_MARKET_FILENAME = ".missing_csv_market"
MISSING_CSV_STOCK_FILENAME = ".missing_csv_stock"
TIME_RANGE_FOLDERNAME = ".time_range"
SNAPSHOT_FOLDER = "snapshot"
UNICODE_ENCODING_IN_FILE = "utf-8"
SCRAPY_WAIT_TIMEOUT = 8
SCRAPY_RETRY_TIMES = 3
COMMA_DATA_SPLIT = ","
SPACE_DATA_SPLIT = " "
COLON_DATA_SPLIT = ":"
# COLUMN_FIELD_START_FLAG_IN_CONFIG = "===== Column Field Start =====" # Caution: Can't start with '#' which is ignored while reading config file
CONFIG_TIMESTAMP_STRING_PREFIX = "#time@"
DATE_IN_CHINESE = u"日期"

SCCRAPY_CLASS_COMPANY_NUMBER = '2330'
SCCRAPY_CLASS_COMPANY_GROUP_NUMBER = 9

SCRAPY_MODULE_FOLDER = "scrapy"

SCRAPY_METHOD_TYPE_SELENIUM_STOCK = {"need_selenium": True, "need_company_number": True,}
SCRAPY_METHOD_TYPE_SELENIUM_MARKET = {"need_selenium": True, "need_company_number": False,}
SCRAPY_METHOD_TYPE_REQUESTS_STOCK = {"need_selenium": False, "need_company_number": True,}
SCRAPY_METHOD_TYPE_REQUESTS_MARKET = {"need_selenium": False, "need_company_number": False,}

SCRAPY_METHOD_CONSTANT_CFG = OrderedDict()

# Market Start
SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_NAME = "taiwan weighted index and volume"
SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_CFG = {# 臺股指數及成交量
# URL Ex: http://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date=20190101
    "description": u'臺股指數及成交量',
    "module_name": "twse_scrapy",
    "class_name": "TwseScrapy",
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "scrapy_time_unit": TIMESLICE_GENERATE_BY_MONTH,
    # "scrapy_time_slice_size": 1,
    "can_set_time_range": True,
    "csv_flush_threshold": 100, # Valid only when can_set_time_range is True
}
SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_CFG.update(SCRAPY_METHOD_TYPE_REQUESTS_MARKET)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_NAME] = SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_CFG

SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_NAME = "taiwan future index and lot"
SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_CFG = {# 臺股期貨指數(近月)及成交口數
# URL Ex: http://www.taifex.com.tw/cht/3/futDailyMarketReport?queryType=2&marketCode=0&commodity_id=TX&queryDate=2019%2F01%2F21
    "description": u'臺股期貨指數(近月)及成交口數',
    "module_name": "taifex_scrapy",
    "class_name": "TaifexScrapy",
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "scrapy_time_unit": TIMESLICE_GENERATE_BY_WORKDAY,
    "can_set_time_range": True,
    "csv_flush_threshold": 30, # Valid only when can_set_time_range is True
}
SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_CFG.update(SCRAPY_METHOD_TYPE_REQUESTS_MARKET)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_NAME] = SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_CFG

SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_NAME = "option put call ratio"
SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_CFG = {# 臺指選擇權賣權買權比
# URL Ex: http://www.taifex.com.tw/cht/3/pcRatio?queryStartDate=2019%2F02%2F12&queryEndDate=2019%2F02%2F18
# Caution: Can NOT scrape more than 30 days for each time
    "description": u'臺指選擇權賣權買權比',
    "module_name": "taifex_scrapy",
    "class_name": "TaifexScrapy",
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "scrapy_time_unit": TIMESLICE_GENERATE_BY_DAY_RANGE,
    "scrapy_time_slice_size": 29,
    "can_set_time_range": True,
    "csv_flush_threshold": 150, # Valid only when can_set_time_range is True
}
SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_CFG.update(SCRAPY_METHOD_TYPE_REQUESTS_MARKET)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_NAME] = SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_CFG

SCRAPY_METHOD_TFE_OPEN_INTEREST_NAME = "tfe open interest"
SCRAPY_METHOD_TFE_OPEN_INTEREST_CFG = {
# URL Ex: https://stock.wearn.com/taifexphoto.asp
    "description": u'台指期未平倉(大額近月、法人所有月)',
    "module_name": "wearn_scrapy",
    "class_name": "WEarnScrapy",
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "can_set_time_range": False,
}
SCRAPY_METHOD_TFE_OPEN_INTEREST_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_MARKET)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_TFE_OPEN_INTEREST_NAME] = SCRAPY_METHOD_TFE_OPEN_INTEREST_CFG

SCRAPY_METHOD_VIX_NAME = "vix"
SCRAPY_METHOD_VIX_CFG = {
    "description": u'VIX波動率',
    "module_name": "stockq_scrapy",
    "class_name": "StockQScrapy",
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "can_set_time_range": False,
}
SCRAPY_METHOD_VIX_CFG.update(SCRAPY_METHOD_TYPE_REQUESTS_MARKET)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_VIX_NAME] = SCRAPY_METHOD_VIX_CFG
# Market End

# Stock Start
SCRAPY_METHOD_REVENUE_NAME = "revenue"
SCRAPY_METHOD_REVENUE_CFG = {# 營收盈餘
    "description": u'營收盈餘',
    "module_name": "cmoney_scrapy",
    "class_name": "CMoneyScrapy",
    "data_time_unit": DATA_TIME_UNIT_MONTH,
    "can_set_time_range": False,
}
SCRAPY_METHOD_REVENUE_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_REVENUE_NAME] = SCRAPY_METHOD_REVENUE_CFG

SCRAPY_METHOD_PROFITABILITY_NAME = "profitability"
SCRAPY_METHOD_PROFITABILITY_CFG = {# 獲利能力
    "description": u'獲利能力',
    "module_name": "cmoney_scrapy",
    "class_name": "CMoneyScrapy",
    "data_time_unit": DATA_TIME_UNIT_QUARTER,
    "can_set_time_range": False,
}
SCRAPY_METHOD_PROFITABILITY_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_PROFITABILITY_NAME] = SCRAPY_METHOD_PROFITABILITY_CFG

SCRAPY_METHOD_CASHFLOW_STATEMENT_NAME = "cashflow statement"
SCRAPY_METHOD_CASHFLOW_STATEMENT_CFG = {# 現金流量表
    "description": u'現金流量表',
    "module_name": "cmoney_scrapy",
    "class_name": "CMoneyScrapy",
    "data_time_unit": DATA_TIME_UNIT_QUARTER,
    "can_set_time_range": False,
}
SCRAPY_METHOD_CASHFLOW_STATEMENT_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_CASHFLOW_STATEMENT_NAME] = SCRAPY_METHOD_CASHFLOW_STATEMENT_CFG

SCRAPY_METHOD_DIVIDEND_NAME = "dividend"
SCRAPY_METHOD_DIVIDEND_CFG = {# 股利政策
    "description": u'股利政策',
    "module_name": "cmoney_scrapy",
    "class_name": "CMoneyScrapy",
    "data_time_unit": DATA_TIME_UNIT_YEAR,
    "can_set_time_range": False,
}
SCRAPY_METHOD_DIVIDEND_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_DIVIDEND_NAME] = SCRAPY_METHOD_DIVIDEND_CFG

SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_NAME = "institutional investor net buy sell"
SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_CFG = {# 三大法人買賣超
    "description": u'三大法人買賣超',
    "module_name": "goodinfo_scrapy",
    "class_name": "GoodInfoScrapy",
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "can_set_time_range": False,
}
SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_CFG.update(SCRAPY_METHOD_TYPE_SELENIUM_STOCK)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_NAME] = SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_CFG

SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_NAME = "stock price and volume"
SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_CFG = {# 個股股價及成交量
# URL Ex: http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo=2458&date=20190301
# URL Ex: http://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d=108/03&stkno=1264
    "description": u'個股股價及成交量',
    "module_name": ["twse_scrapy", "tpex_scrapy",],
    "class_name": ["TwseScrapy", "TpexScrapy",],
    "data_time_unit": DATA_TIME_UNIT_DAY,
    "scrapy_time_unit": TIMESLICE_GENERATE_BY_MONTH,
    "can_set_time_range": True,
    "csv_flush_threshold": 150, # Valid only when can_set_time_range is True
}
SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_CFG.update(SCRAPY_METHOD_TYPE_REQUESTS_STOCK)
SCRAPY_METHOD_CONSTANT_CFG[SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_NAME] = SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_CFG
# Stock End

SCRAPY_METHOD_NAME = SCRAPY_METHOD_CONSTANT_CFG.keys()
SCRAPY_METHOD_INDEX_CONSTANT_CFG = SCRAPY_METHOD_CONSTANT_CFG.values()
SCRAPY_METHOD_LEN = len(SCRAPY_METHOD_NAME)
SCRAPY_METHOD_NAME_TO_INDEX = {name: index for index, name in enumerate(SCRAPY_METHOD_NAME)}
SCRAPY_METHOD_DESCRIPTION = [cfg["description"] for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_METHOD_DATA_TIME_UNIT = [cfg['data_time_unit'] for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_METHOD_MODULE_NAME = [cfg["module_name"] for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_METHOD_CLASS_NAME = [cfg["class_name"] for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_CSV_FILENAME = [method_name.replace(" ", "_").lower() for method_name in SCRAPY_METHOD_NAME]
SCRAPY_DATA_TIME_UNIT = [cfg["data_time_unit"] for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_METHOD_SCRAPY_TIME_UNIT = [(cfg["scrapy_time_unit"] if cfg.has_key("scrapy_time_unit") else None) for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_TIME_SLICE_DEFUALT_SIZE = [(cfg["scrapy_time_slice_size"] if cfg.has_key("scrapy_time_slice_size") else None) for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_CSV_FLUSH_THRESHOLD = [(cfg["csv_flush_threshold"] if cfg.has_key("csv_flush_threshold") else None) for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
SCRAPY_METHOD_NEED_SELECT_CLASS = [(True if type(cfg["class_name"]) is list else False) for cfg in SCRAPY_METHOD_CONSTANT_CFG.values()]
MARKET_SCRAPY_METHOD_INDEX = [index for index, cfg in enumerate(SCRAPY_METHOD_CONSTANT_CFG.values()) if not cfg['need_company_number']]
STOCK_SCRAPY_METHOD_INDEX = [index for index, cfg in enumerate(SCRAPY_METHOD_CONSTANT_CFG.values()) if cfg['need_company_number']]

SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_TAIWAN_WEIGHTED_INDEX_AND_VOLUME_NAME)
SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_TAIWAN_FUTURE_INDEX_AND_LOT_NAME)
SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_OPTION_PUT_CALL_RATIO_NAME)
SCRAPY_METHOD_TFE_OPEN_INTEREST_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_TFE_OPEN_INTEREST_NAME)
SCRAPY_METHOD_VIX_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_VIX_NAME)
SCRAPY_METHOD_REVENUE_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_REVENUE_NAME)
SCRAPY_METHOD_PROFITABILITY_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_PROFITABILITY_NAME)
SCRAPY_METHOD_CASHFLOW_STATEMENT_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_CASHFLOW_STATEMENT_NAME)
SCRAPY_METHOD_DIVIDEND_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_DIVIDEND_NAME)
SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_INSTITUTIONAL_INVESTOR_NET_BUY_SELL_NAME)
SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_INDEX = SCRAPY_METHOD_NAME.index(SCRAPY_METHOD_STOCK_PRICE_AND_VOLUME_NAME)

SCRAPY_METHOD_START = 0
SCRAPY_METHOD_END = len(SCRAPY_METHOD_NAME)

MIN_DATE_STRING_LENGTH = 8
MAX_DATE_STRING_LENGTH = 10
MIN_WEEK_STRING_LENGTH = 5
MAX_WEEK_STRING_LENGTH = 7
MIN_MONTH_STRING_LENGTH = 5
MAX_MONTH_STRING_LENGTH = 7
MIN_QUARTER_STRING_LENGTH = 4
MAX_QUARTER_STRING_LENGTH = 6
MIN_YEAR_STRING_LENGTH = 2
MAX_YEAR_STRING_LENGTH = 4

TIME_DURATION_TYPE_DESCRIPTION = [
    "Today",
    "Last",
    "Time Range",
]

########################################################################################
