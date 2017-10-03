# -*- coding: utf8 -*-

import os
import re
import errno
import logging
import calendar
from datetime import datetime, timedelta
import common_function as CMN_FUNC
# import web_scrapy_logging as WSL
# g_logger = WSL.get_web_scrapy_logger()


FINANCE_ANALYSIS_UNKNOWN = -1
FINANCE_ANALYSIS_MARKET = 0
FINANCE_ANALYSIS_STOCK = 1

FINANCE_MODE = FINANCE_ANALYSIS_UNKNOWN
IS_FINANCE_MARKET_MODE = False
IS_FINANCE_STOCK_MODE = True

FINANCE_MODE_DESCRIPTION = ["Market", "Stock",]

PROJECT_FOLDERPATH = CMN_FUNC.get_project_folderpath()
PROJECT_LIB_FOLDERPATH = "%s/libs" % PROJECT_FOLDERPATH

TIME_OVERLAP_NONE = 0 # The new and original time range does NOT overlap
TIME_OVERLAP_BEFORE = 1 # The new time range overlaps the original one
TIME_OVERLAP_AFTER = 2 # The new time range overlaps the original one
TIME_OVERLAP_COVER = 3 # The new time range covers the original one
TIME_OVERLAP_COVERED = 4 # The new time range is covered by the original one

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

DATA_TIME_DURATION_UNDEF = -1
DATA_TIME_DURATION_TODAY = 0
DATA_TIME_DURATION_LAST = 1
DATA_TIME_DURATION_RANGE = 2

DATA_TIME_UNIT_DAY = 0
DATA_TIME_UNIT_WEEK = 1
DATA_TIME_UNIT_MONTH = 2
DATA_TIME_UNIT_QUARTER = 3
DATA_TIME_UNIT_YEAR = 4

TIMESLICE_GENERATE_BY_WORKDAY = 0
TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER = 1
TIMESLICE_GENERATE_BY_MONTH = 2
TIMESLICE_GENERATE_BY_REVENUE = 3
TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON = 4

TIMESLICE_GENERATE_TO_TIME_UNIT_MAPPING = {
    TIMESLICE_GENERATE_BY_WORKDAY: DATA_TIME_UNIT_DAY,
    TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER: DATA_TIME_UNIT_WEEK,
    TIMESLICE_GENERATE_BY_MONTH: DATA_TIME_UNIT_MONTH,
    TIMESLICE_GENERATE_BY_REVENUE: DATA_TIME_UNIT_MONTH,
    TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON: DATA_TIME_UNIT_QUARTER,
}

REPUBLIC_ERA_YEAR_OFFSET = 1911
START_YEAR = 1950
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
STATEMENT_START_QUARTER_STR = "2013q1"
DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR = "2010-01-01"
TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_START_DATE_STR = "2014-12-01"
TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_DUMMY_COMPANY_CODE_NUMBER = "8888"
# Config filename
# SCRAPY_BEGIN_DATE_STR = START_DATE_STR
# MARKET_LAST_CONFIG_FILENAME = "def_market_last.conf"
MARKET_ALL_TIME_RANGE_CONFIG_FILENAME = "def_market_all_time_range.conf"
# STOCK_LAST_CONFIG_FILENAME = "def_stock_last.conf"
STOCK_ALL_TIME_RANGE_CONFIG_FILENAME = "def_stock_all_time_range.conf"
WORKDAY_CANLENDAR_CONF_FILENAME = ".workday_canlendar.conf"
COMPANY_PROFILE_CONF_FILENAME = ".company_profile.conf"
COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME = ".company_profile_change_list.conf"
COMPANY_GROUP_CONF_FILENAME = ".company_group.conf"
BALANCE_SHEET_FIELD_NAME_CONF_FILENAME = "balance_sheet_field_name.conf"
INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME = "income_statement_field_name.conf"
CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME = "cash_flow_statement_field_name.conf"
STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME = "statement_of_changes_in_equity_field_name.conf"
STATEMENT_FIELD_NAME_CONF_FILENAME = [
    BALANCE_SHEET_FIELD_NAME_CONF_FILENAME,
    INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME,
    CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME,
    STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME,
]
BALANCE_SHEET_INTEREST_FIELD_METADATA_FILENAME = "balance_sheet_interest_field_metadata"
INCOME_STATEMENT_INTEREST_FIELD_METADATA_FILENAME = "income_statement_interest_field_metadata"
CASH_FLOW_STATEMENT_INTEREST_FIELD_METADATA_FILENAME = "cash_flow_statement_interest_field_metadata"
STATEMENT_OF_CHANGES_IN_EQUITY_INTEREST_FIELD_METADATA_FILENAME = "statement_of_changes_in_equity_interest_field_metadata"
FINANCE_ANALYSIS_CONF_FILENAME = ".finance_analysis.conf"
MARKET_STOCK_SWITCH_CONF_FILENAME = "market_stock_switch.conf"
CSV_DATA_TIME_DURATION_FILENAME = ".csv_time_range.conf"
COMMAND_EXAMPLE_FILENAME = "command_example"
# Constants
COPY_CONF_FILE_DST_PROJECT_NAME1 = "finance_recorder_java"
COPY_CONF_FILE_DST_PROJECT_NAME2 = "finance_analyzer"
TODAY_MARKET_DATA_EXIST_HOUR = 20
TODAY_MARKET_DATA_EXIST_MINUTE = 0
TODAY_STOCK_DATA_EXIST_HOUR = 20
TODAY_STOCK_DATA_EXIST_MINUTE = 0
CONF_FOLDER = "conf"
CSV_ROOT_FOLDERPATH = "/var/tmp/finance"
CSV_DST_MERGE_ROOT_FOLDERPATH = "/var/tmp/finance_merge"
CSV_MARKET_FOLDERNAME = "market"
CSV_STOCK_FOLDERNAME = "stock"
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
COLUMN_FIELD_START_FLAG_IN_CONFIG = "===== Column Field Start =====" # Caution: Can't start with '#' which is ignored while reading config file
CONFIG_TIMESTAMP_STRING_PREFIX = "#time@"

SCRAPY_CLASS_CONSTANT_CFG = [
# Market Start
    {# 臺股指數及成交量
        "description": u'臺股指數及成交量',
        "module_name": "stock_exchange_and_volume",
        "module_folder": "market",
        "class_name": "WebScrapyStockExchangeAndVolume",
        "url_format": "http://www.twse.com.tw/exchangeReport/FMTQIK?response=json&date={0}{1:02d}01", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_data_selector": 'data',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_MONTH,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 三大法人買賣金額統計表
        "description": u'三大法人現貨買賣超',
        "module_name": "stock_top3_legal_persons_net_buy_or_sell",
        "module_folder": "market",
        "class_name": "WebScrapyStockTop3LegalPersonsNetBuyOrSell",
        "url_format": "http://www.twse.com.tw/fund/BFI82U?response=json&dayDate={0}{1:02d}{2:02d}&type=day", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_data_selector": 'data',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 融資融券餘額統計表
        "description": u'現貨融資融券餘額',
        "module_name": "stock_margin_trading_and_short_selling",
        "module_folder": "market",
        "class_name": "WebScrapyStockMarginTradingAndShortSelling",
        "url_format": "http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate={0}%2F{1}%2F{2}&selectType=MS", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        # "url_data_selector": '.board_trad tr',
        "url_data_selector": 'tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', 'tr'),
    },
    {# 期貨和選擇權未平倉口數與契約金額
        "description": u'三大法人期貨和選擇權留倉淨額',
        "module_name": "future_and_option_top3_legal_persons_open_interest",
        "module_folder": "market",
        "class_name": "WebScrapyFutureAndOptionTop3LegalPersonsOpenInterest",
        "url_format": "http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F09%2F04", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": '.table_c tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_c tr'), 
    },
    {# 期貨或選擇權未平倉口數與契約金額
        "description": u'三大法人期貨或選擇權留倉淨額',
        "module_name": "future_or_option_top3_legal_persons_open_interest",
        "module_folder": "market",
        "class_name": "WebScrapyFutureOrOptionTop3LegalPersonsOpenInterest",
        "url_format": "http://www.taifex.com.tw/chinese/3/7_12_2.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F09%2F04", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": '.table_f tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),
    },
    {# 臺指選擇權買賣權未平倉口數與契約金額
        "description": u'三大法人選擇權買賣權留倉淨額',
        "module_name": "option_top3_legal_persons_buy_and_sell_option_open_interest",
        "module_folder": "market",
        "class_name": "WebScrapyOptionTop3LegalPersonsBuyAndSellOptionOpenInterest",
        "url_format": "http://www.taifex.com.tw/chinese/3/7_12_5.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F9%2F4&COMMODITY_ID=TXO", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": '.table_f tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),  
    },
    {# 臺指選擇權賣權買權比
        "description": u'三大法人選擇權賣權買權比',
        "module_name": "option_put_call_ratio",
        "module_folder": "market",
        "class_name": "WebScrapyOptionPutCallRatio",
        "url_format": "http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart={0}%2F{1}%2F{2}&dateend={0}%2F{1}%2F{3}", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": '.table_a tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_MONTH,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_a tr'),    
    },
    {# 期貨大額交易人未沖銷部位結構表 : 臺股期貨
        "description": u'十大交易人及特定法人期貨資訊',
        "module_name": "future_top10_dealers_and_legal_persons",
        "module_folder": "market",
        "class_name": "WebScrapyFutureTop10DealersAndLegalPersons",
        "url_format": "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": '.table_f tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),    
    },
# Market End
###############################################################################
# Stock Start
    {# 集保戶股權分散表
        "description": u'個股集保戶股權分散表',
        "module_name": "company_depository_shareholder_distribution_table",
        "module_folder": "stock",
        "class_name": "WebScrapyDepositoryShareholderDistributionTable",
        "url_format": "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE={0}{1:02d}{2:02d}&SqlMethod=StockNo&StockNo={3}&StockName=&sub=%ACd%B8%DF", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_BIG5,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": 'table tbody tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER,
        "company_group_market_type": MARKET_TYPE_NONE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 資產負債表
        "description": u'資產負債表',
        "module_name": "balance_sheet",
        "module_folder": "stock",
        "class_name": "WebScrapyBalanceSheet",
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb03?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_time_unit": DATA_TIME_UNIT_QUARTER,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": 'table tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "company_group_market_type": MARKET_TYPE_NONE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 損益表
        "description": u'損益表',
        "module_name": "income_statement",
        "module_folder": "stock",
        "class_name": "WebScrapyIncomeStatement",
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb04?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_time_unit": DATA_TIME_UNIT_QUARTER,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": 'table tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "company_group_market_type": MARKET_TYPE_NONE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 現金流量表, the data are accumulated quarter by quarter in each year
        "description": u'現金流量表',
        "module_name": "cash_flow_statement",
        "module_folder": "stock",
        "class_name": "WebScrapyCashFlowStatement",
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb05?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_time_unit": DATA_TIME_UNIT_QUARTER,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_data_selector": 'table tr',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "company_group_market_type": MARKET_TYPE_NONE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 股東權益變動表
        "description": u'股東權益變動表',
        "module_name": "statement_of_changes_in_equity",
        "module_folder": "stock",
        "class_name": "WebScrapyStatementOfChangesInEquity",
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb06?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_time_unit": DATA_TIME_UNIT_QUARTER,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_CUSTOMIZATION, 
        "url_data_selector": '', # Useless when url_parsing_method is PARSE_URL_DATA_BY_CUSTOMIZATION
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "company_group_market_type": MARKET_TYPE_NONE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 上市日股價及成交量
        "description": u'上市個股日股價及成交量',
        "module_name": "daily_stock_price_and_volume",
        "module_folder": "stock",
        "class_name": "WebScrapyDailyStockPriceAndVolume",
        "url_format": "http://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={0}{1:02d}01&stockNo={2}", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_data_selector": 'data',
        "url_data_exist_selector": 'stat',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_MONTH,
        "company_group_market_type": MARKET_TYPE_STOCK_EXCHANGE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 上櫃日股價及成交量
        "description": u'上櫃個股日股價及成交量',
        "module_name": "daily_stock_price_and_volume",
        "module_folder": "stock",
        "class_name": "WebScrapyOTCDailyStockPriceAndVolume",
        "url_format": "http://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={0}/{1:02d}&stkno={2}", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_data_selector": 'aaData',
        "url_data_exist_selector": 'iTotalRecords',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_MONTH,
        "company_group_market_type": MARKET_TYPE_OVER_THE_COUNTER,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 上市三大法人個股買賣超日報
        "description": u'上市三大法人個股買賣超日報',
        "module_name": "top3_legal_persons_stock_net_buy_and_sell_summary",
        "module_folder": "stock",
        "class_name": "WebScrapyTop3LegalPersonsStockNetBuyOrSellSummary",
        "url_format": "http://www.twse.com.tw/fund/T86?response=json&date={0}{1:02d}{2:02d}&selectType=ALL", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_data_selector": 'data',
        "url_data_exist_selector": 'stat',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        "company_group_market_type": MARKET_TYPE_STOCK_EXCHANGE,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 上櫃三大法人個股買賣超日報
        "description": u'上櫃三大法人個股買賣超日報',
        "module_name": "top3_legal_persons_stock_net_buy_and_sell_summary",
        "module_folder": "stock",
        "class_name": "WebScrapyOTCTop3LegalPersonsStockNetBuyOrSellSummary",
        "url_format": "http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d={0}/{1:02d}/{2:02d}", 
        "url_time_unit": DATA_TIME_UNIT_DAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_data_selector": 'aaData',
        "url_data_exist_selector": 'iTotalRecords',
        "timeslice_generate_method": TIMESLICE_GENERATE_BY_WORKDAY,
        "company_group_market_type": MARKET_TYPE_OVER_THE_COUNTER,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
# Stock End
]

SCRAPY_CLASS_DESCRIPTION = [cfg["description"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
SCRAPY_CLASS_DESCRIPTION_LEN = len(SCRAPY_CLASS_DESCRIPTION)

SCRAPY_MARKET_METHOD_DESCRIPTION = [
# Market Start
    u'臺股指數及成交量',
    u'三大法人現貨買賣超',
    u'現貨融資融券餘額',
    u'三大法人期貨和選擇權留倉淨額',
    u'三大法人期貨或選擇權留倉淨額',
    u'三大法人選擇權買賣權留倉淨額',
    u'三大法人選擇權賣權買權比',
    u'十大交易人及特定法人期貨資訊',
# Market End
]

SCRAPY_STOCK_METHOD_DESCRIPTION = [
# Stock Start
    u'個股集保戶股權分散表',
    u'資產負債表',
    u'損益表',
    u'現金流量表',
    u'股東權益變動表',
    u'個股日股價及成交量',
    u'三大法人個股買賣超日報',
# Stock End
]
SCRAPY_METHOD_DESCRIPTION = SCRAPY_MARKET_METHOD_DESCRIPTION + SCRAPY_STOCK_METHOD_DESCRIPTION
# Semi-open interval
SCRAPY_MARKET_METHOD_START = 0
SCRAPY_MARKET_METHOD_END = len(SCRAPY_MARKET_METHOD_DESCRIPTION)
SCRAPY_STOCK_METHOD_START = SCRAPY_MARKET_METHOD_END
SCRAPY_STOCK_METHOD_END = len(SCRAPY_METHOD_DESCRIPTION)

SCRAPY_STOCK_METHOD_STATMENT_START = SCRAPY_METHOD_DESCRIPTION.index(u'資產負債表')
SCRAPY_STOCK_METHOD_STATMENT_END = SCRAPY_METHOD_DESCRIPTION.index(u'股東權益變動表') + 1

TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_SCRAPY_CLASS_INDEX_LIST = [
    SCRAPY_CLASS_DESCRIPTION.index(u'上市三大法人個股買賣超日報'),
    SCRAPY_CLASS_DESCRIPTION.index(u'上櫃三大法人個股買賣超日報'),
]

WORKDAY_CANLENDAR_SCRAPY_CLASS_INDEX = SCRAPY_CLASS_DESCRIPTION.index(u"臺股指數及成交量")
DEPOSITORY_SHAREHOLDER_DISTRIBUTION_TABLE_SCRAPY_CLASS_INDEX = SCRAPY_CLASS_DESCRIPTION.index(u"個股集保戶股權分散表")

NO_SUPPORT_MULTITHREAD_SCRAPY_CLASS_INDEX = []
NO_SUPPORT_MULTITHREAD_SCRAPY_CLASS_INDEX.append(TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_SCRAPY_CLASS_INDEX_LIST)

SCRAPY_MODULE_NAME_PREFIX = "web_scrapy_"
SCRAPY_MODULE_NAME_MAPPING = [cfg["module_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
SCRAPY_MODULE_NAME_MAPPING_LEN = len(SCRAPY_MODULE_NAME_MAPPING)

SCRAPY_MODULE_FOLDER_MAPPING = [cfg["module_folder"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]
# Semi-open interval
SCRAPY_MARKET_CLASS_START = 0
SCRAPY_MARKET_CLASS_END = SCRAPY_MODULE_FOLDER_MAPPING.index("stock")
SCRAPY_STOCK_CLASS_START = SCRAPY_MARKET_CLASS_END
SCRAPY_STOCK_CLASS_END = len(SCRAPY_MODULE_FOLDER_MAPPING)
SCRAPY_MARKET_CLASS_SIZE = SCRAPY_MARKET_CLASS_END - SCRAPY_MARKET_CLASS_START
SCRAPY_STOCK_CLASS_SIZE = SCRAPY_STOCK_CLASS_END - SCRAPY_STOCK_CLASS_START

SCRAPY_STOCK_CLASS_STATMENT_START = SCRAPY_MODULE_NAME_MAPPING.index("balance_sheet")
SCRAPY_STOCK_CLASS_STATMENT_END = SCRAPY_MODULE_NAME_MAPPING.index("statement_of_changes_in_equity") + 1

# DATA_SOURCE_STATEMENT_OF_CHANGES_IN_EQUITY_INDEX = SCRAPY_MODULE_NAME_MAPPING.index("statement_of_changes_in_equity")

SCRAPY_CLASS_NAME_MAPPING = [cfg["class_name"] for cfg in SCRAPY_CLASS_CONSTANT_CFG]

MIN_DATE_STRING_LENGTH = 8
MAX_DATE_STRING_LENGTH = 10
MIN_MONTH_STRING_LENGTH = 5
MAX_MONTH_STRING_LENGTH = 7
MIN_QUARTER_STRING_LENGTH = 4
MAX_QUARTER_STRING_LENGTH = 6

TIME_DURATION_TYPE_DESCRIPTION = [
    "Today",
    "Last",
    "Time Range",
]

########################################################################################
