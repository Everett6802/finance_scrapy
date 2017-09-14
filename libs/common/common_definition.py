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

DEF_PROJECT_FOLDERPATH = CMN_FUNC.get_project_folderpath()
DEF_PROJECT_LIB_FOLDERPATH = "%s/libs" % DEF_PROJECT_FOLDERPATH

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

DEF_REPUBLIC_ERA_YEAR_OFFSET = 1911
DEF_START_YEAR = 1950
DEF_END_YEAR = 2100
DEF_START_DATE_STR = "%d-01-01" % DEF_START_YEAR
DEF_END_DATE_STR = "%d-01-01" % DEF_END_YEAR
DEF_REPUBLIC_ERA_START_YEAR = DEF_START_YEAR - DEF_REPUBLIC_ERA_YEAR_OFFSET
DEF_REPUBLIC_ERA_END_YEAR = DEF_END_YEAR - DEF_REPUBLIC_ERA_YEAR_OFFSET
DEF_WORKDAY_CANLENDAR_START_DATE_STR = "2000-01-01"
DEF_START_QUARTER = 1
DEF_END_QUARTER = 4
DEF_START_MONTH = 1
DEF_END_MONTH = 12
DEF_START_DAY = 1
DEF_STATEMENT_START_QUARTER_STR = "2013q1"
DEF_DAILY_STOCK_PRICE_AND_VOLUME_START_DATE_STR = "2010-01-01"
DEF_TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_START_DATE_STR = "2014-12-01"
DEF_TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_DUMMY_COMPANY_CODE_NUMBER = "8888"
# Config filename
# DEF_WEB_SCRAPY_BEGIN_DATE_STR = DEF_START_DATE_STR
# DEF_MARKET_LAST_CONFIG_FILENAME = "def_market_last.conf"
DEF_MARKET_ALL_TIME_RANGE_CONFIG_FILENAME = "def_market_all_time_range.conf"
# DEF_STOCK_LAST_CONFIG_FILENAME = "def_stock_last.conf"
DEF_STOCK_ALL_TIME_RANGE_CONFIG_FILENAME = "def_stock_all_time_range.conf"
DEF_WORKDAY_CANLENDAR_CONF_FILENAME = ".workday_canlendar.conf"
DEF_COMPANY_PROFILE_CONF_FILENAME = ".company_profile.conf"
DEF_COMPANY_PROFILE_CHANGE_LIST_CONF_FILENAME = ".company_profile_change_list.conf"
DEF_COMPANY_GROUP_CONF_FILENAME = ".company_group.conf"
DEF_BALANCE_SHEET_FIELD_NAME_CONF_FILENAME = "balance_sheet_field_name.conf"
DEF_INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME = "income_statement_field_name.conf"
DEF_CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME = "cash_flow_statement_field_name.conf"
DEF_STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME = "statement_of_changes_in_equity_field_name.conf"
DEF_STATEMENT_FIELD_NAME_CONF_FILENAME = [
    DEF_BALANCE_SHEET_FIELD_NAME_CONF_FILENAME,
    DEF_INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME,
    DEF_CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME,
    DEF_STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME,
]
DEF_BALANCE_SHEET_INTEREST_FIELD_METADATA_FILENAME = "balance_sheet_interest_field_metadata"
DEF_INCOME_STATEMENT_INTEREST_FIELD_METADATA_FILENAME = "income_statement_interest_field_metadata"
DEF_CASH_FLOW_STATEMENT_INTEREST_FIELD_METADATA_FILENAME = "cash_flow_statement_interest_field_metadata"
DEF_STATEMENT_OF_CHANGES_IN_EQUITY_INTEREST_FIELD_METADATA_FILENAME = "statement_of_changes_in_equity_interest_field_metadata"
DEF_FINANCE_ANALYSIS_CONF_FILENAME = ".finance_analysis.conf"
DEF_MARKET_STOCK_SWITCH_CONF_FILENAME = "market_stock_switch.conf"
DEF_CSV_DATA_TIME_DURATION_FILENAME = ".csv_time_range.conf"
DEF_COMMAND_EXAMPLE_FILENAME = "command_example"
# Constants
DEF_COPY_CONF_FILE_DST_PROJECT_NAME1 = "finance_recorder_java"
DEF_COPY_CONF_FILE_DST_PROJECT_NAME2 = "finance_analyzer"
DEF_TODAY_MARKET_DATA_EXIST_HOUR = 20
DEF_TODAY_MARKET_DATA_EXIST_MINUTE = 0
DEF_TODAY_STOCK_DATA_EXIST_HOUR = 20
DEF_TODAY_STOCK_DATA_EXIST_MINUTE = 0
DEF_CONF_FOLDER = "conf"
DEF_CSV_ROOT_FOLDERPATH = "/var/tmp/finance"
DEF_CSV_DST_MERGE_ROOT_FOLDERPATH = "/var/tmp/finance_merge"
DEF_CSV_MARKET_FOLDERNAME = "market"
DEF_CSV_STOCK_FOLDERNAME = "stock"
DEF_MISSING_CSV_MARKET_FILENAME = ".missing_csv_market"
DEF_MISSING_CSV_STOCK_FILENAME = ".missing_csv_stock"
DEF_TIME_RANGE_FOLDERNAME = ".time_range"
DEF_SNAPSHOT_FOLDER = "snapshot"
DEF_UNICODE_ENCODING_IN_FILE = "utf-8"
DEF_SCRAPY_WAIT_TIMEOUT = 8
DEF_SCRAPY_RETRY_TIMES = 3
DEF_COMMA_DATA_SPLIT = ","
DEF_SPACE_DATA_SPLIT = " "
DEF_COLON_DATA_SPLIT = ":"
DEF_COLUMN_FIELD_START_FLAG_IN_CONFIG = "===== Column Field Start =====" # Caution: Can't start with '#' which is ignored while reading config file
DEF_CONFIG_TIMESTAMP_STRING_PREFIX = "#time@"

DEF_WEB_SCRAPY_CLASS_CONSTANT_CFG = [
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
        "url_format": "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE={0}{1}{2}&SqlMethod=StockNo&StockNo={3}&StockName=&sub=%ACd%B8%DF", 
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
    {# 現金流量表
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

DEF_WEB_SCRAPY_CLASS_CONSTANT_DESCRIPTION = [cfg["description"] for cfg in DEF_WEB_SCRAPY_CLASS_CONSTANT_CFG]

DEF_WEB_SCRAPY_MARKET_METHOD_DESCRIPTION = [
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

DEF_WEB_SCRAPY_STOCK_METHOD_DESCRIPTION = [
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
DEF_WEB_SCRAPY_METHOD_DESCRIPTION = DEF_WEB_SCRAPY_MARKET_METHOD_DESCRIPTION + DEF_WEB_SCRAPY_STOCK_METHOD_DESCRIPTION
# Semi-open interval
DEF_MARKET_METHOD_START = 0
DEF_MARKET_METHOD_END = len(DEF_WEB_SCRAPY_MARKET_METHOD_DESCRIPTION)
DEF_STOCK_METHOD_START = DEF_MARKET_METHOD_END
DEF_STOCK_METHOD_END = len(DEF_WEB_SCRAPY_METHOD_DESCRIPTION)

DEF_DATA_SOURCE_INDEX_MAPPING = DEF_WEB_SCRAPY_CLASS_CONSTANT_DESCRIPTION
DEF_DATA_SOURCE_INDEX_MAPPING_LEN = len(DEF_DATA_SOURCE_INDEX_MAPPING)

DEF_TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_WEB_SCRAPY_CLASS_INDEX = [
    DEF_WEB_SCRAPY_CLASS_CONSTANT_DESCRIPTION.index(u'上市三大法人個股買賣超日報'),
    DEF_WEB_SCRAPY_CLASS_CONSTANT_DESCRIPTION.index(u'上櫃三大法人個股買賣超日報'),
]

DEF_NO_SUPPORT_MULTITHREAD_WEB_SCRAPY_CLASS_INDEX = []
DEF_NO_SUPPORT_MULTITHREAD_WEB_SCRAPY_CLASS_INDEX.append(DEF_TOP3_LEGAL_PERSONS_STOCK_NET_BUY_OR_SELL_SUMMARY_WEB_SCRAPY_CLASS_INDEX)

DEF_WEB_SCRAPY_MODULE_NAME_PREFIX = "web_scrapy_"
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING = [cfg["module_name"] for cfg in DEF_WEB_SCRAPY_CLASS_CONSTANT_CFG]
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN = len(DEF_WEB_SCRAPY_MODULE_NAME_MAPPING)

DEF_WEB_SCRAPY_MODULE_FOLDER_MAPPING = [cfg["module_folder"] for cfg in DEF_WEB_SCRAPY_CLASS_CONSTANT_CFG]
# Semi-open interval
DEF_DATA_SOURCE_MARKET_START = 0
DEF_DATA_SOURCE_MARKET_END = DEF_WEB_SCRAPY_MODULE_FOLDER_MAPPING.index("stock")
DEF_DATA_SOURCE_STOCK_START = DEF_DATA_SOURCE_MARKET_END
DEF_DATA_SOURCE_STOCK_END = len(DEF_WEB_SCRAPY_MODULE_FOLDER_MAPPING)
DEF_DATA_SOURCE_MARKET_SIZE = DEF_DATA_SOURCE_MARKET_END - DEF_DATA_SOURCE_MARKET_START
DEF_DATA_SOURCE_STOCK_SIZE = DEF_DATA_SOURCE_STOCK_END - DEF_DATA_SOURCE_STOCK_START

DEF_DATA_SOURCE_STOCK_STATMENT_START = DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index("balance_sheet")
DEF_DATA_SOURCE_STOCK_STATMENT_END = DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index("statement_of_changes_in_equity") + 1

# DEF_DATA_SOURCE_STATEMENT_OF_CHANGES_IN_EQUITY_INDEX = DEF_WEB_SCRAPY_MODULE_NAME_MAPPING.index("statement_of_changes_in_equity")

DEF_WEB_SCRAPY_CLASS_NAME_MAPPING = [cfg["class_name"] for cfg in DEF_WEB_SCRAPY_CLASS_CONSTANT_CFG]

DEF_WORKDAY_CANLENDAR_SOURCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyStockExchangeAndVolume")
# DEF_WORKDAY_CANLENDAR_SOURCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyOptionPutCallRatio")
DEF_DEPOSITORY_SHAREHOLDER_DISTRIBUTION_TABLE_SOURCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyDepositoryShareholderDistributionTable")

DEF_MIN_DATE_STRING_LENGTH = 8
DEF_MAX_DATE_STRING_LENGTH = 10
DEF_MIN_MONTH_STRING_LENGTH = 5
DEF_MAX_MONTH_STRING_LENGTH = 7
DEF_MIN_QUARTER_STRING_LENGTH = 4
DEF_MAX_QUARTER_STRING_LENGTH = 6

DEF_TIME_DURATION_TYPE_DESCRIPTION = [
    "Today",
    "Last",
    "Time Range",
]

########################################################################################
