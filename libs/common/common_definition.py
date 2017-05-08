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

TIMESLICE_TO_TIME_UNIT_MAPPING = {
    TIMESLICE_GENERATE_BY_WORKDAY: DATA_TIME_UNIT_DAY,
    TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER: DATA_TIME_UNIT_WEEK,
    TIMESLICE_GENERATE_BY_MONTH: DATA_TIME_UNIT_MONTH,
    TIMESLICE_GENERATE_BY_REVENUE: DATA_TIME_UNIT_MONTH,
    TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON: DATA_TIME_UNIT_QUARTER,
}

DEF_REPUBLIC_ERA_YEAR_OFFSET = 1911
DEF_START_YEAR = 2000
DEF_END_YEAR = 2100
DEF_START_DATE_STR = "%d-01-01" % DEF_START_YEAR
DEF_END_DATE_STR = "%d-01-01" % DEF_END_YEAR
DEF_REPUBLIC_ERA_START_YEAR = DEF_START_YEAR - DEF_REPUBLIC_ERA_YEAR_OFFSET
DEF_REPUBLIC_ERA_END_YEAR = DEF_END_YEAR - DEF_REPUBLIC_ERA_YEAR_OFFSET
DEF_START_QUARTER = 1
DEF_END_QUARTER = 4
DEF_START_MONTH = 1
DEF_END_MONTH = 12
DEF_START_DAY = 1
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
DEF_BALANCE_SHEET_FIELD_NAME_CONF_FILENAME = ".balance_sheet_field_name.conf"
DEF_INCOME_STATEMENT_FIELD_NAME_CONF_FILENAME = ".income_statement_field_name.conf"
DEF_CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME = ".cash_flow_statement_field_name.conf"
DEF_STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME = ".statement_of_changes_in_equity_field_name.conf"
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
DEF_COMMA_DATA_SPLIT = ",";
DEF_SPACE_DATA_SPLIT = " ";
DEF_COLUMN_FIELD_START_FLAG_IN_CONFIG = "===== Column Field Start =====" # Caution: Can't start with '#' which is ignored while reading config file
DEF_CUR_TIMESTAMP_STRING_PREFIX = "# time@"

DEF_DATA_SOURCE_INDEX_MAPPING = [
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
###############################################################################
# Stock Start
    u'個股集保戶股權分散表',
    u'資產負債表',
    u'損益表',
    u'現金流量表',
    u'股東權益變動表',
    # u'三大法人上市個股買賣超彙總',
    # u'三大法人上櫃個股買賣超彙總',
    # u'外資及陸資上市個股投資持股統計',
    # u'外資及陸資買賣超彙總',
    # u'投信買賣超彙總',
    # u'自營商買賣超彙總',
# Stock End
]
DEF_DATA_SOURCE_INDEX_MAPPING_LEN = len(DEF_DATA_SOURCE_INDEX_MAPPING)

DEF_WEB_SCRAPY_MODULE_NAME_PREFIX = "web_scrapy_"
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING = [
# Market Start
    "stock_exchange_and_volume",
    "stock_top3_legal_persons_net_buy_or_sell",
    "stock_margin_trading_and_short_selling",
    "future_and_option_top3_legal_persons_open_interest",
    "future_or_option_top3_legal_persons_open_interest",
    "option_top3_legal_persons_buy_and_sell_option_open_interest",
    "option_put_call_ratio",
    "future_top10_dealers_and_legal_persons",
# Market End
###############################################################################
# Stock Start
    "company_depository_shareholder_distribution_table",
    "balance_sheet",
    "income_statement",
    "cash_flow_statement",
    "statement_of_changes_in_equity",
    # "company_stock_top3_legal_persons_net_buy_and_sell_summary",
    # "otc_company_stock_top3_legal_persons_net_buy_and_sell_summary",
    # "company_foreign_investors_shareholder",
    # "company_foreign_investors_net_buy_and_sell_summary",
    # "company_investment_trust_net_buy_and_sell_summary",
    # "company_dealers_net_buy_and_sell_summary",
# Stock End
]
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN = len(DEF_WEB_SCRAPY_MODULE_NAME_MAPPING)

DEF_WEB_SCRAPY_MODULE_FOLDER_MAPPING = [
    "market",
    "market",
    "market",
    "market",
    "market",
    "market",
    "market",
    "market",
    "stock",
    "stock",
    "stock",
    "stock",
    "stock",
]
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

DEF_WEB_SCRAPY_CLASS_NAME_MAPPING = [
# Market Start
    "WebScrapyStockExchangeAndVolume",
    "WebScrapyStockTop3LegalPersonsNetBuyOrSell",
    "WebScrapyStockMarginTradingAndShortSelling",
    "WebScrapyFutureAndOptionTop3LegalPersonsOpenInterest",
    "WebScrapyFutureOrOptionTop3LegalPersonsOpenInterest",
    "WebScrapyOptionTop3LegalPersonsBuyAndSellOptionOpenInterest",
    "WebScrapyOptionPutCallRatio",
    "WebScrapyFutureTop10DealersAndLegalPersons",
# Market End
###############################################################################
# Stock Start
    "WebScrapyDepositoryShareholderDistributionTable",
    "WebScrapyBalanceSheet",
    "WebScrapyIncomeStatement",
    "WebScrapyCashFlowStatement",
    "WebScrapyStatementOfChangesInEquity",
    # "WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary",
    # "WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary",
    # "WebScrapyCompanyForeignInvestorsShareholder",
    # "WebScrapyCompanyForeignInvestorsNetBuyOrSellSummary",
    # "WebScrapyCompanyInvestmentTrustNetBuyOrSellSummary",
    # "WebScrapyCompanyDealersNetBuyOrSellSummary",
# Stock End
]

# DEF_WORKDAY_CANLENDAR_SOURCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyStockExchangeAndVolume")
DEF_WORKDAY_CANLENDAR_SOURCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyOptionPutCallRatio")
DEF_DEPOSITORY_SHAREHOLDER_DISTRIBUTION_TABLE_SOURCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyDepositoryShareholderDistributionTable")

DEF_SOURCE_URL_PARSING = [
# Market Start
    {# 臺股指數及成交量
        "url_format": "http://www.twse.com.tw/ch/trading/exchange/FMTQIK/genpage/Report{0}{1:02d}/{0}{1:02d}_F3_1_2.php?STK_NO=&myear={0}&mmon={1:02d}", 
        "url_timeslice": TIMESLICE_GENERATE_BY_MONTH,
        "url_encoding": URL_ENCODING_BIG5,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.board_trad tr',
        "url_multi_data_one_page": True,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 三大法人買賣金額統計表
        "url_format": "http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date={0}%2F{1}%2F{2}&mSubmit=%ACd%B8%DF&yr=1979&w_date=19790904&m_date=19790904", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_BIG5,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.board_trad tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', '.board_trad tr'),
    },
    {# 融資融券餘額統計表
        "url_format": "http://www.twse.com.tw/ch/trading/exchange/MI_MARGN/MI_MARGN.php?download=&qdate={0}%2F{1}%2F{2}&selectType=MS", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        # "url_css_selector": '.board_trad tr',
        "url_css_selector": 'tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', 'tr'),
    },
    {# 期貨和選擇權未平倉口數與契約金額
        "url_format": "http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F09%2F04", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.table_c tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_c tr'), 
    },
    {# 期貨或選擇權未平倉口數與契約金額
        "url_format": "http://www.taifex.com.tw/chinese/3/7_12_2.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F09%2F04", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.table_f tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),
    },
    {# 臺指選擇權買賣權未平倉口數與契約金額
        "url_format": "http://www.taifex.com.tw/chinese/3/7_12_5.asp?goday=&DATA_DATE_Y=1979&DATA_DATE_M=9&DATA_DATE_D=4&syear={0}&smonth={1}&sday={2}&datestart=1979%2F9%2F4&COMMODITY_ID=TXO", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.table_f tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),  
    },
    {# 臺指選擇權賣權買權比
        "url_format": "http://www.taifex.com.tw/chinese/3/PCRatio.asp?download=&datestart={0}%2F{1}%2F{2}&dateend={0}%2F{1}%2F{3}", 
        "url_timeslice": TIMESLICE_GENERATE_BY_MONTH,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.table_a tr',
        "url_multi_data_one_page": True,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_a tr'),    
    },
    {# 期貨大額交易人未沖銷部位結構表 : 臺股期貨
        "url_format": "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.table_f tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('utf-8', '.table_f tr'),    
    },
# Market End
###############################################################################
# Stock Start
    {# 集保戶股權分散表
        "url_format": "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE={0}{1}{2}&SqlMethod=StockNo&StockNo={3}&StockName=&sub=%ACd%B8%DF", 
        "url_timeslice": TIMESLICE_GENERATE_BY_COMPANY_FOREIGN_INVESTORS_SHAREHOLDER,
        "url_encoding": URL_ENCODING_BIG5,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": 'table tbody tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 資產負債表
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb03?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_timeslice": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": 'table tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 損益表
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb04?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_timeslice": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": 'table tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 現金流量表
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb05?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_timeslice": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": 'table tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 股東權益變動表
        "url_format": "http://mops.twse.com.tw/mops/web/ajax_t164sb06?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id={0}&year={1}&season={2}", 
        "url_timeslice": TIMESLICE_GENERATE_BY_FINANCIAL_STATEMENT_SEASON,
        "url_encoding": URL_ENCODING_UTF8,
        "url_parsing_method": PARSE_URL_DATA_BY_CUSTOMIZATION, 
        "url_css_selector": '', # Useless when url_parsing_method is PARSE_URL_DATA_BY_CUSTOMIZATION
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    # {# 三大法人上市個股買賣超日報
    #     "url_format": "http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date={0}%2F{1}%2F{2}&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+", 
    #     "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
    #     "url_encoding": URL_ENCODING_BIG5,
    #     "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
    #     "url_css_selector": '.board_trad tr',
    #     "url_multi_data_one_page": False,
    #     # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    # },
    # {# 三大法人上櫃個股買賣超日報
    #     "url_format": "http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d={0}/{1}/{2}&_=1460104675945", 
    #     "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
    #     "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
    #     "url_css_selector": 'aaData',
    #     "url_multi_data_one_page": False,
    #     # "parse_url_data_obj": CMN_CLS.ParseURLDataByJSON('aaData'),    
    # },
# Stock End
]

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
