# -*- coding: utf8 -*-

import os
import re
import errno
import logging
import calendar
from datetime import datetime, timedelta
import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()
import common_function as CMN_FUNC


FINANCE_ANALYSIS_MARKET = 0
FINANCE_ANALYSIS_STOCK = 1

IS_FINANCE_MARKET_MODE = False
IS_FINANCE_STOCK_MODE = True

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

RUN_RESULT_FILENAME = "run_result"
TIME_FILENAME_FORMAT = "%04d%02d%02d%02d%02d"
DATE_STRING_FORMAT = "%04d-%02d-%02d"
SNAPSHOT_FILENAME_FORMAT = "snapshot_result%s.tar.gz" % TIME_FILENAME_FORMAT

WRITE2CSV_ONE_MONTH_PER_FILE = 0
WRITE2CSV_ONE_DAY_PER_FILE = 1

PARSE_URL_DATA_BY_BS4 = 0
PARSE_URL_DATA_BY_JSON = 1

URL_ENCODING_BIG5 = 'big5'
URL_ENCODING_UTF8 = 'utf-8'

MARKET_TYPE_STOCK_EXCHANGE = 0
MARKET_TYPE_OVER_THE_COUNTER = 1

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

DEF_SOURCE_URL_PARSING = [
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
    {# 三大法人上市個股買賣超日報
        "url_format": "http://www.twse.com.tw/ch/trading/fund/T86/T86.php?input_date={0}%2F{1}%2F{2}&select2=ALL&sorting=by_stkno&login_btn=+%ACd%B8%DF+", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_BIG5,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": '.board_trad tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
    {# 三大法人上櫃個股買賣超日報
        "url_format": "http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&se=AL&t=D&d={0}/{1}/{2}&_=1460104675945", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_parsing_method": PARSE_URL_DATA_BY_JSON, 
        "url_css_selector": 'aaData',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByJSON('aaData'),    
    },
    {# 集保戶股權分散表
        "url_format": "https://www.tdcc.com.tw/smWeb/QryStock.jsp?SCA_DATE={0}{1}{2}&SqlMethod=StockNo&StockNo={3}&StockName=&sub=%ACd%B8%DF", 
        "url_timeslice": TIMESLICE_GENERATE_BY_WORKDAY,
        "url_encoding": URL_ENCODING_BIG5,
        "url_parsing_method": PARSE_URL_DATA_BY_BS4, 
        "url_css_selector": 'table tbody tr',
        "url_multi_data_one_page": False,
        # "parse_url_data_obj": CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
    },
]

# DEF_CSV_TIME_UNIT = [
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,
#     DATA_TIME_UNIT_YEAR,  
# ]

DEF_REPUBLIC_ERA_YEAR_OFFSET = 1911
DEF_START_YEAR = 2000
DEF_END_YEAR = 2100
DEF_REPUBLIC_ERA_START_YEAR = DEF_START_YEAR - DEF_REPUBLIC_ERA_YEAR_OFFSET
DEF_REPUBLIC_ERA_END_YEAR = DEF_END_YEAR - DEF_REPUBLIC_ERA_YEAR_OFFSET
DEF_START_QUARTER = 1
DEF_END_QUARTER = 4
DEF_START_MONTH = 1
DEF_END_MONTH = 12
DEF_START_DAY = 1

DEF_WEB_SCRAPY_BEGIN_DATE_STR = "%d-01-01" % DEF_START_YEAR
DEF_TODAY_CONFIG_FILENAME = "today.conf"
DEF_HISTORY_CONFIG_FILENAME = "history.conf"
DEF_WORKDAY_CANLENDAR_CONF_FILENAME = ".workday_canlendar.conf"
DEF_COMPANY_PROFILE_CONF_FILENAME = ".company_profile.conf"
DEF_COMPANY_GROUP_CONF_FILENAME = ".company_group.conf"
DEF_FINANCE_ANALYSIS_CONF_FILENAME = ".finance_analysis.conf"
DEF_MARKET_STOCK_SWITCH_CONF_FILENAME = "market_stock_switch.conf"
DEF_COPY_CONF_FILE_DST_PROJECT_NAME1 = "finance_recorder_java"
DEF_COPY_CONF_FILE_DST_PROJECT_NAME2 = "finance_analyzer"
DEF_TODAY_MARKET_DATA_EXIST_HOUR = 20
DEF_TODAY_MARKET_DATA_EXIST_MINUTE = 0
DEF_TODAY_STOCK_DATA_EXIST_HOUR = 20
DEF_TODAY_STOCK_DATA_EXIST_MINUTE = 0
DEF_CONF_FOLDER = "conf"
DEF_CSV_FILE_PATH = "/var/tmp/finance"
CSV_MARKET_FOLDERNAME = "market"
CSV_STOCK_FOLDERNAME = "stock"
DEF_SNAPSHOT_FOLDER = "snapshot"
DEF_SCRAPY_WAIT_TIMEOUT = 8
DEF_SCRAPY_RETRY_TIMES = 3
DEF_DATA_SOURCE_INDEX_MAPPING = [
    u'臺股指數及成交量',
    u'三大法人現貨買賣超',
    u'現貨融資融券餘額',
    u'三大法人期貨和選擇權留倉淨額',
    u'三大法人期貨或選擇權留倉淨額',
    u'三大法人選擇權買賣權留倉淨額',
    u'三大法人選擇權賣權買權比',
    u'十大交易人及特定法人期貨資訊',
    u'三大法人上市個股買賣超彙總',
    u'三大法人上櫃個股買賣超彙總',
    u'個股集保戶股權分散表',
    # u'外資及陸資上市個股投資持股統計',
    # u'外資及陸資買賣超彙總',
    # u'投信買賣超彙總',
    # u'自營商買賣超彙總',
]
DEF_DATA_SOURCE_INDEX_MAPPING_LEN = len(DEF_DATA_SOURCE_INDEX_MAPPING)

DEF_WEB_SCRAPY_MODULE_NAME_PREFIX = "web_scrapy_"
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING = [
    "stock_exchange_and_volume",
    "stock_top3_legal_persons_net_buy_or_sell",
    "stock_margin_trading_and_short_selling",
    "future_and_option_top3_legal_persons_open_interest",
    "future_or_option_top3_legal_persons_open_interest",
    "option_top3_legal_persons_buy_and_sell_option_open_interest",
    "option_put_call_ratio",
    "future_top10_dealers_and_legal_persons",
    "company_stock_top3_legal_persons_net_buy_and_sell_summary",
    "otc_company_stock_top3_legal_persons_net_buy_and_sell_summary",
    "company_depository_shareholder_distribution_table",
    # "company_foreign_investors_shareholder",
    # "company_foreign_investors_net_buy_and_sell_summary",
    # "company_investment_trust_net_buy_and_sell_summary",
    # "company_dealers_net_buy_and_sell_summary",
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
    # "company_foreign_investors_shareholder",
    # "company_foreign_investors_net_buy_and_sell_summary",
    # "company_investment_trust_net_buy_and_sell_summary",
    # "company_dealers_net_buy_and_sell_summary",
]

DEF_WEB_SCRAPY_CLASS_NAME_MAPPING = [
    "WebScrapyStockExchangeAndVolume",
    "WebScrapyStockTop3LegalPersonsNetBuyOrSell",
    "WebScrapyStockMarginTradingAndShortSelling",
    "WebScrapyFutureAndOptionTop3LegalPersonsOpenInterest",
    "WebScrapyFutureOrOptionTop3LegalPersonsOpenInterest",
    "WebScrapyOptionTop3LegalPersonsBuyAndSellOptionOpenInterest",
    "WebScrapyOptionPutCallRatio",
    "WebScrapyFutureTop10DealersAndLegalPersons",
    "WebScrapyCompanyStockTop3LegalPersonsNetBuyOrSellSummary",
    "WebScrapyOTCCompanyStockTop3LegalPersonsNetBuyOrSellSummary",
    "WebScrapyDepositoryShareholderDistributionTable",
    # "WebScrapyCompanyForeignInvestorsShareholder",
    # "WebScrapyCompanyForeignInvestorsNetBuyOrSellSummary",
    # "WebScrapyCompanyInvestmentTrustNetBuyOrSellSummary",
    # "WebScrapyCompanyDealersNetBuyOrSellSummary",
]

DEF_COMPANY_DATA_SOURCE_START_INDEX = 0
DEF_STOCK_DATA_SOURCE_START_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyDepositoryShareholderDistributionTable")
DEF_COMPANY_DATA_SOURCE_END_INDEX = DEF_STOCK_DATA_SOURCE_START_INDEX - 1
DEF_STOCK_DATA_SOURCE_END_INDEX = len(DEF_WEB_SCRAPY_CLASS_NAME_MAPPING)
DEF_WORKDAY_CANLENDAR_DATA_SOURCE_REFERENCE_INDEX = DEF_WEB_SCRAPY_CLASS_NAME_MAPPING.index("WebScrapyStockExchangeAndVolume")

# DEF_SOURCE_WRITE2CSV_METHOD = [
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     WRITE2CSV_ONE_DAY_PER_FILE,
#     WRITE2CSV_ONE_DAY_PER_FILE,
#     WRITE2CSV_ONE_MONTH_PER_FILE,
#     # WRITE2CSV_ONE_DAY_PER_FILE,
#     # WRITE2CSV_ONE_DAY_PER_FILE,
#     # WRITE2CSV_ONE_DAY_PER_FILE,
#     # WRITE2CSV_ONE_DAY_PER_FILE,
# ]

DEF_WEB_SCRAPY_DATA_SOURCE_TYPE = [
    "TODAY",
    "HISTORY",
    "USER_DEFINED",
]
DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX = DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index("TODAY")
DEF_WEB_SCRAPY_DATA_SOURCE_HISTORY_INDEX = DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index("HISTORY")
DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX = DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index("USER_DEFINED")
DEF_WEB_SCRAPY_DATA_SOURCE_TYPE_LEN = len(DEF_WEB_SCRAPY_DATA_SOURCE_TYPE)

DEF_MIN_DATE_STRING_LENGTH = 8
DEF_MAX_DATE_STRING_LENGTH = 10
DEF_MIN_MONTH_STRING_LENGTH = 5
DEF_MAX_MONTH_STRING_LENGTH = 7
DEF_MIN_QUARTER_STRING_LENGTH = 4
DEF_MAX_QUARTER_STRING_LENGTH = 6

########################################################################################

# def is_republic_era_year(year_value):
#     if isinstance(year_value, int):
#         return True if (year_value / 1000 == 0) else False
#     elif isinstance(year_value, str):
#         return True if len(year_value) != 4 else False
#     raise ValueError("Unknown year value: %s !!!" % str(year_value))


# def check_year_range(year_value):
#     if is_republic_era_year(year_value):
#         if not (DEF_REPUBLIC_ERA_START_YEAR <= int(year_value) <= DEF_REPUBLIC_ERA_END_YEAR):
#             raise ValueError("The republic era year[%d] is NOT in the range [%d, %d]" % (int(year_value), DEF_REPUBLIC_ERA_START_YEAR, DEF_REPUBLIC_ERA_END_YEAR))
#     else:
#         if not (DEF_START_YEAR <= int(year_value) <= DEF_END_YEAR):
#             raise ValueError("The year[%d] is NOT in the range [%d, %d]" % (int(year_value), DEF_START_YEAR, DEF_END_YEAR))


# def check_quarter_range(quarter_value):
#     if not (DEF_START_QUARTER <= int(quarter_value) <= DEF_END_QUARTER):
#         raise ValueError("The quarter[%d] is NOT in the range [%d, %d]" % (int(quarter_value), DEF_START_QUARTER, DEF_END_QUARTER))


# def check_month_range(month_value):
#     if not (DEF_START_MONTH <= int(month_value) <= DEF_END_MONTH):
#         raise ValueError("The month[%d] is NOT in the range [%d, %d]" % (int(month_value), DEF_START_MONTH, DEF_END_MONTH))


# def check_day_range(day_value, year_value, month_value):
#     end_day_in_month = get_month_last_day(int(year_value), int(month_value))
#     if not (DEF_START_DAY <= int(day_value) <= end_day_in_month):
#         raise ValueError("The day[%d] is NOT in the range [%d, %d]" % (int(day_value), DEF_START_DAY, end_day_in_month))


# def check_date_str_format(date_string):
#     mobj = re.match("([\d]{2,4})-([\d]{2})-([\d]{2})", date_string)
#     if mobj is None:
#         raise ValueError("The string[%s] is NOT date format" % date_string)
#     date_string_len = len(date_string)
#     # if date_string_len < DEF_MIN_DATE_STRING_LENGTH or date_string_len > DEF_MAX_DATE_STRING_LENGTH:
#     if not (DEF_MIN_DATE_STRING_LENGTH <= date_string_len <= DEF_MAX_DATE_STRING_LENGTH):
#         raise ValueError("The date stirng[%s] length is NOT in the range [%d, %d]" % (date_string_len, DEF_MIN_DATE_STRING_LENGTH, DEF_MAX_DATE_STRING_LENGTH))
# # # Check Year Range
# #     check_year_range(mobj.group(1))
# # # Check Month Range
# #     check_month_range(mobj.group(2))
# # # Check Day Range
# #     check_day_range(mobj.group(3), mobj.group(1), mobj.group(2))
#     return mobj


# def check_month_str_format(month_string):
#     mobj = re.match("([\d]{2,4})-([\d]{2})", month_string)
#     if mobj is None:
#         raise ValueError("The string[%s] is NOT month format" % month_string)
#     month_string_len = len(month_string)
#     # if month_string_len < DEF_MIN_MONTH_STRING_LENGTH or month_string_len > DEF_MAX_MONTH_STRING_LENGTH:
#     if not (DEF_MIN_MONTH_STRING_LENGTH <= month_string_len <= DEF_MAX_MONTH_STRING_LENGTH):
#         raise ValueError("The month stirng[%s] length is NOT in the range [%d, %d]" % (month_string_len, DEF_MIN_MONTH_STRING_LENGTH, DEF_MAX_MONTH_STRING_LENGTH))
# # # Check Year Range
# #     check_year_range(mobj.group(1))
# # # Check Month Range
# #     check_month_range(mobj.group(2))
#     return mobj


# def check_quarter_str_format(quarter_string):
#     mobj = re.match("([\d]{2,4})[Qq]([\d]{1})", quarter_string)
#     if mobj is None:
#         raise ValueError("The string[%s] is NOT quarter format" % quarter_string)
#     quarter_string_len = len(quarter_string)
#     # if quarter_string_len < DEF_MIN_QUARTER_STRING_LENGTH or quarter_string_len > DEF_MAX_QUARTER_STRING_LENGTH:
#     if not (DEF_MIN_QUARTER_STRING_LENGTH <= quarter_string_len <= DEF_MAX_QUARTER_STRING_LENGTH):
#         raise ValueError("The quarter stirng[%s] length is NOT in the range [%d, %d]" % (quarter_string_len, DEF_MIN_QUARTER_STRING_LENGTH, DEF_MAX_QUARTER_STRING_LENGTH))
# # # Check Year Range
# #     check_year_range(mobj.group(1))
# # # Check Quarter Range
# #     check_quarter_range(mobj.group(2))
#     return mobj


# def transform_date_str(year_value, month_value, day_value):
#     return "%d-%02d-%02d" % (year_value, month_value, day_value)


# def transform_month_str(year_value, month_value):
#     return "%d-%02d" % (year_value, month_value)


# def transform_quarter_str(year_value, quarter_value):
#     return "%dq%d" % (year_value, quarter_value)

# # def transform_string2datetime(date_string, need_year_transform=False):
# #     element_arr = date_string.split('-')
# #     if len(element_arr) != 3:
# #         raise ValueError("Incorrect config date format: %s" % date_string)
# #     return datetime((int(element_arr[0]) if not need_year_transform else (int(element_arr[0]) + DEF_REPUBLIC_ERA_YEAR_OFFSET)), int(element_arr[1]), int(element_arr[2]))


# # def transform_datetime_cfg2string(datetime_cfg, need_year_transform=False):
# #     return transform_datetime2string(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day, need_year_transform)


# # def transform_datetime2string(year, month, day, need_year_transform=False):
# #     year_transform = (int(year) + 1911) if need_year_transform else int(year)
# #     return DATE_STRING_FORMAT % (year_transform, int(month), int(day))


# def get_last_url_data_date(today_data_exist_hour, today_data_exst_minute):
#     datetime_now = datetime.today()
#     datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
#     datetime_yesterday = datetime_today + timedelta(days = -1)
#     datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, today_data_exist_hour, today_data_exst_minute)
#     return datetime_today if datetime_now >= datetime_threshold else datetime_yesterday


# def get_config_filepath(conf_filename):
#     current_path = os.path.dirname(os.path.realpath(__file__))
#     [project_folder, lib_folder] = current_path.rsplit('/', 1)
#     conf_filepath = "%s/%s/%s" % (project_folder, DEF_CONF_FOLDER, conf_filename)
#     g_logger.debug("Parse the config file: %s" % conf_filepath)
#     return conf_filepath


# def get_finance_analysis_mode():
#     conf_filepath = get_config_filepath(DEF_MARKET_STOCK_SWITCH_CONF_FILENAME)
#     try:
#         with open(conf_filepath, 'r') as fp:
#             for line in fp:
#                 mode = int(line)
#                 if mode not in [FINANCE_ANALYSIS_MARKET, FINANCE_ANALYSIS_STOCK]:
#                     raise ValueError("Unknown finance analysis mode: %d" % mode)
#                 return mode
#     except Exception as e:
#         g_logger.error("Error occur while parsing config file[%s], due to %s" % (DEF_MARKET_STOCK_SWITCH_CONF_FILENAME, str(e)))
#         raise e


# def is_market_mode():
#     return get_finance_analysis_mode() == FINANCE_ANALYSIS_MARKET

# def parse_config_file(conf_filename):
#     # import pdb; pdb.set_trace()
#     conf_filepath = get_config_filepath(conf_filename)
#     total_param_list = []
#     try:
#         with open(conf_filepath, 'r') as fp:
#             for line in fp:
#                 # import pdb; pdb.set_trace()
#                 if line.startswith('#'):
#                     continue
#                 line_strip = line.strip('\n')
#                 if len(line_strip) == 0:
#                     continue
#                 param_list = line_strip.split(' ')
#                 param_list_len = len(param_list)
#                 data_source_index = DEF_DATA_SOURCE_INDEX_MAPPING.index(param_list[0].decode('utf-8'))
#                 datetime_range_start = None
#                 if param_list_len >= 2:
#                     datetime_range_start = transform_string2datetime(param_list[1])
#                 datetime_range_end = None
#                 if param_list_len >= 3:
#                     datetime_range_end = transform_string2datetime(param_list[2])
#                 total_param_list.append(
#                     {
#                         "index": data_source_index,
#                         "start": datetime_range_start,
#                         "end": datetime_range_end,
#                     }
#                 )
#     except Exception as e:
#         g_logger.error("Error occur while parsing config file[%s], due to %s" % (conf_filename, str(e)))
#         return None
#     return total_param_list


# def get_cfg_month_last_day(datetime_cfg):
#     return get_month_last_day(datetime_cfg.year, datetime_cfg.month)


# def get_month_last_day(year, month):
#     return calendar.monthrange(year, month)[1]


# # def get_year_offset_datetime_cfg(datetime_cfg, year_offset):
# #     return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


# # def get_datetime_range_by_month_list(datetime_range_start=None, datetime_range_end=None):
# # # Parse the current time
# #     if datetime_range_end is None:
# #         datetime_range_end = datetime.today()
# #     datetime_range_list = []
# #     datetime_cur = datetime_range_start
# #     # import pdb; pdb.set_trace()
# #     while True:
# #         last_day = get_cfg_month_last_day(datetime_cur)
# #         datetime_range_list.append(
# #             {
# #                 'start': datetime(datetime_cur.year, datetime_cur.month, 1),
# #                 'end': datetime(datetime_cur.year, datetime_cur.month, last_day),
# #             }
# #         )
# #         if datetime_range_end.year == datetime_cur.year and datetime_range_end.month == datetime_cur.month:
# #             break
# #         offset_day = 15 if datetime_cur.day > 20 else last_day
# #         datetime_cur +=  timedelta(days = offset_day)
# #     # import pdb; pdb.set_trace()
# #     if len(datetime_range_list) == 0:
# #         raise RuntimeError("The length of the datetime_range_list list should NOT be 0")
# #     if datetime_range_start is not None:
# #         datetime_range_list[0]['start'] = datetime_range_start
# #     if datetime_range_end is not None:
# #         datetime_range_list[-1]['end'] = datetime_range_end

# #     return datetime_range_list


# def get_cur_module_name(module):
#     return os.path.basename(os.path.realpath(module)).split('.')[0]


# def check_success(ret):
#     return True if ret == RET_SUCCESS else False


# def check_failure(ret):
#     return True if ret > RET_FAILURE_BASE else False


# def check_file_exist(filepath):
#     check_exist = True
#     try:
#         os.stat(filepath)
#     except OSError as exception:
#         if exception.errno != errno.ENOENT:
#             print "%s: %s" % (errno.errorcode[exception.errno], os.strerror(exception.errno))
#             raise
#         check_exist = True
#     return check_exist


# def create_folder_if_not_exist(filepath):
#     if not check_file_exist(filepath):
#         os.mkdir(filepath)


# def remove_comma_in_string(original_string):
#     return str(original_string).replace(',', '')


# def transform_share_number_string_to_board_lot(share_number_string):
#     element = remove_comma_in_string(share_number_string)
#     value = int(int(element) / 1000)
#     return value


# def to_str(unicode_or_str, encoding):
#     if isinstance(unicode_or_str, unicode):
#         value = unicode_or_str.encode(encoding)
#     else:
#         value = unicode_or_str
#     return value


# def to_unicode(unicode_or_str, encoding):
#     if isinstance(unicode_or_str, str):
#         value = unicode_or_str.decode(encoding)
#     else:
#         value = unicode_or_str
#     return value


# # def to_date_only_str(datetime_cfg):
# #     if not isinstance(datetime_cfg, datetime):
# #         raise ValueError("The type of datetime_cfg is NOT datetime")
# #     return (("%s" % datetime_cfg)).split(' ')[0]


# def is_the_same_year(datetime_cfg1, datetime_cfg2):
#     return (datetime_cfg1.year == datetime_cfg2.year)


# def is_the_same_month(datetime_cfg1, datetime_cfg2):
#     return (is_the_same_year(datetime_cfg1, datetime_cfg2) and datetime_cfg1.month == datetime_cfg2.month)


# def assemble_csv_year_time_str(timeslice_list):
#     if not isinstance(timeslice_list, list):
#         raise ValueError("timeslice_list is NOT a list")
#     timeslice_list_len = len(timeslice_list)
#     datetime_cfg_start = timeslice_list[0]
#     for index in range(1, timeslice_list_len):
#         if not is_the_same_year(datetime_cfg_start, timeslice_list[index]):
#             raise ValueError("The time[%s] is NOT in the year: %04d" % (to_date_only_str(timeslice_list[index]), datetime_cfg_start.year))
#     return "%04d" % datetime_cfg_start.year


# def assemble_csv_month_time_str(timeslice_list):
#     if not isinstance(timeslice_list, list):
#         raise ValueError("timeslice_list is NOT a list")
#     timeslice_list_len = len(timeslice_list)
#     datetime_cfg_start = timeslice_list[0]
#     for index in range(1, timeslice_list_len):
#         if not is_the_same_month(datetime_cfg_start, timeslice_list[index]):
#             raise ValueError("The time[%s] is NOT in the month: %04d-%02d" % (to_date_only_str(timeslice_list[index]), datetime_cfg_start.year, datetime_cfg_start.month))
#     return "%04d%02d" % (datetime_cfg_start.year, datetime_cfg_start.month)


# # DEF_DATA_SOURCE_START_DATE_CFG = [
# #     transform_string2datetime("2001-01-01"),
# #     transform_string2datetime("2004-04-07"),
# #     transform_string2datetime("2001-01-01"),
# #     get_year_offset_datetime_cfg(datetime.today(), -3),
# #     get_year_offset_datetime_cfg(datetime.today(), -3),
# #     get_year_offset_datetime_cfg(datetime.today(), -3),
# #     transform_string2datetime("2002-01-01"),
# #     transform_string2datetime("2004-07-01"),
# #     transform_string2datetime("2012-05-02"),
# #     transform_string2datetime("2012-05-02"),
# #     transform_string2datetime("2015-04-30"),
# #     # transform_string2datetime("2010-01-04"),
# #     # transform_string2datetime("2004-12-17"),
# #     # transform_string2datetime("2004-12-17"),
# #     # transform_string2datetime("2004-12-17"),
# # ]
