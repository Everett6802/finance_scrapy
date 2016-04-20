# -*- coding: utf8 -*-

import os
import re
import errno
import logging
import calendar
from datetime import datetime, timedelta
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


RET_SUCCESS = 0

RET_WARN_BASE = 100
RET_WARN_URL_NOT_EXIST = RET_WARN_BASE + 1

RET_FAILURE_BASE = 100
RET_FAILURE_UNKNOWN = RET_FAILURE_BASE + 1
RET_FAILURE_TIMEOUT = RET_FAILURE_BASE + 2

RUN_RESULT_FILENAME = "run_result"
TIME_FILENAME_FORMAT = "%04d%02d%02d%02d%02d"
DATE_STRING_FORMAT = "%04d-%02d-%02d"
SNAPSHOT_FILENAME_FORMAT = "snapshot_result%s.tar.gz" % TIME_FILENAME_FORMAT

WRITE2CSV_ONE_MONTH_PER_FILE = 0
WRITE2CSV_ONE_DAY_PER_FILE = 1

PARSE_URL_DATA_BY_BS4 = 0
PARSE_URL_DATA_BY_JSON = 1

DEF_WEB_SCRAPY_BEGIN_DATE_STR = "2000-01-01"
DEF_WORKDAY_CANLENDAR_CONF_FILENAME = ".workday_canlendar.conf"
DEF_WORKDAY_CANLENDAR_CONF_FILE_DST_PROJECT_NAME1 = "finance_recorder_java"
DEF_WORKDAY_CANLENDAR_CONF_FILE_DST_PROJECT_NAME2 = "finance_analyzer"
DEF_TODAY_DATA_EXIST_HOUR = 20
DEF_TODAY_DATA_EXIST_MINUTE = 0
DEF_CONF_FOLDER = "conf"
DEF_CSV_FILE_PATH = "/var/tmp/finance"
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

DEF_DATA_SOURCE_WRITE2CSV_METHOD = [
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    WRITE2CSV_ONE_DAY_PER_FILE,
    WRITE2CSV_ONE_DAY_PER_FILE,
    WRITE2CSV_ONE_MONTH_PER_FILE,
    # WRITE2CSV_ONE_DAY_PER_FILE,
    # WRITE2CSV_ONE_DAY_PER_FILE,
    # WRITE2CSV_ONE_DAY_PER_FILE,
    # WRITE2CSV_ONE_DAY_PER_FILE,
]

DEF_WEB_SCRAPY_DATA_SOURCE_TYPE = [
    "TODAY",
    "HISTORY",
    "USER_DEFINED",
]
DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX = DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index("TODAY")
DEF_WEB_SCRAPY_DATA_SOURCE_HISTORY_INDEX = DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index("HISTORY")
DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX = DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index("USER_DEFINED")
DEF_WEB_SCRAPY_DATA_SOURCE_TYPE_LEN = len(DEF_WEB_SCRAPY_DATA_SOURCE_TYPE)

DEF_TODAY_CONFIG_FILENAME = "today.conf"
DEF_HISTORY_CONFIG_FILENAME = "history.conf"


def transform_string2datetime(date_string, need_year_transform=False):
    element_arr = date_string.split('-')
    if len(element_arr) != 3:
        raise ValueError("Incorrect config date format: %s" % date_string)
    return datetime((int(element_arr[0]) if not need_year_transform else (int(element_arr[0]) + 1911)), int(element_arr[1]), int(element_arr[2]))


def transform_datetime_cfg2string(datetime_cfg, need_year_transform=False):
    return transform_datetime2string(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day, need_year_transform)


def transform_datetime2string(year, month, day, need_year_transform=False):
    year_transform = (int(year) + 1911) if need_year_transform else int(year)
    return DATE_STRING_FORMAT % (year_transform, int(month), int(day))


def parse_config_file(conf_filename):
    current_path = os.path.dirname(os.path.realpath(__file__))
    [project_folder, lib_folder] = current_path.rsplit('/', 1)
    conf_filepath = "%s/%s/%s" % (project_folder, DEF_CONF_FOLDER, conf_filename)
    g_logger.debug("Parse the config file: %s" % conf_filepath)
    # import pdb; pdb.set_trace()
    total_param_list = []

    try:
        with open(conf_filepath, 'r') as fp:
            for line in fp:
                # import pdb; pdb.set_trace()
                if line.startswith('#'):
                    continue
                line_strip = line.strip('\n')
                if len(line_strip) == 0:
                    continue
                param_list = line_strip.split(' ')
                param_list_len = len(param_list)
                data_source_index = DEF_DATA_SOURCE_INDEX_MAPPING.index(param_list[0].decode('utf-8'))
                datetime_range_start = None
                if param_list_len >= 2:
                    datetime_range_start = transform_string2datetime(param_list[1])
                datetime_range_end = None
                if param_list_len >= 3:
                    datetime_range_end = transform_string2datetime(param_list[2])
                total_param_list.append(
                    {
                        "index": data_source_index,
                        "start": datetime_range_start,
                        "end": datetime_range_end,
                    }
                )
    except Exception as e:
        g_logger.error("Error occur while parsing config file[%s], due to %s" % (conf_filename, str(e)))
        return None
    return total_param_list


def get_cfg_month_last_day(datetime_cfg):
    return get_month_last_day(datetime_cfg.year, datetime_cfg.month)


def get_month_last_day(year, month):
    return calendar.monthrange(year, month)[1]


def get_year_offset_datetime_cfg(datetime_cfg, year_offset):
    return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


def get_datetime_range_by_month_list(datetime_range_start=None, datetime_range_end=None):
# Parse the current time
    if datetime_range_end is None:
        datetime_range_end = datetime.today()
    datetime_range_list = []
    datetime_cur = datetime_range_start
    # import pdb; pdb.set_trace()
    while True:
        last_day = get_cfg_month_last_day(datetime_cur)
        datetime_range_list.append(
            {
                'start': datetime(datetime_cur.year, datetime_cur.month, 1),
                'end': datetime(datetime_cur.year, datetime_cur.month, last_day),
            }
        )
        if datetime_range_end.year == datetime_cur.year and datetime_range_end.month == datetime_cur.month:
            break
        offset_day = 15 if datetime_cur.day > 20 else last_day
        datetime_cur +=  timedelta(days = offset_day)
    # import pdb; pdb.set_trace()
    if len(datetime_range_list) == 0:
        raise RuntimeError("The length of the datetime_range_list list should NOT be 0")
    if datetime_range_start is not None:
        datetime_range_list[0]['start'] = datetime_range_start
    if datetime_range_end is not None:
        datetime_range_list[-1]['end'] = datetime_range_end

    return datetime_range_list


def get_cur_module_name(module):
    return os.path.basename(os.path.realpath(module)).split('.')[0]


def check_success(ret):
    return True if ret == RET_SUCCESS else False


def check_failure(ret):
    return True if ret > RET_FAILURE_BASE else False


def create_folder_if_not_exist(filepath):
    try:
        os.stat(filepath)
    except OSError as exception:
        if exception.errno == errno.ENOENT:
            os.mkdir(filepath)
        else:
            print "%s: %s" % (errno.errorcode[exception.errno], os.strerror(exception.errno))
            raise

def remove_comma_in_string(original_string):
    return str(original_string).replace(',', '')


def transform_share_number_string_to_board_lot(share_number_string):
    element = remove_comma_in_string(share_number_string)
    value = int(int(element) / 1000)
    return value


DEF_DATA_SOURCE_START_DATE_CFG = [
    transform_string2datetime("2001-01-01"),
    transform_string2datetime("2004-04-07"),
    transform_string2datetime("2001-01-01"),
    get_year_offset_datetime_cfg(datetime.today(), -3),
    get_year_offset_datetime_cfg(datetime.today(), -3),
    get_year_offset_datetime_cfg(datetime.today(), -3),
    transform_string2datetime("2002-01-01"),
    transform_string2datetime("2004-07-01"),
    transform_string2datetime("2012-05-02"),
    transform_string2datetime("2012-05-02"),
    transform_string2datetime("2015-04-30"),
    # transform_string2datetime("2010-01-04"),
    # transform_string2datetime("2004-12-17"),
    # transform_string2datetime("2004-12-17"),
    # transform_string2datetime("2004-12-17"),
]
