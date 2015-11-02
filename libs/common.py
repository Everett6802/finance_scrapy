# -*- coding: utf8 -*-

import os
import re
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


DEF_CONF_FOLDER = "config"
DEF_CSV_FILE_PATH = "/var/tmp/finance"
DEF_DATA_SOURCE_INDEX_MAPPING = [
    u'三大法人現貨買賣超',
    u'三大法人期貨留倉淨額',
    u'十大交易人及特定法人期貨資訊',
]
DEF_DATA_SOURCE_INDEX_MAPPING_LEN = len(DEF_DATA_SOURCE_INDEX_MAPPING)

DEF_WEB_SCRAPY_MODULE_NAME_PREFIX = "web_scrapy_"
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING = [
    "stock_top3_legal_persons_net_buy_or_sell",
    "future_top3_legal_persons_open_interest",
    "future_top10_dealers_and_legal_persons",
]
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN = len(DEF_WEB_SCRAPY_MODULE_NAME_MAPPING)

DEF_WEB_SCRAPY_CLASS_NAME_MAPPING = [
    "WebSracpyStockTop3LegalPersonsNetBuyOrSell",
    "WebSracpyFutureTop3LegalPersonsOpenInterest",
    "WebSracpyFutureTop10DealersAndLegalPersons",
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


def transform_string2datetime(date_string):
    element_arr = date_string.split('-')
    if len(element_arr) != 3:
        raise ValueError("Incorrect config date format: %s" % date_string)
    return datetime(int(element_arr[0]), int(element_arr[1]), int(element_arr[2]))


def parse_config_file(conf_filename):
    conf_filepath = "%s/%s/%s" % (os.getcwd(), DEF_CONF_FOLDER, conf_filename)
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


def get_datetime_range_by_month_list(datetime_range_start=None, datetime_range_end=None):
# Parse the current time
    if datetime_range_end is None:
        datetime_range_end = datetime.today()
    datetime_range_list = []
    datetime_cur = datetime_range_start
    # import pdb; pdb.set_trace()
    while True:
        last_day = calendar.monthrange(datetime_cur.year, datetime_cur.month)[1]
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


def check_success(ret):
    return True if ret == RET_SUCCESS else False


def check_failure(ret):
    return True if ret > RET_FAILURE_BASE else False