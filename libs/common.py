# -*- coding: utf8 -*-

import os
import re
import logging
import calendar
from datetime import datetime, timedelta
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


DEF_CONF_FOLDER = "config"
DEF_CSV_FILE_PATH = "/var/tmp"
DEF_FINANCE_DATA_INDEX_MAPPING = [
    u'十大交易人及特定法人期貨資訊',
    u'三大法人期貨留倉淨額',
    u'三大法人現貨買賣超',
]
DEF_WEB_SCRAPY_MODULE_NAME_MAPPING = [
    "web_scrapy_future_top10_dealers_and_legal_persons",
    "web_scrapy_stock_top3_legal_persons_net_buy_or_sell",
    "web_scrapy_future_top3_legal_persons_open_interest",
]

DEF_WEB_SCRAPY_CLASS_NAME_MAPPING = [
    "WebSracpyFutureTop10DealersAndLegalPersons",
    "WebSracpyFutureTop3LegalPersonsNetInterest",
    "WebSracpyFutureTop3LegalPersonsNetBuyOrSell",
]

def parse_config(conf_filename):
    conf_filepath = "%s/%s/%s" % (os.getcwd(), DEF_CONF_FOLDER, conf_filename)
    g_logger.debug("Parse the config file: %s" % conf_filepath)
    # import pdb; pdb.set_trace()
    total_param_list = []
    def transform_string2datetime(date_string):
        element_arr = date_string.split('-')
        if len(element_arr) != 3:
            raise ValueError("Incorrect config date format: %s" % date_string)
        return datetime(int(element_arr[0]), int(element_arr[1]), int(element_arr[2]))

    try:
        with open(conf_filepath, 'r') as fp:
            for line in fp:
                # import pdb; pdb.set_trace()
                line_strip = line.strip('\n')
                if len(line_strip) == 0:
                    continue
                param_list = line_strip.split(' ')
                param_list_len = len(param_list)
                finance_data_index = DEF_FINANCE_DATA_INDEX_MAPPING.index(param_list[0].decode('utf-8'))
                datetime_range_start = None
                if param_list_len >= 2:
                    datetime_range_start = transform_string2datetime(param_list[1])
                datetime_range_end = None
                if param_list_len >= 3:
                    datetime_range_end = transform_string2datetime(param_list[2])
                total_param_list.append(
                    {
                        "index": finance_data_index,
                        "start": datetime_range_start,
                        "end": datetime_range_end,
                    }
                )
    except Exception as e:
        g_logger.error("Error occur while parsing config file[%s], due to %s" % (conf_filename, str(e)))
        return None
    return total_param_list


def get_datetime_range_by_month_list(datetime_range_start, datetime_range_end=None):
# Parse the current time
    if datetime_range_end is None:
        datetime_range_end = datetime.today()
    datetime_range_list = []
    datetime_cur = datetime_range_start
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
    if len(datetime_range_list) == 0:
        raise RuntimeError("The length of the datetime_range_list list should NOT be 0")
    datetime_range_list[0]['start'].day = datetime_range_start.day
    datetime_range_list[-1]['end'].day = datetime_range_end.day

    return datetime_range_list
