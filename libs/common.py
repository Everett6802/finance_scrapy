# -*- coding: utf8 -*-

import os
import re
import logging
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

def parse_config(conf_filename):
    conf_filepath = "%s/%s/%s" % (os.getcwd(), DEF_CONF_FOLDER, conf_filename)
    g_logger.debug("Parse the config file: %s" % conf_filepath)
    # import pdb; pdb.set_trace()
    total_param_list = []
    def transform_string2datetime(date_string):
    	element_arr = date_string.split('-')
    	if len(element_arr) != 3:
    		raise ValueError("Incorrect config date format: %s" % date_string)
    	return datetime(element_arr[0], element_arr[1], element_arr[2])
    try:
        with open(conf_filepath, 'r') as fp:
            for line in fp:
                param_list = line.strip('\n').split(' ')
                param_list_len = len(param_list)
                if param_list_len == 0:
                	continue
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

# def get_web_data(self, url, encoding, select_flag):
#     web_data = None
#     try:
#         res = requests.get(url)
#         res.encoding = encoding
#         #print res.text
#         soup = BeautifulSoup(res.text)
#         web_data = soup.select(select_flag)
#     except Exception as e:
#         print "Fail to scrapy URL[%s], due to: %s" % (url, str(e))

#     return web_data


# def get_time_range_list(start_year, start_month):
# # Parse the current time
#     today = datetime.today()
#     end_year = today.year
#     end_month = today.month
# # Generate the time range list
#     time_range_list = []
#     cur_year = start_year
#     cur_month = start_month
#     while True:
#         time_range_list.append({"year": cur_year, "month": cur_month})
#         cur_month += 1
#         if cur_month > 12:
#             cur_year += 1
#             cur_month = 1
#         if cur_year == end_year and cur_month == end_month:
#             break

# 	return time_range_list
