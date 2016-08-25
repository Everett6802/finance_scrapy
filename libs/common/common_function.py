# -*- coding: utf8 -*-

import os
import re
import errno
import logging
import calendar
from datetime import datetime, timedelta
import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()
import common_definition as CMN_DEF
import common_class as CMN_CLS


########################################################################################

def is_republic_era_year(year_value):
    if isinstance(year_value, int):
        return True if (year_value / 1000 == 0) else False
    elif isinstance(year_value, str):
        return True if len(year_value) != 4 else False
    raise ValueError("Unknown year value: %s !!!" % str(year_value))


def check_year_range(year_value):
    if is_republic_era_year(year_value):
        if not (CMN_DEF.DEF_REPUBLIC_ERA_START_YEAR <= int(year_value) <= CMN_DEF.DEF_REPUBLIC_ERA_END_YEAR):
            raise ValueError("The republic era year[%d] is NOT in the range [%d, %d]" % (int(year_value), CMN_DEF.DEF_REPUBLIC_ERA_START_YEAR, CMN_DEF.DEF_REPUBLIC_ERA_END_YEAR))
    else:
        if not (CMN_DEF.DEF_START_YEAR <= int(year_value) <= CMN_DEF.DEF_END_YEAR):
            raise ValueError("The year[%d] is NOT in the range [%d, %d]" % (int(year_value), CMN_DEF.DEF_START_YEAR, CMN_DEF.DEF_END_YEAR))


def check_quarter_range(quarter_value):
    if not (CMN_DEF.DEF_START_QUARTER <= int(quarter_value) <= CMN_DEF.DEF_END_QUARTER):
        raise ValueError("The quarter[%d] is NOT in the range [%d, %d]" % (int(quarter_value), CMN_DEF.DEF_START_QUARTER, CMN_DEF.DEF_END_QUARTER))


def check_month_range(month_value):
    if not (CMN_DEF.DEF_START_MONTH <= int(month_value) <= CMN_DEF.DEF_END_MONTH):
        raise ValueError("The month[%d] is NOT in the range [%d, %d]" % (int(month_value), CMN_DEF.DEF_START_MONTH, CMN_DEF.DEF_END_MONTH))


def check_day_range(day_value, year_value, month_value):
    end_day_in_month = get_month_last_day(int(year_value), int(month_value))
    if not (CMN_DEF.DEF_START_DAY <= int(day_value) <= end_day_in_month):
        raise ValueError("The day[%d] is NOT in the range [%d, %d]" % (int(day_value), CMN_DEF.DEF_START_DAY, end_day_in_month))


def check_date_str_format(date_string):
    mobj = re.match("([\d]{2,4})-([\d]{2})-([\d]{2})", date_string)
    if mobj is None:
        raise ValueError("The string[%s] is NOT date format" % date_string)
    date_string_len = len(date_string)
    # if date_string_len < DEF_MIN_DATE_STRING_LENGTH or date_string_len > DEF_MAX_DATE_STRING_LENGTH:
    if not (CMN_DEF.DEF_MIN_DATE_STRING_LENGTH <= date_string_len <= CMN_DEF.DEF_MAX_DATE_STRING_LENGTH):
        raise ValueError("The date stirng[%s] length is NOT in the range [%d, %d]" % (date_string_len, CMN_DEF.DEF_MIN_DATE_STRING_LENGTH, CMN_DEF.DEF_MAX_DATE_STRING_LENGTH))
# # Check Year Range
#     check_year_range(mobj.group(1))
# # Check Month Range
#     check_month_range(mobj.group(2))
# # Check Day Range
#     check_day_range(mobj.group(3), mobj.group(1), mobj.group(2))
    return mobj


def check_month_str_format(month_string):
    mobj = re.match("([\d]{2,4})-([\d]{2})", month_string)
    if mobj is None:
        raise ValueError("The string[%s] is NOT month format" % month_string)
    month_string_len = len(month_string)
    # if month_string_len < DEF_MIN_MONTH_STRING_LENGTH or month_string_len > DEF_MAX_MONTH_STRING_LENGTH:
    if not (CMN_DEF.DEF_MIN_MONTH_STRING_LENGTH <= month_string_len <= CMN_DEF.DEF_MAX_MONTH_STRING_LENGTH):
        raise ValueError("The month stirng[%s] length is NOT in the range [%d, %d]" % (month_string_len, CMN_DEF.DEF_MIN_MONTH_STRING_LENGTH, CMN_DEF.DEF_MAX_MONTH_STRING_LENGTH))
# # Check Year Range
#     check_year_range(mobj.group(1))
# # Check Month Range
#     check_month_range(mobj.group(2))
    return mobj


def check_quarter_str_format(quarter_string):
    mobj = re.match("([\d]{2,4})[Qq]([\d]{1})", quarter_string)
    if mobj is None:
        raise ValueError("The string[%s] is NOT quarter format" % quarter_string)
    quarter_string_len = len(quarter_string)
    # if quarter_string_len < DEF_MIN_QUARTER_STRING_LENGTH or quarter_string_len > DEF_MAX_QUARTER_STRING_LENGTH:
    if not (CMN_DEF.DEF_MIN_QUARTER_STRING_LENGTH <= quarter_string_len <= CMN_DEF.DEF_MAX_QUARTER_STRING_LENGTH):
        raise ValueError("The quarter stirng[%s] length is NOT in the range [%d, %d]" % (quarter_string_len, CMN_DEF.DEF_MIN_QUARTER_STRING_LENGTH, CMN_DEF.DEF_MAX_QUARTER_STRING_LENGTH))
# # Check Year Range
#     check_year_range(mobj.group(1))
# # Check Quarter Range
#     check_quarter_range(mobj.group(2))
    return mobj


def transform_date_str(year_value, month_value, day_value):
    return "%d-%02d-%02d" % (year_value, month_value, day_value)


def transform_month_str(year_value, month_value):
    return "%d-%02d" % (year_value, month_value)


def transform_quarter_str(year_value, quarter_value):
    return "%dq%d" % (year_value, quarter_value)

# def transform_string2datetime(date_string, need_year_transform=False):
#     element_arr = date_string.split('-')
#     if len(element_arr) != 3:
#         raise ValueError("Incorrect config date format: %s" % date_string)
#     return datetime((int(element_arr[0]) if not need_year_transform else (int(element_arr[0]) + CMN_DEF.DEF_REPUBLIC_ERA_YEAR_OFFSET)), int(element_arr[1]), int(element_arr[2]))


# def transform_datetime_cfg2string(datetime_cfg, need_year_transform=False):
#     return transform_datetime2string(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day, need_year_transform)


# def transform_datetime2string(year, month, day, need_year_transform=False):
#     year_transform = (int(year) + CMN_DEF.DEF_REPUBLIC_ERA_YEAR_OFFSET) if need_year_transform else int(year)
#     return DATE_STRING_FORMAT % (year_transform, int(month), int(day))


def get_last_url_data_date(today_data_exist_hour, today_data_exst_minute):
    datetime_now = datetime.today()
    datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
    datetime_yesterday = datetime_today + timedelta(days = -1)
    datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, today_data_exist_hour, today_data_exst_minute)
    return CMN_CLS.FinanceDate(datetime_today) if datetime_now >= datetime_threshold else CMN_CLS.FinanceDate(datetime_yesterday)


def get_project_folderpath():
    RELATIVE_COMMON_FOLDERPATH = ""
    current_path = os.path.dirname(os.path.realpath(__file__))
    # print current_path
    [project_folder, lib_folder, common_folder] = current_path.rsplit('/', 2)
    assert (lib_folder == "libs"), "The lib folder name[%s] is NOT as expected: libs" %  lib_folder
    assert (common_folder == "common"), "The common folder name[%s] is NOT as expected: common" % common_folder
    return project_folder


def get_config_filepath(conf_filename):
    # current_path = os.path.dirname(os.path.realpath(__file__))
    # [project_folder, lib_folder] = current_path.rsplit('/', 1)
    conf_filepath = "%s/%s/%s" % (CMN_DEF.DEF_PROJECT_FOLDERPATH, CMN_DEF.DEF_CONF_FOLDER, conf_filename)
    g_logger.debug("Parse the config file: %s" % conf_filepath)
    return conf_filepath


def get_finance_analysis_mode():
    conf_filepath = get_config_filepath(CMN_DEF.DEF_MARKET_STOCK_SWITCH_CONF_FILENAME)
    try:
        with open(conf_filepath, 'r') as fp:
            for line in fp:
                mode = int(line)
                if mode not in [CMN_DEF.FINANCE_ANALYSIS_MARKET, CMN_DEF.FINANCE_ANALYSIS_STOCK]:
                    raise ValueError("Unknown finance analysis mode: %d" % mode)
                return mode
    except Exception as e:
        g_logger.error("Error occur while parsing config file[%s], due to %s" % (CMN_DEF.DEF_MARKET_STOCK_SWITCH_CONF_FILENAME, str(e)))
        raise e


def is_market_mode():
    return get_finance_analysis_mode() == CMN_DEF.FINANCE_ANALYSIS_MARKET
def is_stock_mode():
    return get_finance_analysis_mode() == CMN_DEF.FINANCE_ANALYSIS_STOCK


def parse_source_type_time_range_config_file(conf_filename):
    # import pdb; pdb.set_trace()
    conf_filepath = get_config_filepath(conf_filename)
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
                source_type_index = CMN_DEF.DEF_DATA_SOURCE_INDEX_MAPPING.index(param_list[0].decode('utf-8'))
                datetime_range_start = None
                if param_list_len >= 2:
                    datetime_range_start = transform_string2datetime(param_list[1])
                datetime_range_end = None
                if param_list_len >= 3:
                    datetime_range_end = transform_string2datetime(param_list[2])
                total_param_list.append(
                    # {
                    #     "index": source_type_index,
                    #     "start": datetime_range_start,
                    #     "end": datetime_range_end,
                    # }
                    CMN_CLS.SourceTypeTimeRangeTuple(source_type_index, datetime_range_start, datetime_range_end)
                )
    except Exception as e:
        g_logger.error("Error occur while parsing config file[%s], due to %s" % (conf_filename, str(e)))
        return None
    return total_param_list


def get_cfg_month_last_day(datetime_cfg):
    return get_month_last_day(datetime_cfg.year, datetime_cfg.month)


def get_month_last_day(year, month):
    return calendar.monthrange(year, month)[1]


# def get_year_offset_datetime_cfg(datetime_cfg, year_offset):
#     return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


# def get_datetime_range_by_month_list(datetime_range_start=None, datetime_range_end=None):
# # Parse the current time
#     if datetime_range_end is None:
#         datetime_range_end = datetime.today()
#     datetime_range_list = []
#     datetime_cur = datetime_range_start
#     # import pdb; pdb.set_trace()
#     while True:
#         last_day = get_cfg_month_last_day(datetime_cur)
#         datetime_range_list.append(
#             {
#                 'start': datetime(datetime_cur.year, datetime_cur.month, 1),
#                 'end': datetime(datetime_cur.year, datetime_cur.month, last_day),
#             }
#         )
#         if datetime_range_end.year == datetime_cur.year and datetime_range_end.month == datetime_cur.month:
#             break
#         offset_day = 15 if datetime_cur.day > 20 else last_day
#         datetime_cur +=  timedelta(days = offset_day)
#     # import pdb; pdb.set_trace()
#     if len(datetime_range_list) == 0:
#         raise RuntimeError("The length of the datetime_range_list list should NOT be 0")
#     if datetime_range_start is not None:
#         datetime_range_list[0]['start'] = datetime_range_start
#     if datetime_range_end is not None:
#         datetime_range_list[-1]['end'] = datetime_range_end

#     return datetime_range_list


def get_cur_module_name(module):
    return os.path.basename(os.path.realpath(module)).split('.')[0]


def check_success(ret):
    return True if ret == CMN_DEF.RET_SUCCESS else False


def check_failure(ret):
    return True if ret > CMN_DEF.RET_FAILURE_BASE else False


def check_file_exist(filepath):
    check_exist = True
    try:
        os.stat(filepath)
    except OSError as exception:
        if exception.errno != errno.ENOENT:
            print "%s: %s" % (errno.errorcode[exception.errno], os.strerror(exception.errno))
            raise
        check_exist = False
    return check_exist


def create_folder_if_not_exist(filepath):
    if not check_file_exist(filepath):
        os.mkdir(filepath)


def remove_comma_in_string(original_string):
    return str(original_string).replace(',', '')


def transform_share_number_string_to_board_lot(share_number_string):
    element = remove_comma_in_string(share_number_string)
    value = int(int(element) / 1000)
    return value


def to_str(unicode_or_str, encoding):
    if isinstance(unicode_or_str, unicode):
        value = unicode_or_str.encode(encoding)
    else:
        value = unicode_or_str
    return value


def to_unicode(unicode_or_str, encoding):
    if isinstance(unicode_or_str, str):
        value = unicode_or_str.decode(encoding)
    else:
        value = unicode_or_str
    return value


# def to_date_only_str(datetime_cfg):
#     if not isinstance(datetime_cfg, datetime):
#         raise ValueError("The type of datetime_cfg is NOT datetime")
#     return (("%s" % datetime_cfg)).split(' ')[0]


def is_the_same_year(datetime_cfg1, datetime_cfg2):
    return (datetime_cfg1.year == datetime_cfg2.year)


def is_the_same_month(datetime_cfg1, datetime_cfg2):
    return (is_the_same_year(datetime_cfg1, datetime_cfg2) and datetime_cfg1.month == datetime_cfg2.month)


def assemble_csv_year_time_str(timeslice_list):
    if not isinstance(timeslice_list, list):
        raise ValueError("timeslice_list is NOT a list")
    timeslice_list_len = len(timeslice_list)
    datetime_cfg_start = timeslice_list[0]
    for index in range(1, timeslice_list_len):
        if not is_the_same_year(datetime_cfg_start, timeslice_list[index]):
            raise ValueError("The time[%s] is NOT in the year: %04d" % (to_date_only_str(timeslice_list[index]), datetime_cfg_start.year))
    return "%04d" % datetime_cfg_start.year


def assemble_csv_month_time_str(timeslice_list):
    if not isinstance(timeslice_list, list):
        raise ValueError("timeslice_list is NOT a list")
    timeslice_list_len = len(timeslice_list)
    datetime_cfg_start = timeslice_list[0]
    for index in range(1, timeslice_list_len):
        if not is_the_same_month(datetime_cfg_start, timeslice_list[index]):
            raise ValueError("The time[%s] is NOT in the month: %04d-%02d" % (to_date_only_str(timeslice_list[index]), datetime_cfg_start.year, datetime_cfg_start.month))
    return "%04d%02d" % (datetime_cfg_start.year, datetime_cfg_start.month)


# DEF_DATA_SOURCE_START_DATE_CFG = [
#     transform_string2datetime("2001-01-01"),
#     transform_string2datetime("2004-04-07"),
#     transform_string2datetime("2001-01-01"),
#     get_year_offset_datetime_cfg(datetime.today(), -3),
#     get_year_offset_datetime_cfg(datetime.today(), -3),
#     get_year_offset_datetime_cfg(datetime.today(), -3),
#     transform_string2datetime("2002-01-01"),
#     transform_string2datetime("2004-07-01"),
#     transform_string2datetime("2012-05-02"),
#     transform_string2datetime("2012-05-02"),
#     transform_string2datetime("2015-04-30"),
#     # transform_string2datetime("2010-01-04"),
#     # transform_string2datetime("2004-12-17"),
#     # transform_string2datetime("2004-12-17"),
#     # transform_string2datetime("2004-12-17"),
# ]
