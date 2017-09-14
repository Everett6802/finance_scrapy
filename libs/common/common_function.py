# -*- coding: utf8 -*-

import os
import re
import sys
import errno
import logging
import calendar
import requests
import shutil
import inspect
from datetime import datetime, timedelta
import common_definition as CMN_DEF
import common_class as CMN_CLS
import common_exception as CMN_EXCEPTION
import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


########################################################################################

def try_print(msg):
    if CMN_DEF.CAN_PRINT_CONSOLE: 
        print msg


def get_full_stack_traceback():
    tb = sys.exc_info()[2]
    errmsg = 'Traceback (most recent call last):'
    for item in reversed(inspect.getouterframes(tb.tb_frame)[1:]):
        errmsg += ' File "{1}", line {2}, in {3}\n'.format(*item)
        for line in item[4]:
            errmsg += ' ' + line.lstrip()
        for item in inspect.getinnerframes(tb):
            errmsg += ' File "{1}", line {2}, in {3}\n'.format(*item)
        for line in item[4]:
            errmsg += ' ' + line.lstrip()
    return errmsg


def import_web_scrapy_module(module_folder, module_name):
    # import pdb; pdb.set_trace()
    module_path = "%s/%s" % (CMN_DEF.PROJECT_LIB_FOLDERPATH, module_folder)
    sys.path.insert(0, module_path)
    module_file = '%s/%s.py' % (module_path, module_name)
    assert os.path.exists(module_file), "module file does not exist: %s" % module_file
    try:
        module = __import__(module_name)
        if module:
            # print 'Import file: %s.py (%s)' % (module_name, module)
            return reload(module)
    except CMN_EXCEPTION.WebScrapyException as e:
        msg = 'Import template file failure: %s.py, due to: %s' % (module_name, str(e))
        try_print(msg)
        raise e
    except Exception as e:
        msg = 'Import template file failure: %s.py, due to: %s' % (module_name, str(e))
        try_print(msg)
        raise e


def get_web_scrapy_class_for_name(module_folder, module_name, class_name):
    # import pdb; pdb.set_trace()
    m = import_web_scrapy_module(module_folder, module_name)
    parts = module_name.split('.')
    parts.append(class_name)
    for comp in parts[1:]:
        m = getattr(m, comp)            
    return m


def get_web_scrapy_class(source_type_index, init_class_variables=True):
    # import pdb; pdb.set_trace()
    module_folder = CMN_DEF.SCRAPY_MODULE_FOLDER_MAPPING[source_type_index]
    module_name = CMN_DEF.SCRAPY_MODULE_NAME_PREFIX + CMN_DEF.SCRAPY_MODULE_NAME_MAPPING[source_type_index]
    class_name = CMN_DEF.SCRAPY_CLASS_NAME_MAPPING[source_type_index]
    g_logger.debug("Try to instantiate %s.%s" % (module_name, class_name))
# Find the module
    web_scrapy_class = get_web_scrapy_class_for_name(module_folder, module_name, class_name)
    if init_class_variables:
        web_scrapy_class.init_class_common_variables() # Caution: Must be called in the leaf derived class
        web_scrapy_class.init_class_customized_variables() # Caution: Must be called in the leaf derived class         
    return web_scrapy_class


def get_web_scrapy_object(web_scrapy_class, **kwargs):
# Instantiate the class 
    web_scrapy_obj = web_scrapy_class(**kwargs)
    return web_scrapy_obj


def instantiate_web_scrapy_object(source_type_index, **kwargs):
    # import pdb; pdb.set_trace()
# Get the class
    web_scrapy_class = get_web_scrapy_class(source_type_index)
# Instantiate the class 
    web_scrapy_obj = get_web_scrapy_object(web_scrapy_class, **kwargs)
    return web_scrapy_obj


def check_source_type_index_in_range(source_type_index):
    if CMN_DEF.IS_FINANCE_MARKET_MODE:
        return True if CMN_DEF.DATA_SOURCE_MARKET_START <= source_type_index < CMN_DEF.DATA_SOURCE_MARKET_END else False
    elif CMN_DEF.IS_FINANCE_STOCK_MODE:
        return True if CMN_DEF.DATA_SOURCE_STOCK_START <= source_type_index < CMN_DEF.DATA_SOURCE_STOCK_END else False
    raise RuntimeError("Unknown finance mode")


def check_statement_source_type_index_in_range(source_type_index):
    if CMN_DEF.IS_FINANCE_MARKET_MODE:
        return False
    elif CMN_DEF.IS_FINANCE_STOCK_MODE:
        return True if CMN_DEF.DATA_SOURCE_STOCK_STATMENT_START <= source_type_index < CMN_DEF.DATA_SOURCE_STOCK_STATMENT_END else False
    raise RuntimeError("Unknown finance mode")


def get_source_type_index_range():
    if CMN_DEF.IS_FINANCE_MARKET_MODE:
        return (CMN_DEF.DATA_SOURCE_MARKET_START, CMN_DEF.DATA_SOURCE_MARKET_END)
    elif CMN_DEF.IS_FINANCE_STOCK_MODE:
        return (CMN_DEF.DATA_SOURCE_STOCK_START, CMN_DEF.DATA_SOURCE_STOCK_END)
    raise RuntimeError("Unknown finance mode")


def get_method_index_range():
    if CMN_DEF.IS_FINANCE_MARKET_MODE:
        return (CMN_DEF.SCRAPY_MARKET_METHOD_START, CMN_DEF.SCRAPY_MARKET_METHOD_END)
    elif CMN_DEF.IS_FINANCE_STOCK_MODE:
        return (CMN_DEF.SCRAPY_STOCK_METHOD_START, CMN_DEF.SCRAPY_STOCK_METHOD_END)
    raise RuntimeError("Unknown finance mode")


def get_source_type_size():
    if CMN_DEF.IS_FINANCE_MARKET_MODE:
        return CMN_DEF.DATA_SOURCE_MARKET_SIZE
    elif CMN_DEF.IS_FINANCE_STOCK_MODE:
        return CMN_DEF.DATA_SOURCE_STOCK_SIZE
    raise RuntimeError("Unknown finance mode")


def get_source_type_index_from_description(source_type_description, ignore_exception=False):
    source_type_index = -1
    try:
        source_type_index = CMN_DEF.DATA_SOURCE_INDEX_MAPPING.index(source_type_description)
    except ValueError as e:
        if not ignore_exception:
            raise e
        g_logger.warn("Unknown source type description: %s", source_type_description);
    return source_type_index


def get_source_type_index_list_from_method_description(method_description):
    source_type_index_list = []
    for source_type_index, class_constant_cfg in enumerate(CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG):
        if re.search(method_description, class_constant_cfg["description"], re.U):
            source_type_index_list.append(source_type_index)
    if len(source_type_index_list) == 0:
        raise ValueError("Unknown method description: %s" % method_description)
    return source_type_index_list


def get_source_type_index_list_from_method_index(method_index):
    method_description = CMN_DEF.SCRAPY_METHOD_DESCRIPTION[method_index]
    return get_source_type_index_list_from_method_description(method_description)


def get_source_type_index_range_list():
    source_type_index_list = []
    (source_type_start_index, source_type_end_index) = get_source_type_index_range()
# Semi-open interval
    for index in range(source_type_start_index, source_type_end_index):
        source_type_index_list.append(index)
    return source_type_index_list


def get_method_index_range_list():
    method_index_list = []
    (method_start_index, method_end_index) = get_method_index_range()
# Semi-open interval
    for index in range(method_start_index, method_end_index):
        method_index_list.append(index)
    return method_index_list


def is_republic_era_year(year_value):
    if isinstance(year_value, int):
        return True if (year_value / 1000 == 0) else False
    elif isinstance(year_value, str):
        return True if len(year_value) != 4 else False
    raise ValueError("Unknown year value: %s !!!" % str(year_value))


def check_year_range(year_value):
    if is_republic_era_year(year_value):
        if not (CMN_DEF.REPUBLIC_ERA_START_YEAR <= int(year_value) <= CMN_DEF.REPUBLIC_ERA_END_YEAR):
            raise ValueError("The republic era year[%d] is NOT in the range [%d, %d]" % (int(year_value), CMN_DEF.REPUBLIC_ERA_START_YEAR, CMN_DEF.REPUBLIC_ERA_END_YEAR))
    else:
        if not (CMN_DEF.START_YEAR <= int(year_value) <= CMN_DEF.END_YEAR):
            raise ValueError("The year[%d] is NOT in the range [%d, %d]" % (int(year_value), CMN_DEF.START_YEAR, CMN_DEF.END_YEAR))


def check_quarter_range(quarter_value):
    if not (CMN_DEF.START_QUARTER <= int(quarter_value) <= CMN_DEF.END_QUARTER):
        raise ValueError("The quarter[%d] is NOT in the range [%d, %d]" % (int(quarter_value), CMN_DEF.START_QUARTER, CMN_DEF.END_QUARTER))


def check_month_range(month_value):
    if not (CMN_DEF.START_MONTH <= int(month_value) <= CMN_DEF.END_MONTH):
        raise ValueError("The month[%d] is NOT in the range [%d, %d]" % (int(month_value), CMN_DEF.START_MONTH, CMN_DEF.END_MONTH))


def check_day_range(day_value, year_value, month_value):
    end_day_in_month = get_month_last_day(int(year_value), int(month_value))
    if not (CMN_DEF.START_DAY <= int(day_value) <= end_day_in_month):
        raise ValueError("The day[%d] is NOT in the range [%d, %d]" % (int(day_value), CMN_DEF.START_DAY, end_day_in_month))


def check_date_str_format(date_string):
    mobj = re.match("([\d]{2,4})-([\d]{2})-([\d]{2})", date_string)
    if mobj is None:
        raise ValueError("The string[%s] is NOT date format" % date_string)
    date_string_len = len(date_string)
    # if date_string_len < MIN_DATE_STRING_LENGTH or date_string_len > MAX_DATE_STRING_LENGTH:
    if not (CMN_DEF.MIN_DATE_STRING_LENGTH <= date_string_len <= CMN_DEF.MAX_DATE_STRING_LENGTH):
        raise ValueError("The date stirng[%s] length is NOT in the range [%d, %d]" % (date_string_len, CMN_DEF.MIN_DATE_STRING_LENGTH, CMN_DEF.MAX_DATE_STRING_LENGTH))
# # Check Year Range
#     check_year_range(mobj.group(1))
# # Check Month Range
#     check_month_range(mobj.group(2))
# # Check Day Range
#     check_day_range(mobj.group(3), mobj.group(1), mobj.group(2))
    return mobj


def is_date_str_format(date_string):
    check_result = True
    try:
        check_date_str_format(date_string)
    except ValueError:
        check_result = False
    return check_result


def check_month_str_format(month_string):
    mobj = re.match("([\d]{2,4})-([\d]{2})", month_string)
    if mobj is None:
        raise ValueError("The string[%s] is NOT month format" % month_string)
    month_string_len = len(month_string)
    # if month_string_len < MIN_MONTH_STRING_LENGTH or month_string_len > MAX_MONTH_STRING_LENGTH:
    if not (CMN_DEF.MIN_MONTH_STRING_LENGTH <= month_string_len <= CMN_DEF.MAX_MONTH_STRING_LENGTH):
        raise ValueError("The month stirng[%s] length is NOT in the range [%d, %d]" % (month_string_len, CMN_DEF.MIN_MONTH_STRING_LENGTH, CMN_DEF.MAX_MONTH_STRING_LENGTH))
# # Check Year Range
#     check_year_range(mobj.group(1))
# # Check Month Range
#     check_month_range(mobj.group(2))
    return mobj


def is_month_str_format(month_string):
    check_result = True
    try:
        check_month_str_format(month_string)
    except ValueError:
        check_result = False
    return check_result


def check_quarter_str_format(quarter_string):
    mobj = re.match("([\d]{2,4})[Qq]([\d]{1})", quarter_string)
    if mobj is None:
        raise ValueError("The string[%s] is NOT quarter format" % quarter_string)
    quarter_string_len = len(quarter_string)
    # if quarter_string_len < MIN_QUARTER_STRING_LENGTH or quarter_string_len > MAX_QUARTER_STRING_LENGTH:
    if not (CMN_DEF.MIN_QUARTER_STRING_LENGTH <= quarter_string_len <= CMN_DEF.MAX_QUARTER_STRING_LENGTH):
        raise ValueError("The quarter stirng[%s] length is NOT in the range [%d, %d]" % (quarter_string_len, CMN_DEF.MIN_QUARTER_STRING_LENGTH, CMN_DEF.MAX_QUARTER_STRING_LENGTH))
# # Check Year Range
#     check_year_range(mobj.group(1))
# # Check Quarter Range
#     check_quarter_range(mobj.group(2))
    return mobj


def is_quarter_str_format(quarter_string):
    check_result = True
    try:
        check_quarter_str_format(quarter_string)
    except ValueError:
        check_result = False
    return check_result


def transform_date_str(year_value, month_value, day_value):
    return "%d-%02d-%02d" % (year_value, month_value, day_value)


def transform_month_str(year_value, month_value):
    return "%d-%02d" % (year_value, month_value)


def transform_quarter_str(year_value, quarter_value):
    return "%dq%d" % (year_value, quarter_value)


def transform_time_str(hour_value, minute_value, second_value):
    return "%02d:%02d:%02d" % (hour_value, minute_value, second_value)


def generate_cur_timestamp_str():
    datetime_cur = datetime.today()
    date_str = transform_date_str(datetime_cur.year, datetime_cur.month, datetime_cur.day)
    time_str = transform_time_str(datetime_cur.hour, datetime_cur.minute, datetime_cur.second)
    return "%s %s %s" % (CMN_DEF.CONFIG_TIMESTAMP_STRING_PREFIX, date_str, time_str)


# def transform_string2datetime(date_string, need_year_transform=False):
#     element_arr = date_string.split('-')
#     if len(element_arr) != 3:
#         raise ValueError("Incorrect config date format: %s" % date_string)
#     return datetime((int(element_arr[0]) if not need_year_transform else (int(element_arr[0]) + CMN_DEF.REPUBLIC_ERA_YEAR_OFFSET)), int(element_arr[1]), int(element_arr[2]))


# def transform_datetime_cfg2string(datetime_cfg, need_year_transform=False):
#     return transform_datetime2string(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day, need_year_transform)


# def transform_datetime2string(year, month, day, need_year_transform=False):
#     year_transform = (int(year) + CMN_DEF.REPUBLIC_ERA_YEAR_OFFSET) if need_year_transform else int(year)
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


def get_config_filepath(conf_filename, conf_folderpath=None):
    # current_path = os.path.dirname(os.path.realpath(__file__))
    # [project_folder, lib_folder] = current_path.rsplit('/', 1)
    if conf_folderpath is None:
        conf_filepath = "%s/%s/%s" % (CMN_DEF.PROJECT_FOLDERPATH, CMN_DEF.CONF_FOLDER, conf_filename)
    else:
        conf_filepath = "%s/%s" % (conf_folderpath, conf_filename)
    # g_logger.debug("Parse the config file: %s" % conf_filepath)
    return conf_filepath


def get_finance_analysis_mode():
    conf_line_list = read_config_file_lines(CMN_DEF.MARKET_STOCK_SWITCH_CONF_FILENAME)
    if len(conf_line_list) != 1:
        raise ValueError("Incorrect setting in %s" % CMN_DEF.MARKET_STOCK_SWITCH_CONF_FILENAME)
    mode = int(conf_line_list[0])
    if mode not in [CMN_DEF.FINANCE_ANALYSIS_MARKET, CMN_DEF.FINANCE_ANALYSIS_STOCK]:
        raise ValueError("Unknown finance analysis mode: %d" % mode)
    return mode
    # conf_filepath = get_config_filepath(CMN_DEF.MARKET_STOCK_SWITCH_CONF_FILENAME)
    # try:
    #     with open(conf_filepath, 'r') as fp:
    #         for line in fp:
    #             mode = int(line)
    #             if mode not in [CMN_DEF.FINANCE_ANALYSIS_MARKET, CMN_DEF.FINANCE_ANALYSIS_STOCK]:
    #                 raise ValueError("Unknown finance analysis mode: %d" % mode)
    #             return mode
    # except Exception as e:
    #     g_logger.error("Error occur while parsing config file[%s], due to %s" % (CMN_DEF.MARKET_STOCK_SWITCH_CONF_FILENAME, str(e)))
    #     raise e


def is_market_mode():
    if CMN_DEF.FINANCE_MODE == CMN_DEF.FINANCE_ANALYSIS_UNKNOWN:
        CMN_DEF.FINANCE_MODE = get_finance_analysis_mode()
    return CMN_DEF.FINANCE_MODE == CMN_DEF.FINANCE_ANALYSIS_MARKET
def is_stock_mode():
    if CMN_DEF.FINANCE_MODE == CMN_DEF.FINANCE_ANALYSIS_UNKNOWN:
        CMN_DEF.FINANCE_MODE = get_finance_analysis_mode()
    return CMN_DEF.FINANCE_MODE == CMN_DEF.FINANCE_ANALYSIS_STOCK


def get_finance_mode_description():
    if CMN_DEF.FINANCE_MODE == CMN_DEF.FINANCE_ANALYSIS_UNKNOWN:
        CMN_DEF.FINANCE_MODE = get_finance_analysis_mode()
    return CMN_DEF.FINANCE_MODE_DESCRIPTION[CMN_DEF.FINANCE_MODE]


def read_file_lines_ex(filepath, file_read_attribute):
    line_list = []
    try:
        with open(filepath, file_read_attribute) as fp:
            for line in fp:
                if line.startswith('#'):
                    continue
                line_strip = line.strip('\n')
                if len(line_strip) == 0:
                    continue
                line_list.append(line_strip)
    except Exception as e:
        errmsg = "Error occur while reading file[%s], due to %s" % (filepath, str(e))
        g_logger.error(errmsg)
        raise ValueError(errmsg)
    return line_list


def read_config_file_lines_ex(conf_filename, conf_file_read_attribute, conf_folderpath=None):
    conf_filepath = get_config_filepath(conf_filename, conf_folderpath)
    return read_file_lines_ex(conf_filepath, conf_file_read_attribute)


def read_config_file_lines(conf_filename, conf_folderpath=None):
    return read_config_file_lines_ex(conf_filename, 'r', conf_folderpath)


def unicode_read_config_file_lines_ex(conf_filename, conf_file_read_attribute, conf_folderpath=None, conf_unicode_encode=None):
    if conf_unicode_encode is None:
        conf_unicode_encode = CMN_DEF.UNICODE_ENCODING_IN_FILE
    conf_filepath = get_config_filepath(conf_filename, conf_folderpath)
    conf_line_list = []
    try:
        with open(conf_filepath, conf_file_read_attribute) as fp:
            for line_tmp in fp:
                line = line_tmp.decode(conf_unicode_encode)
                if line.startswith('#'):
                    continue
                line_strip = line.strip('\n')
                if len(line_strip) == 0:
                    continue
                conf_line_list.append(line_strip)
    except Exception as e:
        errmsg = "Error occur while reading config file[%s] in unicode, due to %s" % (conf_filename, str(e))
        g_logger.error(errmsg)
        raise ValueError(errmsg)
    return conf_line_list


def unicode_read_config_file_lines(conf_filename, conf_folderpath=None, conf_unicode_encode=None):
    if conf_unicode_encode is None:
        conf_unicode_encode = CMN_DEF.UNICODE_ENCODING_IN_FILE
    return unicode_read_config_file_lines_ex(conf_filename, 'rb', conf_folderpath, conf_unicode_encode)


def write_file_lines_ex(line_list, filepath, file_write_attribute):
    try:
        with open(filepath, file_write_attribute) as fp:
            for line in line_list:
                if not line.endswith("\n"):
                    line += "\n"
                fp.write(line)
    except Exception as e:
        errmsg = "Error occur while writing file[%s], due to %s" % (filepath, str(e))
        g_logger.error(errmsg)
        raise ValueError(errmsg)


def write_config_file_lines_ex(conf_line_list, conf_filename, conf_file_write_attribute, conf_folderpath=None):
    conf_filepath = get_config_filepath(conf_filename, conf_folderpath)
    write_file_lines_ex(conf_line_list, conf_filepath, conf_file_write_attribute)


def write_config_file_lines(conf_line_list, conf_filename, conf_folderpath=None):
    return write_config_file_lines_ex(conf_line_list, conf_filename, 'w', conf_folderpath)


def unicode_write_config_file_lines_ex(conf_line_list, conf_filename, conf_file_write_attribute, conf_folderpath=None, conf_unicode_encode=None):
    if conf_unicode_encode is None:
        conf_unicode_encode = CMN_DEF.UNICODE_ENCODING_IN_FILE
    conf_filepath = get_config_filepath(conf_filename, conf_folderpath)
    try:
        with open(conf_filepath, conf_file_write_attribute) as fp:
            for line_tmp in conf_line_list:
                line = line_tmp.encode(conf_unicode_encode)
                if not line.endswith("\n"):
                    line += "\n"
                fp.write(line)
    except Exception as e:
        errmsg = "Error occur while writing config file[%s], due to %s" % (conf_filename, str(e))
        g_logger.error(errmsg)
        raise ValueError(errmsg)


def unicode_write_config_file_lines(conf_line_list, conf_filename, conf_folderpath=None, conf_unicode_encode=None):
    if conf_unicode_encode is None:
        conf_unicode_encode = CMN_DEF.UNICODE_ENCODING_IN_FILE
    return unicode_write_config_file_lines_ex(conf_line_list, conf_filename, 'wb', conf_folderpath, conf_unicode_encode)


def read_source_type_time_duration_config_file(conf_filename, time_duration_type):
    # import pdb; pdb.set_trace()
    conf_line_list = read_config_file_lines(conf_filename)
    source_type_time_duration_config_list = []
    for line in conf_line_list:
        param_list = line.split(' ')
        param_list_len = len(param_list)
        # source_type_index = CMN_DEF.DATA_SOURCE_INDEX_MAPPING.index(param_list[0].decode(CMN_DEF.UNICODE_ENCODING_IN_FILE))
        # source_type_index = get_source_type_index_from_description(param_list[0].decode(CMN_DEF.UNICODE_ENCODING_IN_FILE))
        source_type_index_list = get_source_type_index_list_from_method_description(param_list[0].decode(CMN_DEF.UNICODE_ENCODING_IN_FILE))
        time_duration_start = None
        if param_list_len >= 2:
            # time_duration_start = transform_string2datetime(param_list[1])
            time_duration_start = CMN_CLS.FinanceTimeBase.from_string(param_list[1])
        time_duration_end = None
        if param_list_len >= 3:
            # time_duration_end = transform_string2datetime(param_list[2])
            time_duration_end = CMN_CLS.FinanceTimeBase.from_string(param_list[2])
        for source_type_index in source_type_index_list:
            source_type_time_duration_config_list.append(
                CMN_CLS.SourceTypeTimeDurationTuple(source_type_index, time_duration_type, time_duration_start, time_duration_end)
            )
    return source_type_time_duration_config_list


def read_csv_time_duration_config_file(conf_filename, conf_folderpath, return_as_list=False):
    # import pdb; pdb.set_trace()
    csv_time_duration_dict = {}
    try:
        conf_line_list = read_config_file_lines(conf_filename, conf_folderpath)
        for line in conf_line_list:
            param_list = line.split(' ')
            param_list_len = len(param_list)
            # source_type_index = CMN_DEF.DATA_SOURCE_INDEX_MAPPING.index(param_list[0].decode(CMN_DEF.UNICODE_ENCODING_IN_FILE))
            if param_list_len != 3:
                raise ValueError("Incorrect csv time duration setting: %s, list len: %d" % (line, param_list_len))
            source_type_index = get_source_type_index_from_description(param_list[0].decode(CMN_DEF.UNICODE_ENCODING_IN_FILE))
            time_range_start = CMN_CLS.FinanceTimeBase.from_time_string(param_list[1])
            time_range_end = CMN_CLS.FinanceTimeBase.from_time_string(param_list[2])
            csv_time_duration_dict[source_type_index] = CMN_CLS.TimeDurationTuple(time_range_start, time_range_end)
    except ValueError as e:
        csv_time_duration_dict = None
    return csv_time_duration_dict


def write_csv_time_duration_config_file(conf_filename, conf_folderpath, csv_time_duration_dict):
    # import pdb; pdb.set_trace()
    conf_line_list = []
    source_type_start_index, source_type_end_index = get_source_type_index_range()
    for source_type_index in range(source_type_start_index, source_type_end_index):
        time_duration_tuple = csv_time_duration_dict.get(source_type_index, None)
        if time_duration_tuple is None:
            continue
        csv_time_duration_entry_unicode = u"%s %s %s" % (CMN_DEF.DATA_SOURCE_INDEX_MAPPING[source_type_index], time_duration_tuple.time_duration_start, time_duration_tuple.time_duration_end)
        conf_line_list.append(csv_time_duration_entry_unicode.encode(CMN_DEF.UNICODE_ENCODING_IN_FILE) + "\n")
    write_config_file_lines_ex(conf_line_list, conf_filename, "wb", conf_folderpath)


def read_company_config_file(conf_filename):
    # import pdb; pdb.set_trace()
    conf_line_list = read_config_file_lines(conf_filename)
    company_config_list = []
    for line in conf_line_list:
        param_list = line.split(CMN_DEF.SPACE_DATA_SPLIT)
        for param in param_list:
            company_config_list.append(param)
    return company_config_list


def get_cfg_month_last_day(datetime_cfg):
    return get_month_last_day(datetime_cfg.year, datetime_cfg.month)


def get_month_last_day(year, month):
    return calendar.monthrange(year, month)[1]


# def get_year_offset_datetime_cfg(datetime_cfg, year_offset):
#     return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


# def get_datetime_duration_by_month_list(datetime_duration_start=None, datetime_duration_end=None):
# # Parse the current time
#     if datetime_duration_end is None:
#         datetime_duration_end = datetime.today()
#     datetime_duration_list = []
#     datetime_cur = datetime_duration_start
#     # import pdb; pdb.set_trace()
#     while True:
#         last_day = get_cfg_month_last_day(datetime_cur)
#         datetime_duration_list.append(
#             {
#                 'start': datetime(datetime_cur.year, datetime_cur.month, 1),
#                 'end': datetime(datetime_cur.year, datetime_cur.month, last_day),
#             }
#         )
#         if datetime_duration_end.year == datetime_cur.year and datetime_duration_end.month == datetime_cur.month:
#             break
#         offset_day = 15 if datetime_cur.day > 20 else last_day
#         datetime_cur +=  timedelta(days = offset_day)
#     # import pdb; pdb.set_trace()
#     if len(datetime_duration_list) == 0:
#         raise RuntimeError("The length of the datetime_duration_list list should NOT be 0")
#     if datetime_duration_start is not None:
#         datetime_duration_list[0]['start'] = datetime_duration_start
#     if datetime_duration_end is not None:
#         datetime_duration_list[-1]['end'] = datetime_duration_end

#     return datetime_duration_list


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


def check_config_file_exist(conf_filename, conf_folderpath=None):
    conf_filepath = get_config_filepath(conf_filename, conf_folderpath)
    return check_file_exist(conf_filepath)


def create_folder(folderpath):
    # os.mkdir(folderpath)
    os.makedirs(folderpath)


def create_folder_if_not_exist(folderpath):
    need_create = not check_file_exist(folderpath)
    if need_create:
        create_folder(folderpath)
    return need_create


def rename_file(old_filepath, new_filepath):
    os.rename(old_filepath, new_filepath)


def rename_file_if_exist(old_filepath, new_filepath):
    can_rename = check_file_exist(old_filepath)
    if can_rename:
        rename_file(old_filepath, new_filepath)
    return can_rename


def remove_file(filepath):
    os.remove(filepath)


def remove_file_if_exist(filepath):
    can_remove = check_file_exist(filepath)
    if can_remove:
        remove_file(filepath)
    return can_remove


def remove_config_file_if_exist(conf_filename, conf_folderpath=None):
    conf_filepath = get_config_filepath(conf_filename, conf_folderpath)
    return remove_file_if_exist(conf_filepath)


def remove_folder(folderpath):
    shutil.rmtree(folderpath)


def remove_folder_if_exist(folderpath):
    can_remove = check_file_exist(folderpath)
    if can_remove:
        remove_folder(folderpath)
    return can_remove


def copy_file(src_filepath, dst_filepath):
    shutil.copy2(src_filepath, dst_filepath)


def copy_file_if_exist(src_filepath, dst_filepath):
    can_copy = check_file_exist(src_filepath)
    if can_copy:
        copy_file(src_filepath, dst_filepath)
    return can_copy


def append_data_into_file(src_filepath, dst_filepath):
    if not check_file_exist(src_filepath):
        raise ValueError("The file[%s] does NOT exist" % src_filepath)
    with open(src_filepath, 'r') as src_fp, open(dst_filepath, 'a+') as dst_fp:
        for line in src_fp:
            dst_fp.write(line)


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


def get_filename_from_filepath(filepath):
    return filepath.rsplit("/", 1)[-1]


def assemble_market_csv_folderpath(finance_root_folderpath):
    csv_filepath = "%s/%s" % (finance_root_folderpath, CMN_DEF.CSV_MARKET_FOLDERNAME) 
    return csv_filepath


def assemble_market_csv_filepath(finance_root_folderpath, source_type_index):
    csv_filepath = "%s/%s/%s.csv" % (finance_root_folderpath, CMN_DEF.CSV_MARKET_FOLDERNAME, CMN_DEF.SCRAPY_MODULE_NAME_MAPPING[source_type_index]) 
    return csv_filepath


def assemble_stock_csv_group_filepath(finance_root_folderpath, company_group_number):
    csv_filepath = "%s/%s%02d" % (finance_root_folderpath, CMN_DEF.CSV_STOCK_FOLDERNAME, int(company_group_number)) 
    return csv_filepath


def assemble_stock_csv_folderpath(finance_root_folderpath, company_code_number, company_group_number):
    csv_filepath = "%s/%s%02d/%s" % (finance_root_folderpath, CMN_DEF.CSV_STOCK_FOLDERNAME, int(company_group_number), company_code_number) 
    return csv_filepath


def assemble_stock_csv_filepath(finance_root_folderpath, source_type_index, company_code_number, company_group_number):
    csv_filepath = "%s/%s%02d/%s/%s.csv" % (finance_root_folderpath, CMN_DEF.CSV_STOCK_FOLDERNAME, int(company_group_number), company_code_number, CMN_DEF.SCRAPY_MODULE_NAME_MAPPING[source_type_index]) 
    return csv_filepath


# SCRAPY_WAIT_TIMEOUT = 8
def request_from_url_and_check_return(url, timeout=None):
    if timeout is None:
        timeout = CMN_DEF.SCRAPY_WAIT_TIMEOUT
    res = requests.get(url, timeout=timeout)
    if res.status_code != 200:
        if res.status_code == 503:
            raise CMN_EXCEPTION.WebScrapyServerBusyException("Fail to scrape URL[%s] due to Server is busy......")
        else:
            errmsg = "####### HTTP error: %d #######\nURL: %s" % (res.status_code, url)
            g_logger.error(errmsg)
            raise RuntimeError(errmsg)
    return res


def try_to_request_from_url_and_check_return(url, timeout=None):
    req = None
    for index in range(CMN_DEF.SCRAPY_RETRY_TIMES):
        try:
            # g_logger.debug("Retry to scrap web data [%s]......%d" % (url, index))
            req = request_from_url_and_check_return(url, timeout)
        except requests.exceptions.Timeout as ex:
            # g_logger.debug("Retry to scrap web data [%s]......%d, FAIL!!!" % (url, index))
            time.sleep(randint(3, 9))
        else:
            return req            
    errmsg = "Fail to scrap web data [%s] even retry for %d times !!!!!!" % (url, CMN_DEF.SCRAPY_RETRY_TIMES)
    g_logger.error(errmsg)
    raise RuntimeError(errmsg)


def is_time_in_range(finance_time_range_start, finance_time_range_end, finance_time):
    if finance_time_range_start <= finance_time_range_end:
        return (True if (finance_time_range_start <= finance_time <= finance_time_range_end) else False)
    else:
        return (True if (finance_time_range_start >= finance_time >= finance_time_range_end) else False)


def get_time_range_overlap_case(new_finance_time_start, new_finance_time_end, orig_finance_time_start, orig_finance_time_end):
    assert new_finance_time_start <= new_finance_time_end, "The new start time[%s] should be smaller than the end time[%s]" % (new_finance_time_start.to_string(), new_finance_time_end.to_string())
    assert orig_finance_time_start <= orig_finance_time_end, "The original start time[%s] should be smaller than the end time[%s]" % (orig_finance_time_start.to_string(), orig_finance_time_end.to_string())
    if new_finance_time_end < orig_finance_time_start or new_finance_time_start > orig_finance_time_end:
        return CMN_DEF.TIME_OVERLAP_NONE
    else:
        new_start_time_in_range = is_time_in_range(orig_finance_time_start, orig_finance_time_end, new_finance_time_start)
        new_end_time_in_range = is_time_in_range(orig_finance_time_start, orig_finance_time_end, new_finance_time_end)
        if new_start_time_in_range and new_end_time_in_range:
            return CMN_DEF.TIME_OVERLAP_COVERED
        elif (not new_start_time_in_range) and new_end_time_in_range:
            return CMN_DEF.TIME_OVERLAP_BEFORE
        elif new_start_time_in_range and (not new_end_time_in_range):
            return CMN_DEF.TIME_OVERLAP_AFTER
        elif (not new_start_time_in_range) and (not new_end_time_in_range):
            return CMN_DEF.TIME_OVERLAP_COVER
    raise CMN_EXCEPTION.WebScrapyUnDefiedCaseException("Un-Defined case !!!")



def is_time_range_overlap(finance_time1_start, finance_time1_end, finance_time2_start, finance_time2_end):
    check_overlap1 = True
    if finance_time1_start is not None and finance_time2_end is not None:
        check_overlap1 = (finance_time1_start <= finance_time2_end)
    check_overlap2 = True
    if finance_time2_start is not None and finance_time1_end is not None:
        check_overlap2 = (finance_time2_start <= finance_time1_end)  
    return (check_overlap1 and check_overlap2)


def is_continous_time_duration(time_duration1, time_duration2):
    assert type(time_duration1) == type(time_duration2), "The time types[%s, %s] are NOT identical" % (type(time_duration1), type(time_duration2))  
    if time_duration1 == time_duration2:
        return True
    time_duration_start = time_duration1
    time_duration_end = time_duration2
    if time_duration1 > time_duration2:
        time_duration_start = time_duration2
        time_duration_end = time_duration1            
    return True if (time_duration_start + 1) == time_duration_end else False


def get_year_offset_datetime_cfg(datetime_cfg, year_offset):
    return datetime(datetime_cfg.year + year_offset, datetime_cfg.month, datetime_cfg.day)


def get_data_not_whole_month_list(date_duration_start, date_duration_end):
    assert isinstance(date_duration_start, CMN_CLS.FinanceDate), "The input start date duration time unit is %s, not FinanceDate" % type(date_duration_start)
    assert isinstance(date_duration_end, CMN_CLS.FinanceDate), "The input end date duration time unit is %s, not FinanceDate" % type(date_duration_end)
    data_not_whole_month_list = []
# Find the month which data does NOT contain the whole month
    if CMN_CLS.FinanceDate.is_same_month(date_duration_start, date_duration_end):
        if date_duration_start.day > 1 or date_duration_end.day < get_month_last_day(date_duration_end.year, date_duration_end.month):
            data_not_whole_month_list.append(CMN_CLS.FinanceMonth(date_duration_end.year, date_duration_end.month))
    else:
        if date_duration_start.day > 1:
            data_not_whole_month_list.append(CMN_CLS.FinanceMonth(date_duration_start.year, date_duration_start.month))
        if date_duration_end.day < get_month_last_day(date_duration_end.year, date_duration_end.month):
            data_not_whole_month_list.append(CMN_CLS.FinanceMonth(date_duration_end.year, date_duration_end.month))
    return data_not_whole_month_list
