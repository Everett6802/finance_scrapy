# -*- coding: utf8 -*-

import re
import numpy as np
import pandas as pd
import talib
# conda install -c quantopian ta-lib=0.4.9
# https://blog.csdn.net/fortiy/article/details/76531700
import libs.common as CMN
import common_definition as CMN_DEF
g_logger = CMN.LOG.get_logger()


def print_dataset_metadata(df, column_description_list):
    print "*** Time Period ***"
    print "%s - %s" % (df.index[0].strftime("%Y-%m-%d"), df.index[-1].strftime("%Y-%m-%d"))
    print "*** Column Mapping ***"
    for index in range(1, len(column_description_list)):
        print u"%s: %s" % (df.columns[index - 1], column_description_list[index])


def get_dataset_sma(df, column_name):
    SMA = talib.SMA(df[column_name].as_matrix())
    return SMA


def Date2date(Date_str):
    element_list = Date_str.split("-")
    return "%02s%02s%02s" % (element_list[0][2:], element_list[1], element_list[2])  

def date2Date(date_str):
    return "20%s-%02s-%02s" % (date_str[0:2], date_str[2:4], date_str[4:6])  


def parse_stock_price_statistics_config(company_code_number, config_folderpath=None):
    if config_folderpath is None:
        config_folderpath = CMN_DEF.DEF_SUPPORT_RESISTANCE_FOLDER_PATH
    config_filepath = "%s/%s.conf" % (config_folderpath, company_code_number)
    stock_price_statistics_config = {}
    conf_line_list = CMN.FUNC.read_file_lines_ex(config_filepath)
    cur_conf_field = None
    cur_conf_field_index = None
    for conf_line in conf_line_list:
        if re.match("\[[\w]+\]", conf_line) is not None:
            cur_conf_field = conf_line.strip("[]")
            if cur_conf_field == CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_START_DATE:
                cur_conf_field_index = CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_START_DATE_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_KEY_SUPPORT_RESISTANCE:
                cur_conf_field_index = CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_KEY_SUPPORT_RESISTANCE_INDEX
                stock_price_statistics_config[cur_conf_field] = []
            else:
                raise ValueError("Unknown config field: %s" % cur_conf_field)                
        else:
            assert cur_conf_field is not None, "cur_conf_field should NOT be None"
            if cur_conf_field_index == CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_START_DATE_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_KEY_SUPPORT_RESISTANCE_INDEX:
                stock_price_statistics_config[cur_conf_field].append(conf_line)
            else:
                raise ValueError("Unknown config field index: %d" % cur_conf_field_index)
    return stock_price_statistics_config


def sort_stock_price_statistics_ex(df, cur_price=None, price_range_low_value=None, price_range_low_percentage=None, price_range_high_value=None, price_range_high_percentage=None, stock_price_statistics_config=None):
    # import pdb; pdb.set_trace()
    if cur_price is None:
        if price_range_low_value is not None:
            g_logger.warn("The price_range_low_value argument is invalid since cur_price is None")
            price_range_low_value = None
        if price_range_low_percentage is not None:
            g_logger.warn("The price_range_low_percentage argument is invalid since cur_price is None")
            price_range_low_percentage = None
        if price_range_high_value is not None:
            g_logger.warn("The price_range_high_value argument is invalid since cur_price is None")
            price_range_high_value = None
        if price_range_high_percentage is not None:
            g_logger.warn("The price_range_high_percentage argument is invalid since cur_price is None")
            price_range_high_percentage = None
    else:
        if price_range_low_value is not None and price_range_low_percentage is not None:
            g_logger.warn("The price_range_low_value argument is invalid since price_range_low_percentage is None")
            price_range_low_value = None
        if price_range_high_value is not None and price_range_high_percentage is not None:
            g_logger.warn("The price_range_high_value argument is invalid since price_range_high_percentage is None")
            price_range_high_value = None

    price_range_low = None
    price_range_high = None

    if price_range_low_percentage is not None:
        price_range_low = cur_price * (100.0 - price_range_low_percentage) / 100.0
    elif price_range_low_value is not None:
        price_range_low = cur_price - price_range_low_value
    if price_range_high_percentage is not None:
        price_range_high = cur_price * (100.0 + price_range_high_percentage) / 100.0
    elif price_range_high_value is not None:
        price_range_high = cur_price + price_range_high_value

    # import pdb; pdb.set_trace()
    start_date = None
    if stock_price_statistics_config is not None:
        start_date = stock_price_statistics_config.get(CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_START_DATE, None)
    df_copy = None
    if start_date is None: 
        df_copy = df.copy()
    else:
        start_date_index = date2Date(start_date)
        mask = (df.index >= start_date_index)
        df_copy = df[mask].copy()
    # df_copy.sort_index(ascending=True)
    df_copy['open_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_NONE
    df_copy['open_mark'].astype('category')
    df_copy['high_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_NONE
    df_copy['high_mark'].astype('category')
    df_copy['low_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_NONE
    df_copy['low_mark'].astype('category')
    df_copy['close_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_NONE
    df_copy['close_mark'].astype('category')
    # import pdb; pdb.set_trace()
    if stock_price_statistics_config is not None:
        key_support_resistance_list = stock_price_statistics_config[CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_KEY_SUPPORT_RESISTANCE]
        for key_support_resistance in key_support_resistance_list:
            key_support_resistance_mark_list = CMN_DEF.DEF_KEY_SUPPORT_RESISTANCE_MARK
            if len(key_support_resistance) > CMN_DEF.DEF_KEY_SUPPORT_RESISTANCE_LEN:
                key_support_resistance_mark_list = key_support_resistance[CMN_DEF.DEF_KEY_SUPPORT_RESISTANCE_LEN:]
            key_support_resistance_date = key_support_resistance[:CMN_DEF.DEF_KEY_SUPPORT_RESISTANCE_LEN]
            key_support_resistance_date_index = date2Date(key_support_resistance_date)
# Check if the date exist
            if key_support_resistance_date_index not in df_copy.index:
                g_logger.warn("The date[%s] does NOT exist in data" % key_support_resistance_date)
                continue
            for key_support_resistance_mark in key_support_resistance_mark_list:
                if key_support_resistance_mark == 'O':
                    df_copy.ix[key_support_resistance_date_index, 'open_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_KEY
                elif key_support_resistance_mark == 'H':
                    df_copy.ix[key_support_resistance_date_index, 'high_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_KEY
                elif key_support_resistance_mark == 'L':
                    df_copy.ix[key_support_resistance_date_index, 'low_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_KEY
                elif key_support_resistance_mark == 'C':
                    df_copy.ix[key_support_resistance_date_index, 'close_mark'] = CMN_DEF.SUPPORT_RESISTANCE_MARK_KEY
                else:
                    raise ValueError("Unkown mark type" % key_support_resistance_mark)

    df_copy.reset_index(inplace=True)
    df_open = (df_copy[['date','open','open_mark']].copy())
    df_open['type'] = "O"
    df_open.rename(columns={'open': 'price', 'open_mark': 'mark'}, inplace=True)
    df_high = df_copy[['date','high','high_mark']].copy()
    df_high['type'] = "H"
    df_high.rename(columns={'high': 'price', 'high_mark': 'mark'}, inplace=True)
    df_low = df_copy[['date','low','low_mark']].copy()
    df_low['type'] = "L"
    df_low.rename(columns={'low': 'price', 'low_mark': 'mark'}, inplace=True)
    df_close = df_copy[['date','close','close_mark']].copy()
    df_close['type'] = "C"
    df_close.rename(columns={'close': 'price', 'close_mark': 'mark'}, inplace=True)
    df_total_value = pd.concat([df_open, df_high, df_low, df_close])

# Display the price in range
    if price_range_low is not None:
        df_total_value = df_total_value[df_total_value['price'] >= price_range_low]
    if price_range_high is not None:
        df_total_value = df_total_value[df_total_value['price'] <= price_range_high]
        
#     df_total_value.sort_values("price", ascending=True, inplace=True)
    price_statistics = df_total_value.groupby('price')
    return price_statistics


def sort_stock_price_statistics(df, cur_price, price_range_low_percentage=12, price_range_high_percentage=12, stock_price_statistics_config=None):
    return sort_stock_price_statistics_ex(df, cur_price, price_range_low_percentage=price_range_low_percentage, price_range_high_percentage=price_range_high_percentage, stock_price_statistics_config=stock_price_statistics_config)


def print_stock_price_statistics(df, cur_price=None, price_range_low_percentage=12, price_range_high_percentage=12, stock_price_statistics_config=None, show_stock_price_statistics_fiter=None):
    price_statistics = sort_stock_price_statistics(df, cur_price, price_range_low_percentage=price_range_low_percentage, price_range_high_percentage=price_range_high_percentage, stock_price_statistics_config=stock_price_statistics_config)
    price_statistics_size = len(price_statistics)

    show_marked_only = CMN_DEF.DEF_SUPPORT_RESISTANCE_SHOW_MARKED_ONLY
    group_size_thres = CMN_DEF.DEF_SUPPORT_RESISTANCE_GROUP_SIZE_THRES
# parse the filter
    if show_stock_price_statistics_fiter is not None:
        show_marked_only = show_stock_price_statistics_fiter.get("show_marked_only", CMN_DEF.DEF_SUPPORT_RESISTANCE_SHOW_MARKED_ONLY)
        group_size_thres = show_stock_price_statistics_fiter.get("group_size_thres", CMN_DEF.DEF_SUPPORT_RESISTANCE_GROUP_SIZE_THRES)

    cur_price_print = False
    for price, df_data in price_statistics:
# Print the current stock price      
        if not cur_price_print and cur_price < price:
            print CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_CUR + ">> %.2f <<" % cur_price
            cur_price_print = True
# Print the support and resistance
        is_marked = False
        # import pdb; pdb.set_trace()
        for index, row in df_data.iterrows():
            if row['mark'] != CMN_DEF.SUPPORT_RESISTANCE_MARK_NONE:
                is_marked = True
                break
        price_color_str = CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_MARK if is_marked else CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_NONE
        
        can_print = True
        if not is_marked:
            if show_marked_only and len(df_data) < group_size_thres:
                can_print = False
        if can_print:
            total_str = ""
            total_str += (" " + price_color_str + ("%.2f" % price) + CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_NONE + "  ")
            for index, row in df_data.iterrows():
                if row['mark'] != CMN_DEF.SUPPORT_RESISTANCE_MARK_NONE:
                    total_str += (CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_MARK + row['date'].strftime("%y%m%d") + row['type'] + CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_NONE + " ")
                else:
                    total_str += (CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_NONE + row['date'].strftime("%y%m%d") + row['type'] + " ")
            print total_str
# Print the current stock price      
        if not cur_price_print and cur_price == price:
            print CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_CUR + ">> %.2f <<" % cur_price
            cur_price_print = True
        # print "\n"
        # data_str = ",".join([if .strftime("%y%m%d")+row['type'] for index, row in df_data.iterrows()])    
        # if not cur_price_print and cur_price < price:
        #     print CMN_DEF.DEF_SUPPORT_RESISTANCE_COLOR_STR_CUR + "CUR: %f" % cur_price
        #     cur_price_print = True
        # print "\033[1;30;47m" + "Price: %f, Data: %s" % (price,data_str)
