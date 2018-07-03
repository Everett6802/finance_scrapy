# -*- coding: utf8 -*-

import re
import numpy as np
import pandas as pd
import talib
# conda install -c quantopian ta-lib=0.4.9
# https://blog.csdn.net/fortiy/article/details/76531700
import libs.common as CMN
import common_definition as DS_CMN_DEF
#CAUTION: DO NOT import common_class here to avoid import loop
# import common_class as DS_CMN_CLS
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


def str2bool(bool_str):
    if bool_str.lower() == "true":
        return True
    elif bool_str.lower() == "false":
        return False
    else:
        raise ValueError("Unknown boolean string: %s" % bool_str)


def parse_stock_price_statistics_config(company_code_number, config_folderpath=None):
    if config_folderpath is None:
        config_folderpath = DS_CMN_DEF.DEF_SR_FOLDER_PATH
    config_filepath = "%s/%s.conf" % (config_folderpath, company_code_number)
    stock_price_statistics_config = {}
    conf_line_list = CMN.FUNC.read_file_lines_ex(config_filepath)
    cur_conf_field = None
    cur_conf_field_index = None
    for conf_line in conf_line_list:
        if re.match("\[[\w]+\]", conf_line) is not None:
            cur_conf_field = conf_line.strip("[]")
            if cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_START_DATE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_START_DATE_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_KEY_SR_INDEX
                stock_price_statistics_config[cur_conf_field] = []
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_AUTO_DETECT_JUMP_GAP:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_AUTO_DETECT_JUMP_GAP_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP_INDEX
                stock_price_statistics_config[cur_conf_field] = []
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_TREND_LINE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_TREND_LINE_INDEX
                stock_price_statistics_config[cur_conf_field] = []
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SR_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_OVERWRITE_DATASET:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_OVERWRITE_DATASET_INDEX
                stock_price_statistics_config[cur_conf_field] = []
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_DATE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_DATE_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_PRICE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_PRICE_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH:
                cur_conf_field_index = DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH_INDEX
                stock_price_statistics_config[cur_conf_field] = None
            else:
                raise ValueError("Unknown config field: %s" % cur_conf_field)                
        else:
            assert cur_conf_field is not None, "cur_conf_field should NOT be None"
            if cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_START_DATE_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_KEY_SR_INDEX:
                stock_price_statistics_config[cur_conf_field].append(conf_line)
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_AUTO_DETECT_JUMP_GAP:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field == DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP:
                stock_price_statistics_config[cur_conf_field].append(conf_line)
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_TREND_LINE_INDEX:
                stock_price_statistics_config[cur_conf_field].append(conf_line)
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SR_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_OVERWRITE_DATASET_INDEX:
                stock_price_statistics_config[cur_conf_field].append(conf_line)
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_DATE_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_PRICE_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            elif cur_conf_field_index == DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH_INDEX:
                stock_price_statistics_config[cur_conf_field] = conf_line
            else:
                raise ValueError("Unknown config field index: %d" % cur_conf_field_index)

# Transform the value of the config setting
# Change the type of auto-detect jump gap
    auto_detect_jump_gap = DS_CMN_DEF.DEF_SR_AUTO_DETECT_JUMP_GAP
    auto_detect_jump_gap_from_config = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_AUTO_DETECT_JUMP_GAP, None)
    if auto_detect_jump_gap_from_config is not None:
        auto_detect_jump_gap = str2bool(auto_detect_jump_gap_from_config)

# Cleanup the jump_gap config setting if auto_detect_jump_gap is True
    if auto_detect_jump_gap:
        jump_gap_list = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_AUTO_DETECT_JUMP_GAP, None)
        if jump_gap_list is not None and len(jump_gap_list) > 0:
            g_logger.warn("The jump_gap parameter in the config is ignored due to auto-detection......")
# If auto_detect_jump_gap is True, this field is ignored
            stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP] = None
    else:
        jump_gap_from_config = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP, None)
        if jump_gap_from_config is not None:
            stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP] = []
            for line in jump_gap_from_config:
                line_split = line.split(":")
                if len(line_split) != 3:
                    raise ValueError("Incorrect jump gap format: %s" % line)
                if line_split[1] not in (DS_CMN_DEF.SR_PRICE_TYPE_HIGH, DS_CMN_DEF.SR_PRICE_TYPE_LOW):
                    raise ValueError("Incorrect price type in jump gap: %s" % line_split[1])
                stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP].append(line_split)

# Change the data type of the show_main_key_support_resistance config field
    show_main_key_support_resistance = int(stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE, DS_CMN_DEF.SHOW_MAIN_KEY_SR_DEFAULT))
    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE] = show_main_key_support_resistance

# Change the type of drawing support resistance date
    draw_support_resistance_date = DS_CMN_DEF.DEF_SR_DRAW_SR_DATE
    draw_support_resistance_date_from_config = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_DATE, None)
    if draw_support_resistance_date_from_config is not None:
        draw_support_resistance_date = str2bool(draw_support_resistance_date_from_config)
    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_DATE] = draw_support_resistance_date

# Change the type of drawing support resistance price
    draw_support_resistance_price = DS_CMN_DEF.DEF_SR_DRAW_SR_PRICE
    draw_support_resistance_price_from_config = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_PRICE, None)
    if draw_support_resistance_price_from_config is not None:
        draw_support_resistance_price = str2bool(draw_support_resistance_price_from_config)
    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_DRAW_SR_PRICE] = draw_support_resistance_price

# Change the type of the flag for saving figure
    save_figure = DS_CMN_DEF.DEF_SR_SAVE_FIGURE
    save_figure_from_config = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE, None)
    if save_figure_from_config is not None:
        save_figure = str2bool(save_figure_from_config)
    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE] = save_figure

# Change the type of the flag for generating report
    generate_report = DS_CMN_DEF.DEF_SR_GENERATE_REPORT
    generate_report_from_config = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT, None)
    if generate_report_from_config is not None:
        generate_report = str2bool(generate_report_from_config)
    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT] = generate_report

    if stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH, None) is None:
        stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH] = DS_CMN_DEF.DEF_SR_OUTPUT_FOLDER_PATH

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
    df_copy = df.copy()
    # start_date = None
    # if stock_price_statistics_config is not None:
    #     start_date = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_START_DATE, None)
    # df_copy = None
    # if start_date is None: 
    #     df_copy = df.copy()
    # else:
    #     start_date_index = date2Date(start_date)
    #     mask = (df.index >= start_date_index)
    #     df_copy = df[mask].copy()
    df_copy.sort_index(ascending=True)
    df_copy['open_mark'] = DS_CMN_DEF.SR_MARK_NONE
    df_copy['open_mark'].astype('category')
    df_copy['high_mark'] = DS_CMN_DEF.SR_MARK_NONE
    df_copy['high_mark'].astype('category')
    df_copy['low_mark'] = DS_CMN_DEF.SR_MARK_NONE
    df_copy['low_mark'].astype('category')
    df_copy['close_mark'] = DS_CMN_DEF.SR_MARK_NONE
    df_copy['close_mark'].astype('category')
    # import pdb; pdb.set_trace()
    if stock_price_statistics_config is not None:
# Mark Key Support Resistance
        key_support_resistance_list = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE]
        for key_support_resistance in key_support_resistance_list:
            key_support_resistance_mark_list = DS_CMN_DEF.DEF_KEY_SR_MARK
            if len(key_support_resistance) > DS_CMN_DEF.DEF_KEY_SR_LEN:
                key_support_resistance_mark_list = key_support_resistance[DS_CMN_DEF.DEF_KEY_SR_LEN:]
            key_support_resistance_date = key_support_resistance[:DS_CMN_DEF.DEF_KEY_SR_LEN]
            key_support_resistance_date_index = date2Date(key_support_resistance_date)
# Check if the date exist
            if key_support_resistance_date_index not in df_copy.index:
                g_logger.warn("The date[%s] does NOT exist in data" % key_support_resistance_date)
                continue
            for key_support_resistance_mark in key_support_resistance_mark_list:
                if key_support_resistance_mark == DS_CMN_DEF.SR_PRICE_TYPE_OPEN:
                    df_copy.ix[key_support_resistance_date_index, 'open_mark'] = DS_CMN_DEF.SR_MARK_KEY
                elif key_support_resistance_mark == DS_CMN_DEF.SR_PRICE_TYPE_HIGH:
                    df_copy.ix[key_support_resistance_date_index, 'high_mark'] = DS_CMN_DEF.SR_MARK_KEY
                elif key_support_resistance_mark == DS_CMN_DEF.SR_PRICE_TYPE_LOW:
                    df_copy.ix[key_support_resistance_date_index, 'low_mark'] = DS_CMN_DEF.SR_MARK_KEY
                elif key_support_resistance_mark == DS_CMN_DEF.SR_PRICE_TYPE_CLOSE:
                    df_copy.ix[key_support_resistance_date_index, 'close_mark'] = DS_CMN_DEF.SR_MARK_KEY
                else:
                    raise ValueError("Unkown mark type" % key_support_resistance_mark)
        # import pdb; pdb.set_trace()
# Mark Jump Gap
        jump_gap_list = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP, None)
        if jump_gap_list is not None:
            for jump_gap in jump_gap_list:
                jump_gap_date_cur = jump_gap[0]
                jump_gap_date_cur_index = date2Date(jump_gap_date_cur)
                jump_gap_date_next = jump_gap[2]
                jump_gap_date_next_index = date2Date(jump_gap_date_next)
                if jump_gap[1] == DS_CMN_DEF.SR_PRICE_TYPE_HIGH:
# The type of the return value of ix() is float, I DON'T known why
                    df_copy.ix[jump_gap_date_cur_index, 'high_mark'] = int(df_copy.ix[jump_gap_date_cur_index, 'high_mark']) | DS_CMN_DEF.SR_MARK_JUMP_GAP
                    df_copy.ix[jump_gap_date_next_index, 'low_mark'] = int(df_copy.ix[jump_gap_date_next_index, 'low_mark']) | DS_CMN_DEF.SR_MARK_JUMP_GAP
                elif jump_gap[1] == DS_CMN_DEF.SR_PRICE_TYPE_LOW:
# The type of the return value of ix() is float, I DON'T known why
                    df_copy.ix[jump_gap_date_cur_index, 'low_mark'] = int(df_copy.ix[jump_gap_date_cur_index, 'low_mark']) | DS_CMN_DEF.SR_MARK_JUMP_GAP
                    df_copy.ix[jump_gap_date_next_index, 'high_mark'] = int(df_copy.ix[jump_gap_date_next_index, 'high_mark']) | DS_CMN_DEF.SR_MARK_JUMP_GAP
                else:
                    raise ValueError("Unkown mark type in jump gap: %s" % jump_gap[1])

    df_copy.reset_index(inplace=True)
    df_open = (df_copy[['date','open','open_mark']].copy())
    df_open['type'] = DS_CMN_DEF.SR_PRICE_TYPE_OPEN
    df_open.rename(columns={'open': 'price', 'open_mark': 'mark'}, inplace=True)
    df_high = df_copy[['date','high','high_mark']].copy()
    df_high['type'] = DS_CMN_DEF.SR_PRICE_TYPE_HIGH
    df_high.rename(columns={'high': 'price', 'high_mark': 'mark'}, inplace=True)
    df_low = df_copy[['date','low','low_mark']].copy()
    df_low['type'] = DS_CMN_DEF.SR_PRICE_TYPE_LOW
    df_low.rename(columns={'low': 'price', 'low_mark': 'mark'}, inplace=True)
    df_close = df_copy[['date','close','close_mark']].copy()
    df_close['type'] = DS_CMN_DEF.SR_PRICE_TYPE_CLOSE
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


def print_stock_price_statistics(df, cur_price=None, price_range_low_percentage=12, price_range_high_percentage=12, stock_price_statistics_config=None, show_stock_price_statistics_fiter=None, html_report=None):

    price_statistics = sort_stock_price_statistics(df, cur_price, price_range_low_percentage=price_range_low_percentage, price_range_high_percentage=price_range_high_percentage, stock_price_statistics_config=stock_price_statistics_config)
    price_statistics_size = len(price_statistics)

    show_marked_only = DS_CMN_DEF.DEF_SR_SHOW_MARKED_ONLY
    group_size_thres = DS_CMN_DEF.DEF_SR_GROUP_SIZE_THRES
# parse the filter
    if show_stock_price_statistics_fiter is not None:
        show_marked_only = show_stock_price_statistics_fiter.get("show_marked_only", DS_CMN_DEF.DEF_SR_SHOW_MARKED_ONLY)
        group_size_thres = show_stock_price_statistics_fiter.get("group_size_thres", DS_CMN_DEF.DEF_SR_GROUP_SIZE_THRES)

# To avoid import loop
    from common_class import StockPrice as PRICE

    key_support_list = []
    key_resistance_list = []
    cur_price_print = False
    for price, df_data in price_statistics:
# Deep copy since the data is probably modified
        df_data = df_data.copy()
# Print the current stock price      
        if not cur_price_print and cur_price < price:
            print DS_CMN_DEF.DEF_SR_COLOR_STR_CUR + ">> %s <<" % PRICE(cur_price)
            if html_report: html_report.text(">> %s <<" % PRICE(cur_price), color_index=DS_CMN_DEF.SR_COLOR_CUR_INDEX)
            cur_price_print = True
# Print the support and resistance
        is_marked = False
        # import pdb; pdb.set_trace()
        for index, row in df_data.iterrows():
            if row['mark'] != DS_CMN_DEF.SR_MARK_NONE:
                is_marked = True
                break
        price_color_str = DS_CMN_DEF.DEF_SR_COLOR_STR_MARK if is_marked else DS_CMN_DEF.DEF_SR_COLOR_STR_NONE
        
        can_print = True
        if not is_marked:
            if show_marked_only and len(df_data) < group_size_thres:
                can_print = False
        if can_print:
            # import pdb; pdb.set_trace()
            total_str = ""
            total_str += (" " + price_color_str + "%s" % PRICE(price) + DS_CMN_DEF.DEF_SR_COLOR_STR_NONE + "  ")

            if html_report: html_report.text(" ", tail_newline_count=0).text("%s" % PRICE(price), color_index=(DS_CMN_DEF.SR_COLOR_MARK_INDEX if is_marked else None), tail_newline_count=0).text("  ", tail_newline_count=0)

            df_data.sort_index(ascending=False, inplace=True)
            cur_price_key_support_list = []
            cur_price_key_resistance_list = []
            for index, row in df_data.iterrows():
                if row['mark'] != DS_CMN_DEF.SR_MARK_NONE:
                    total_str += (DS_CMN_DEF.DEF_SR_COLOR_STR_MARK + row['date'].strftime("%y%m%d") + row['type'] + DS_CMN_DEF.DEF_SR_COLOR_STR_NONE + " ")
                    if html_report: html_report.text(row['date'].strftime("%y%m%d") + row['type'], color_index=DS_CMN_DEF.SR_COLOR_MARK_INDEX, tail_newline_count=0).text(" ", color_index=DS_CMN_DEF.SR_COLOR_NONE_INDEX, tail_newline_count=0)
                    # key_price_str = "%s[%s%s]" % (PRICE(price), row['date'].strftime("%y%m%d"), row['type'])
                    key_price_str = "%s%s" % (row['date'].strftime("%y%m%d"), row['type'])
                    if cur_price_print:
                        cur_price_key_resistance_list.append(key_price_str)
                    else:
                        cur_price_key_support_list.append(key_price_str)
                else:
                    total_str += (DS_CMN_DEF.DEF_SR_COLOR_STR_NONE + row['date'].strftime("%y%m%d") + row['type'] + " ")
                    if html_report: html_report.text(row['date'].strftime("%y%m%d") + row['type'] + " ", color_index=DS_CMN_DEF.SR_COLOR_NONE_INDEX, tail_newline_count=0)
            print total_str
            if html_report: html_report.newline()

            if len(cur_price_key_support_list) > 0:
                key_support_list.append("%s[" % PRICE(price) + ','.join(cur_price_key_support_list) + "]")
            if len(cur_price_key_resistance_list) > 0:
                key_resistance_list.append("%s[" % PRICE(price) + ','.join(cur_price_key_resistance_list) + "]")
# Print the current stock price      
        if not cur_price_print and cur_price == price:
            print DS_CMN_DEF.DEF_SR_COLOR_STR_CUR + ">> %s <<" % PRICE(cur_price)
            if html_report: html_report.text(">> %s <<" % PRICE(cur_price), color_index=DS_CMN_DEF.SR_COLOR_CUR_INDEX)
            cur_price_print = True

    if stock_price_statistics_config is not None:
        # import pdb; pdb.set_trace()
        show_main_key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE, DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE)
        if show_main_key_support_resistance != DS_CMN_DEF.SHOW_MAIN_NO_KEY_SUPPORT_RESISTANCE:
            main_key_support_resistance = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_MAIN_KEY_SUPPORT_RESISTANCE]
            print "\n***** Main Key Support Resistance *****"
            if html_report: html_report.text("***** Main Key Support Resistance *****", head_newline_count=1)
            # from common_class import StockPrice as PRICE
            if show_main_key_support_resistance in [DS_CMN_DEF.SHOW_MAIN_KEY_SUPPORT_ONLY,DS_CMN_DEF.SHOW_MAIN_KEY_SR_BOTH,]:
                support_date = main_key_support_resistance[0]
                support_date_index = date2Date(support_date)
                msg_s = "S: %s, %s%s" % (PRICE(df.ix[support_date_index, 'high']), support_date, DS_CMN_DEF.SR_PRICE_TYPE_HIGH)
                print msg_s
                if html_report: html_report.text(msg_s)
            if show_main_key_support_resistance in [DS_CMN_DEF.SHOW_MAIN_KEY_RESISTANCE_ONLY,DS_CMN_DEF.SHOW_MAIN_KEY_SR_BOTH,]:
                resistance_date = main_key_support_resistance[1]
                resistance_date_index = date2Date(resistance_date)
                msg_r = "R: %s, %s%s" % (PRICE(df.ix[resistance_date_index, 'low']), resistance_date, DS_CMN_DEF.SR_PRICE_TYPE_LOW)
                print msg_r
                if html_report: html_report.text(msg_r)
            print "***************************************"
            if html_report: html_report.text("***************************************")

        print "\n***** Key Support Resistance: *****"
        msg_s = "S: " + " > ".join(reversed(key_support_list))
        print msg_s
        msg_r = "R: " + " > ".join(key_resistance_list)
        print msg_r 
        print "**********************************"
        if html_report is not None:
            html_report.text("***** Key Support Resistance: *****", head_newline_count=1).text(msg_s).text(msg_r).text("**********************************")

        key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE, None)
        if key_support_resistance is not None:
            print "\n***** Key Support Resistance Price *****"
            if html_report is not None: html_report.text("***** Key Support Resistance Price *****", head_newline_count=1)
            for key_date in key_support_resistance:
                key_date_index = date2Date(key_date)
                row = df.ix[key_date_index]
                data_str = "%s O:%s H:%s L:%s C:%s" % (key_date, PRICE(row['open']), PRICE(row['high']), PRICE(row['low']), PRICE(row['close']))
                print data_str
                if html_report is not None: html_report.text(data_str)
            print "****************************************"
            if html_report is not None: html_report.text("****************************************", tail_newline_count=2)


def find_stock_price_jump_gap(df, tick_for_jump_gap=DS_CMN_DEF.DEF_TICK_FOR_JUMP_GAP):
# Return:
    # A list, element in a list:
    # (1) 0: Cur High Price, 1: H, 2: Next Low Price
    # (2) 0: Cur Low Price, 1: L, 2: Next High Price
    assert tick_for_jump_gap >= 1, "tick_for_jump_gap should be greater than 1"
    jump_gap_list = []
# To avoid import loop
    from common_class import StockPrice as PRICE
    for index in range(len(df) - 1):
        row_cur = df.ix[index]
        row_next = df.ix[index + 1]
        if row_next['low'] > row_cur['high']:
            # import pdb; pdb.set_trace()
# CAUTION: Can't get correct index in this way !!!
# Need to enhance
            # jump_gap_list.append((row_cur.index, DS_CMN_DEF.SR_PRICE_TYPE_HIGH, row_next.index))
            if tick_for_jump_gap > 1:
                if PRICE.get_new_stock_price_with_tick(row_cur['high'], tick_for_jump_gap) > row_next['low']:
                    continue
            jump_gap_list.append((df.index[index].strftime("%y%m%d"), DS_CMN_DEF.SR_PRICE_TYPE_HIGH, df.index[index + 1].strftime("%y%m%d")))
        elif row_next['high'] < row_cur['low']:
            # import pdb; pdb.set_trace()
# CAUTION: Can't get correct index in this way !!!
# Need to enhance
            # jump_gap_list.append((row_cur.index, DS_CMN_DEF.SR_PRICE_TYPE_LOW, row_next.index))
            if tick_for_jump_gap > 1:
                if PRICE.get_new_stock_price_with_tick(row_cur['low'], tick_for_jump_gap * (-1)) < row_next['high']:
                    continue
            jump_gap_list.append((df.index[index].strftime("%y%m%d"), DS_CMN_DEF.SR_PRICE_TYPE_LOW, df.index[index + 1].strftime("%y%m%d")))
    return jump_gap_list


def print_stock_price_jump_gap(df, jump_gap_list):
    print "Jump Gap:"
    # import pdb; pdb.set_trace()
# To avoid import loop
    from common_class import StockPrice as PRICE
    for jump_gap in jump_gap_list:
        jump_gate_date_cur = jump_gap[0]
        jump_gate_date_cur_index = date2Date(jump_gate_date_cur)
        jump_gate_date_next = jump_gap[2]
        jump_gate_date_next_index = date2Date(jump_gate_date_next)
        if jump_gap[1] == DS_CMN_DEF.SR_PRICE_TYPE_HIGH:
            print "%s[%sH]:%s[%sL]" % (PRICE(df.ix[jump_gate_date_cur_index, 'high']), jump_gap[0], PRICE(df.ix[jump_gate_date_next_index, 'low']), jump_gap[2])
        elif jump_gap[1] == DS_CMN_DEF.SR_PRICE_TYPE_LOW:
            print "%s[%sL]:%s[%sH]" % (PRICE(df.ix[jump_gate_date_cur_index, 'low']), jump_gap[0], PRICE(df.ix[jump_gate_date_next_index, 'high']), jump_gap[2])
        else:
            raise ValueError("Incorrect Support Resistance Price Type: %s" % jump_gap[1])
    print "\n"


def find_stock_price_main_key_supprot_resistance(df):
# Return:
    # A list, element in a list:
    # 0: supoort date string, 1: resistance date string
    min_price = df.ix[0]['low']
    # min_price_H = df.ix[0]['high']
    min_price_date = df.index[0]
    max_price = df.ix[0]['high']
    # max_price_L = df.ix[0]['low']
    max_price_date = df.index[0]

    for index, row in df.iterrows():
# Find the key support
        if row['low'] < min_price:
            min_price = row['low']
            # min_price_H = row['high']
            min_price_date = index
# Find the key resistance 
        if row['high'] > max_price:
            max_price = row['high']
            # max_price_L = row['low']
            max_price_date = index
    return (min_price_date.strftime("%y%m%d"), max_price_date.strftime("%y%m%d"))


def print_key_support_resistance_price(df, key_support_resistance):
    # import pdb; pdb.set_trace()
    # stock_price_statistics_config = DS_CMN_FUNC.parse_stock_price_statistics_config(company_number)

    # df, _ = DS_LD.load_stock_price_history(company_number)
    # key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE, None)
    # if key_support_resistance is None:
    #     print "***** No Key Support Resistance Data !!! *****\n"
    #     return

    print "\n***** Key Support Resistance Price *****"
    for key_date in key_support_resistance:
        key_date_index = DS_CMN_FUNC.date2Date(key_date)
        row = df.ix[key_date_index]
        print "%s O:%s H:%s L:%s C:%s" % (key_date, PRICE(row['open']), PRICE(row['high']), PRICE(row['low']), PRICE(row['close']))
    print "*****************************************\n"

# def print_stock_price_main_key_supprot_resistance(df, main_key_supprot_resistance, ):
#     print "Main Key Support Resistance:"
#     from common_class import StockPrice as PRICE
#     support_date = main_key_supprot_resistance[0]
#     support_date_index = date2Date(support_date)
#     print "S: %s, %s%s" % PRICE(df.ix[support_date_index, 'high'], support_date, DS_CMN_DEF.SR_PRICE_TYPE_HIGH)
#     resistance_date = main_key_supprot_resistance[1]
#     resistance_date_index = date2Date(resistance_date)
#     print "R: %s, %s%s" % PRICE(df.ix[resistance_date_index, 'low'], resistance_date, DS_CMN_DEF.SR_PRICE_TYPE_LOW)
#     print "\n"
