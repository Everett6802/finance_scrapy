# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import talib
# conda install -c quantopian ta-lib=0.4.9
# https://blog.csdn.net/fortiy/article/details/76531700
import libs.common as CMN
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


def sort_stock_price_statistics_ex(df, cur_price=None, price_range_low_value=None, price_range_low_percentage=None, price_range_high_value=None, price_range_high_percentage=None):
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

    df_copy = df.copy()
    # df_copy.sort_index(ascending=True)
    df_copy.reset_index(inplace=True)
    df_copy.head()
    df_open = (df_copy[['date','open']].copy())
    df_open['type'] = "O"
    df_open.rename(columns={'open': 'price'}, inplace=True)
    df_high = df_copy[['date','high']].copy()
    df_high['type'] = "H"
    df_high.rename(columns={'high': 'price'}, inplace=True)
    df_low = df_copy[['date','low']].copy()
    df_low['type'] = "L"
    df_low.rename(columns={'low': 'price'}, inplace=True)
    df_close = df_copy[['date','close']].copy()
    df_close['type'] = "C"
    df_close.rename(columns={'close': 'price'}, inplace=True)
    df_total_value = pd.concat([df_open, df_high, df_low, df_close])
# Display the price in range
    if price_range_low is not None:
        df_total_value = df_total_value[df_total_value['price'] >= price_range_low]
    if price_range_high is not None:
        df_total_value = df_total_value[df_total_value['price'] <= price_range_high]
        
#     df_total_value.sort_values("price", ascending=True, inplace=True)
    price_statistics = df_total_value.groupby('price')
    return price_statistics


def sort_stock_price_statistics(df, cur_price, price_range_low_percentage=12, price_range_high_percentage=12):
    return sort_stock_price_statistics_ex(df, cur_price, price_range_low_percentage=price_range_low_percentage, price_range_high_percentage=price_range_high_percentage)


def print_stock_price_statistics(df, cur_price=None, price_range_low_percentage=12, price_range_high_percentage=12):
    price_statistics = sort_stock_price_statistics(df, cur_price, price_range_low_percentage=price_range_low_percentage, price_range_high_percentage=price_range_high_percentage)
    cur_price_print = False
    for price, df_data in price_statistics:
        data_str = ",".join([row['date'].strftime("%y%m%d")+row['type'] for index, row in df_data.iterrows()])    
        if not cur_price_print and cur_price < price:
            print "\033[1;31;47m" + "CUR: %f" % cur_price
            cur_price_print = True
        print "\033[1;30;47m" + "Price: %f, Data: %s" % (price,data_str)
