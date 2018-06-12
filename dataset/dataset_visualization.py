# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

import libs.common as CMN
# from libs.common.common_variable import GlobalVar as GV
import common_definition as DS_CMN_DEF
# import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
from dataset.common_variable import DatasetVar as DV
g_logger = CMN.LOG.get_logger()


"""This cell defineds the plot_candles function"""
"""https://www.quantopian.com/posts/plot-candlestick-charts-in-research"""

def plot_candles_v1(pricing, title=None, 
                    volume_bars=False, 
                    color_function=None, 
                    technicals=None):
    """ Plots a candlestick chart using quantopian pricing data.
    
    Author: Daniel Treiman
    
    Args:
      pricing: A pandas dataframe with columns ['open', 'close', 'high', 'low', 'volume']
      title: An optional title for the chart
      volume_bars: If True, plots volume bars
      color_function: A function which, given a row index and price series, returns a candle color.
      technicals: A list of additional data series to add to the chart.  Must be the same length as pricing.
    """
    if not DV.CAN_VISUALIZE:
        g_logger.warn("Can NOT Visualize")
        return

    def default_color(index, open_price, close_price, low_price, high_price):
        return 'r' if open_price[index] > close_price[index] else 'g'
    
    color_function = color_function or default_color
    technicals = technicals or []
    open_price = pricing['open']
    close_price = pricing['close']
    low_price = pricing['low']
    high_price = pricing['high']
    oc = pd.concat([open_price, close_price], axis=1)
    oc_min = oc.min(axis=1)
    oc_max = oc.max(axis=1)

    if volume_bars:
        fig, (ax1, ax2) = plt.subplots(2, 1, sharex=True, gridspec_kw={'height_ratios': [3,1]})
    else:
        fig, ax1 = plt.subplots(1, 1)
    if title:
        ax1.set_title(title)

    x = np.arange(len(pricing))
    candle_colors = [color_function(i, open_price, close_price, low_price, high_price) for i in x]
    candles = ax1.bar(x, oc_max-oc_min, bottom=oc_min, color=candle_colors, linewidth=0)
    lines = ax1.vlines(x + 0.4, low_price, high_price, color=candle_colors, linewidth=1)
    ax1.xaxis.grid(False)
    ax1.xaxis.set_tick_params(which='major', length=3.0, direction='in', top='off')
    # Assume minute frequency if first two bars are in the same day.
    frequency = 'minute' if (pricing.index[1] - pricing.index[0]).days == 0 else 'day'
    time_format = '%Y-%m-%d'
    if frequency == 'minute':
        time_format = '%H:%M'
    # Set X axis tick labels.
# type(pricing.index): <class 'pandas.tseries.index.DatetimeIndex'>
# type(pricing.index[0]): <class 'pandas.tslib.Timestamp'>
# Only mark the xtick of Monday
    [x_tick, x_tick_label] = zip(*[(x[index], date.strftime(time_format)) for index, date in enumerate(pricing.index) if date.weekday() == 0])
    plt.xticks(x_tick, x_tick_label, rotation='vertical')
#     plt.xticks(x, [date.strftime(time_format) for date in pricing.index], rotation='vertical')
    for indicator in technicals:
        ax1.plot(x, indicator)
    
    if volume_bars:
        volume = pricing['volume']
        volume_scale = None
        scaled_volume = volume
        if volume.max() > 1000000:
            volume_scale = 'M'
            scaled_volume = volume / 1000000
        elif volume.max() > 1000:
            volume_scale = 'K'
            scaled_volume = volume / 1000
        ax2.bar(x, scaled_volume, color=candle_colors)
        volume_title = 'Volume'
        if volume_scale:
            volume_title = 'Volume (%s)' % volume_scale
        ax2.set_title(volume_title)
        ax2.xaxis.grid(False)
# plt.show() SHOULD be called only once
    # plt.show()


def plot_candles_v2(pricing, title=None,
                 volume_bars=False,
                 color_function=None,
                 overlays=None,
                 technicals=None,
                 technicals_titles=None,
                 stock_price_statistics_config=None):
    """ Plots a candlestick chart using quantopian pricing data.
    
    Author: Daniel Treiman
    
    Args:
      pricing: A pandas dataframe with columns ['open_price', 'close_price', 'high', 'low', 'volume']
      title: An optional title for the chart
      volume_bars: If True, plots volume bars
      color_function: A function which, given a row index and price series, returns a candle color.
      overlays: A list of additional data series to overlay on top of pricing.  Must be the same length as pricing.
      technicals: A list of additional data series to display as subplots.
      technicals_titles: A list of titles to display for each technical indicator.
    """
    if not DV.CAN_VISUALIZE:
        g_logger.warn("Can NOT Visualize")
        return

    def default_color(index, open_price, close_price, low, high):
        return 'r' if open_price[index] > close_price[index] else 'g'

# Parse the config if not None
    # start_date = None
    key_support_resistance = None
    jump_gap = None
    trend_line = None
    if stock_price_statistics_config is not None:
        # start_date = stock_price_statistics_config.get(DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_START_DATE, None)
        key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_KEY_SUPPORT_RESISTANCE, None)
        jump_gap = stock_price_statistics_config.get(DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_JUMP_GAP, None)
        trend_line = stock_price_statistics_config.get(DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_TREND_LINE, None)

    color_function = color_function or default_color
    overlays = overlays or []
    technicals = technicals or []
    technicals_titles = technicals_titles or []

    # if start_date is not None:
    #     # import pdb; pdb.set_trace()
    #     start_date_index = DS_CMN_FUNC.date2Date(start_date)
    #     pricing = pricing[pricing.index >= start_date_index]

    open_price = pricing['open']
    close_price = pricing['close']
    low_price = pricing['low']
    high_price = pricing['high']
    oc = pd.concat([open_price, close_price], axis=1)
    oc_min = oc.min(axis=1)
    oc_max = oc.max(axis=1)
    
    subplot_count = 1
    if volume_bars:
        subplot_count = 2
    if technicals:
        subplot_count += len(technicals)
    
    if subplot_count == 1:
        fig, ax1 = plt.subplots(1, 1)
    else:
        ratios = np.insert(np.full(subplot_count - 1, 1), 0, 3)
        fig, subplots = plt.subplots(subplot_count, 1, sharex=True, gridspec_kw={'height_ratios': ratios})
        ax1 = subplots[0]
        
    if title:
        ax1.set_title(title)
    # import pdb; pdb.set_trace()
# Set the background color of candle stick
    ax1.patch.set_facecolor('black')

    pricing_len = len(pricing)
    x = np.arange(pricing_len)

    candle_colors = [color_function(i, open_price, close_price, low_price, high_price) for i in x]
# Draw candle stick
    candles = ax1.bar(x, oc_max-oc_min, bottom=oc_min, color=candle_colors, linewidth=0)
    lines = ax1.vlines(x + 0.4, low_price, high_price, color=candle_colors, linewidth=1)
# Show the price statistics on the candle stick plot
# Mark the important candle stick
    if key_support_resistance is not None:
        # mark_dates = stock_price_statistics_config[DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_KEY_SUPPORT_RESISTANCE]
        # if type(mark_dates) is not list:
        #     mark_dates = [mark_dates,]
        for mark_date in key_support_resistance:
            mark_date_index = DS_CMN_FUNC.date2Date(mark_date)
            try:
                loc = pricing.index.get_loc(mark_date_index)
                # Create a Rectangle patch
                rect = patches.Rectangle((x[loc]-0.1, low_price[loc]-0.15), 1, high_price[loc]-low_price[loc]+0.3, linewidth=2, edgecolor='y', facecolor='none')
                # Add the patch to the Axes
                ax1.add_patch(rect)
            except KeyError:
                g_logger.warn("The data on the date[%s] does NOT exsit" % mark_date)
# Mark the jump gap
    if jump_gap is not None:
       for mark_date_range in jump_gap:
            mark_date_cur_index = DS_CMN_FUNC.date2Date(mark_date_range[0])
            mark_date_next_index = DS_CMN_FUNC.date2Date(mark_date_range[2])
            try:
                loc_cur = pricing.index.get_loc(mark_date_cur_index)
                loc_next = pricing.index.get_loc(mark_date_next_index)
                # Create a Rectangle patch
                if mark_date_range[1] == DS_CMN_DEF.SUPPORT_RESISTANCE_PRICE_TYPE_HIGH:
                    rect = patches.Rectangle((x[loc_cur]-0.1, high_price[loc_cur]-0.15), 1.8, low_price[loc_next]-high_price[loc_cur]+0.3, linewidth=1.5, edgecolor='Magenta', facecolor='none')
                elif mark_date_range[1] == DS_CMN_DEF.SUPPORT_RESISTANCE_PRICE_TYPE_LOW:
                    rect = patches.Rectangle((x[loc_cur]-0.1, high_price[loc_next]-0.15), 1.8, low_price[loc_cur]-high_price[loc_next]+0.3, linewidth=1.5, edgecolor='Magenta', facecolor='none')
                else:
                    raise ValueError("Unkown mark type in jump gap: %s" % jump_gap[1])
                # Add the patch to the Axes
                ax1.add_patch(rect)
            except KeyError:
                g_logger.warn("The data on the date[%s] does NOT exsit" % mark_date)

# Draw Trend line
# Parse the trend line
    # trend_line_segment_list = None
    if trend_line is not None:
        def get_extended_point_y(s_x1, s_y1, s_x2, s_y2, e_x):
            if s_x1 == s_x2:
                raise ValueError("The x[%d] position should NOT be the same" % s_x1)
            slope = float(s_y2 - s_y1) / (s_x2 - s_x1)
            e_y = s_y1 + slope * (e_x - s_x1)
            return e_y
        # import pdb; pdb.set_trace()
        # trend_line_price_list = []
        for line in trend_line:
            line_split = line.split(":")
            if len(line_split) != 2:
                raise ValueError("Incorrect trend line format: %s" % line)
            x_pt = []
            y_pt = []
            for pt in line_split:
                if len(pt) != 7:
                    raise ValueError("Incorrect trend line point format: %s" % pt)
                pt_date_index = DS_CMN_FUNC.date2Date(pt[:6])
                loc = pricing.index.get_loc(pt_date_index)
                x_pt.append(x[loc])
                if pt[6] == DS_CMN_DEF.SUPPORT_RESISTANCE_PRICE_TYPE_HIGH:
                    y_pt.append(high_price.iloc[loc])
                elif pt[6] == DS_CMN_DEF.SUPPORT_RESISTANCE_PRICE_TYPE_LOW:
                    y_pt.append(low_price.iloc[loc])
                else:
                    raise ValueError("Unkown mark type in trend line: %s" % pt[6])
            last_x_pt = x[-1]
            last_y_pt = get_extended_point_y(x_pt[0], y_pt[0], x_pt[1], y_pt[1], last_x_pt)

            ax1.plot([x_pt[0], last_x_pt], [y_pt[0], last_y_pt])

    ax1.grid(color='white', linestyle=':', linewidth=0.5)
    ax1.xaxis.grid(True)
    ax1.yaxis.grid(True)
    ax1.xaxis.set_tick_params(which='major', length=3.0, direction='in', top='off')
    # Assume minute frequency if first two bars are in the same day.
    frequency = 'minute' if (pricing.index[1] - pricing.index[0]).days == 0 else 'day'
    time_format = '%Y-%m-%d'
    if frequency == 'minute':
        time_format = '%H:%M'
    # Set X axis tick labels.
# type(pricing.index): <class 'pandas.tseries.index.DatetimeIndex'>
# type(pricing.index[0]): <class 'pandas.tslib.Timestamp'>
# Only mark the xtick of Monday
    [x_tick, x_tick_label] = zip(*[(x[index], date.strftime(time_format)) for index, date in enumerate(pricing.index) if date.weekday() == 0])
    plt.xticks(x_tick, x_tick_label, rotation='vertical')
#     plt.xticks(x, [date.strftime(time_format) for date in pricing.index], rotation='vertical')
    # import pdb; pdb.set_trace()
    for overlay in overlays:
        start_index = 0
        # if start_date is not None:
        #     start_index = len(overlay) - pricing_len
        ax1.plot(x, overlay[start_index:])
    # Plot volume bars if needed
    if volume_bars:
        ax2 = subplots[1]
        volume = pricing['volume']
        volume_scale = None
        scaled_volume = volume
        if volume.max() > 1000000:
            volume_scale = 'M'
            scaled_volume = volume / 1000000
        elif volume.max() > 1000:
            volume_scale = 'K'
            scaled_volume = volume / 1000
        ax2.bar(x, scaled_volume, color=candle_colors)
        volume_title = 'Volume'
        if volume_scale:
            volume_title = 'Volume (%s)' % volume_scale
        ax2.set_title(volume_title)
        ax2.xaxis.grid(False)
        ax2.yaxis.grid(True)
    # Plot additional technical indicators
    for (i, technical) in enumerate(technicals):
        ax = subplots[i - len(technicals)] # Technical indicator plots are shown last
        ax.plot(x, technical)
        if i < len(technicals_titles):
            ax.set_title(technicals_titles[i])
# plt.show() SHOULD be called only once
    # plt.show()

plot_candles = plot_candles_v2


def plot_stock_price_statistics(df, cur_price, price_range_low_percentage=12, price_range_high_percentage=12):
    price_statistics = DS_CMN_FUNC.sort_stock_price_statistics(df, cur_price, price_range_low_percentage=price_range_low_percentage, price_range_high_percentage=price_range_high_percentage)
    price_statistics_len = len(price_statistics)
    fig = plt.figure(figsize=(50,20))
    # fig.suptitle('bold figure suptitle', fontsize=14, fontweight='bold')

    ax = fig.add_subplot(111)
    # fig.subplots_adjust(top=0.85)
    # ax.set_title('axes title')

    # ax.set_xlabel('xlabel')
    # ax.set_ylabel('ylabel')

    line_cnt = price_statistics_len
    for price, df_data in price_statistics:
        data_str = ",".join([row['date'].strftime("%y%m%d")+row['type'] for index, row in df_data.iterrows()])    
        ax.text(2, 2* line_cnt, data_str, style='italic',
            bbox={'facecolor':'red', 'alpha':0.5, 'pad':1})
        line_cnt -= 1
    ax.axis([0, 10, 0, 2* price_statistics_len + 1])
    # plt.show()


def show_plot():
    plt.show()
