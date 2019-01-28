# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

from scrapy import common as CMN
# from libs.common.common_variable import GlobalVar as GV
import common_definition as DS_CMN_DEF
# import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
from common_class import StockPrice as PRICE
from dataset.common_variable import DatasetVar as DV
g_logger = CMN.LOG.get_logger()


"""This cell defineds the plot_support_resistance function"""
"""https://www.quantopian.com/posts/plot-candlestick-charts-in-research"""

def default_candle_stick_color(index, open_price, close_price):
    if open_price[index] > close_price[index]:
        return 'g'
    elif open_price[index] < close_price[index]:
        return 'r'
    return 'w'


def generate_x_tick_label(df, df_time_unit, x_axis=None):
    # import pdb; pdb.set_trace()
    x_tick = None
    x_tick_label = None
    if df_time_unit == CMN.DEF.DATA_TIME_UNIT_DAY:
        cur_month = None
        for index, df_date in enumerate(df.index):
            if cur_month == df_date.month:
                continue
            if x_tick_label is None:
                if x_axis is not None: x_tick = [x_axis[index],]
                x_tick_label = [df_date.strftime("%y%m%d"),]        
            elif df_date.month == 1:
                if x_axis is not None: x_tick.append(x_axis[index])
                x_tick_label.append(df_date.strftime("%y%m"))
            else:
                if x_axis is not None: x_tick.append(x_axis[index])
                x_tick_label.append(df_date.strftime("%m"))
            cur_month = df_date.month
    elif df_time_unit == CMN.DEF.DATA_TIME_UNIT_MONTH:
        for index, df_month in enumerate(df.index):
            if x_tick_label is None:
                if x_axis is not None: x_tick = [x_axis[index],]
                x_tick_label = [df_month.strftime("%y%m"),] 
            elif df_month.month == 1:
                if x_axis is not None: x_tick.append(x_axis[index])
                x_tick_label.append(df_month.strftime("%y%m"))
            else:
                if x_axis is not None: x_tick.append(x_axis[index])
                x_tick_label.append(df_month.strftime("%m"))
    else:
        raise ValueError("UnSupport time unit[%d] while drawing candle stick" % df_time_unit)
    return x_tick, x_tick_label


def draw_candle_stick(fig_ax, df, x_axis=None, df_time_unit=CMN.DEF.DATA_TIME_UNIT_DAY, title=None, candle_colors=None, color_function=None, show_xaxis_tick=True):
    def price_flat(oc):
        return True if oc['open'] == oc['close'] else False
    # import pdb; pdb.set_trace()
    if 'open' not in df.columns:
        raise ValueError("The open field does NOT exist in column: %s" % df.columns)
    if 'close' not in df.columns:
        raise ValueError("The close field does NOT exist in column: %s" % df.columns)
    if 'low' not in df.columns:
        raise ValueError("The low field does NOT exist in column: %s" % df.columns)
    if 'high' not in df.columns:
        raise ValueError("The high field does NOT exist in column: %s" % df.columns)
    open_price = df['open']
    close_price = df['close']
    low_price = df['low']
    high_price = df['high']

    oc = pd.concat([open_price, close_price], axis=1)
    # import pdb; pdb.set_trace()
    oc_flat_flag = oc.apply(price_flat, axis=1) 
    oc_flat = oc[oc_flat_flag]
    oc_min = oc.min(axis=1)
    oc_max = oc.max(axis=1)

    if title is not None:
        fig_ax.set_title(title)
# Set the background color of candle stick
    fig_ax.patch.set_facecolor('black')

    if x_axis is None:
        df_len = len(df)
        x_axis = np.arange(df_len)

    if candle_colors is None:
        color_function = color_function or default_candle_stick_color
        candle_colors = [color_function(i, open_price, close_price) for i in x_axis]
# Draw candle stick
    candles = fig_ax.bar(x_axis, oc_max-oc_min, bottom=oc_min, color=candle_colors, linewidth=0)
    # lines = fig_ax.vlines(x + 0.4, low_price, high_price, color=candle_colors, linewidth=1)
    for index, row in oc_flat.iterrows():
        loc = df.index.get_loc(index)
        fig_ax.plot([x_axis[loc], x_axis[loc] + 0.8], [row['open'], row['close']], color='w')
    lines = fig_ax.vlines(x_axis + 0.4, low_price, high_price, color=candle_colors, linewidth=1)

    fig_ax.grid(color='white', linestyle=':', linewidth=0.5)
    fig_ax.xaxis.grid(True)
    fig_ax.yaxis.grid(True)
    fig_ax.xaxis.set_tick_params(which='major', length=3.0, direction='in', top='off')
# Only mark the xtick of the first day of the month
    if show_xaxis_tick:
        x_tick, x_tick_label = generate_x_tick_label(df, df_time_unit, x_axis)
        plt.xticks(x_tick, x_tick_label, rotation='vertical')
#     plt.xticks(x, [date.strftime(time_format) for date in pricing.index], rotation='vertical')


def draw_revenue_growth(fig_ax, df, month_yoy_growth_3, month_yoy_growth_12, sign_change_positive_index=None, sign_change_negative_index=None, x_axis=None, title=None, show_xaxis_tick=True):
    # import pdb; pdb.set_trace()
    if x_axis is None:
        df_len = len(df)
        x_axis = np.arange(df_len)

    fig_ax.plot(x_axis, month_yoy_growth_3, label='3 Months')
    fig_ax.plot(x_axis, month_yoy_growth_12, label='12 Months')
    if sign_change_positive_index is not None:
        plt.plot(x_axis[sign_change_positive_index], month_yoy_growth_3[sign_change_positive_index], 'ro')
    if sign_change_negative_index is not None:
        plt.plot(x_axis[sign_change_negative_index], month_yoy_growth_3[sign_change_negative_index], 'go')
    if title is not None:
        fig_ax.set_title(title)
# Set the tick on x-axis
    if show_xaxis_tick:
        x_tick, x_tick_label = generate_x_tick_label(df, CMN.DEF.DATA_TIME_UNIT_MONTH, x_axis)
        plt.xticks(x_tick, x_tick_label, rotation='vertical')


def plot_support_resistance_v1(pricing,
                    title=None, 
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
    # if not DV.CAN_VISUALIZE:
    #     g_logger.warn("Can NOT Visualize")
    #     return

    def default_candle_stick_color(index, open_price, close_price, low_price, high_price):
        return 'r' if open_price[index] > close_price[index] else 'g'
    
    color_function = color_function or default_candle_stick_color
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


def plot_support_resistance_v2(
        df_pricing, 
        title=None,
        volume_bars=False,
        color_function=None,
        overlays=None,
        technicals=None,
        technicals_titles=None,
        stock_price_statistics_config=None):
    """ Plots a candlestick chart using quantopian pricing data.
    
    Author: Daniel Treiman
    
    Args:
      df_pricing: A pandas dataframe with columns ['open_price', 'close_price', 'high', 'low', 'volume']
      title: An optional title for the chart
      volume_bars: If True, plots volume bars
      color_function: A function which, given a row index and price series, returns a candle color.
      overlays: A list of additional data series to overlay on top of df_pricing.  Must be the same length as df_pricing.
      technicals: A list of additional data series to display as subplots.
      technicals_titles: A list of titles to display for each technical indicator.
    """
# Parse the config if not None
    # start_date = None
    key_support_resistance = None
    jump_gap = None
    trend_line = None
    main_key_support_resistance = None
    draw_key_support_resistance_date = None
    draw_key_support_resistance_price = None
    over_thres_date_list = None
    under_thres_date_list = None
    if stock_price_statistics_config is not None:
        # start_date = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_START_DATE, None)
        key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE, None)
        jump_gap = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP, None)
        trend_line = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_TREND_LINE, None)
        main_key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_MAIN_KEY_SUPPORT_RESISTANCE, None)
        draw_key_support_resistance_date = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_DRAW_SUPPORT_RESISTANCE_DATE]
        draw_key_support_resistance_price = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_DRAW_SUPPORT_RESISTANCE_PRICE]
        over_thres_date_list = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_OVER_THRES_DATE_LIST, None)
        under_thres_date_list = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_UNDER_THRES_DATE_LIST, None)

    overlays = overlays or []
    technicals = technicals or []
    technicals_titles = technicals_titles or []

    # if start_date is not None:
    #     # import pdb; pdb.set_trace()
    #     start_date_index = DS_CMN_FUNC.date2Date(start_date)
    #     df_pricing = df_pricing[df_pricing.index >= start_date_index]

    open_price = df_pricing['open']
    close_price = df_pricing['close']
    low_price = df_pricing['low']
    high_price = df_pricing['high']
    
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

    df_pricing_len = len(df_pricing)
    x = np.arange(df_pricing_len)

    color_function = color_function or default_candle_stick_color
    candle_colors = [color_function(i, open_price, close_price) for i in x]
# Draw candle stick
    draw_candle_stick(ax1, df_pricing, x_axis=x, df_time_unit=CMN.DEF.DATA_TIME_UNIT_DAY, title=title, candle_colors=candle_colors, color_function=color_function, show_xaxis_tick=True)
# Show the price statistics on the candle stick plot
# Mark the important candle stick
    if key_support_resistance is not None:
        # mark_dates = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE]
        # if type(mark_dates) is not list:
        #     mark_dates = [mark_dates,]
        for mark_data in key_support_resistance:
            # import pdb; pdb.set_trace()
            mark_date = mark_data[:6]
            mark_date_index = DS_CMN_FUNC.date2Date(mark_date)
            mark_type = DS_CMN_DEF.DEF_KEY_SUPPORT_RESISTANCE_MARK if (len(mark_data) == DS_CMN_DEF.DEF_KEY_SUPPORT_RESISTANCE_LEN) else mark_data[6:]
            try:
                loc = df_pricing.index.get_loc(mark_date_index)
                # Create a Rectangle patch
                rect = patches.Rectangle((x[loc]-0.1, low_price[loc]-0.15), 1, high_price[loc]-low_price[loc]+0.3, linewidth=2, edgecolor='y', facecolor='none')
                # Add the patch to the Axes
                ax1.add_patch(rect)
                # import pdb; pdb.set_trace()
                if draw_key_support_resistance_date:
                    row = df_pricing.iloc[loc]
                    is_top = True
                    if loc >= 1:
                        row_prev = df_pricing.iloc[loc - 1]
                        if row['high'] > row_prev['high']:
                            is_top = True
                        elif row['low'] < row_prev['low']:
                            is_top = False
                        else:
                            raise ValueError("Unkown condition !!!")
                    # row_str = "%s O: %s H: %s L: %s C: %s" % (row_date, PRICE(row['open']), PRICE(row['high']), PRICE(row['low']), PRICE(row['close']))
                    y = None
                    verticalalignment = None
                    if is_top:
                        y = row['high'] * 1.015
                        verticalalignment = "bottom"
                    else:
                        y = row['low'] * 0.985
                        verticalalignment = "top"
                    ax1.text(x[loc], y, df_pricing.index[loc].strftime("%y%m%d"), color='yellow', horizontalalignment='center', verticalalignment=verticalalignment, fontsize=10)
                # import pdb; pdb.set_trace()
                if draw_key_support_resistance_price:
                    x_pt = x[-1] + 1
                    if DS_CMN_DEF.SR_PRICE_TYPE_OPEN in mark_type:
                        ax1.text(x_pt, row['open'], "%s" % PRICE(row['open']), color='magenta', verticalalignment='center', fontsize=8)
                    if DS_CMN_DEF.SR_PRICE_TYPE_HIGH in mark_type:
                        ax1.text(x_pt, row['high'], "%s" % PRICE(row['high']), color='magenta', verticalalignment='center', fontsize=8)
                    if DS_CMN_DEF.SR_PRICE_TYPE_LOW in mark_type:
                        ax1.text(x_pt, row['low'], "%s" % PRICE(row['low']), color='magenta', verticalalignment='center', fontsize=8)
                    if DS_CMN_DEF.SR_PRICE_TYPE_CLOSE in mark_type:
                        ax1.text(x_pt, row['close'], "%s" % PRICE(row['close']), color='magenta', verticalalignment='center', fontsize=8)
            except KeyError:
                g_logger.warn("The data on the date[%s] does NOT exsit" % mark_date)
    # ax1.text(x[-1] + 1, 17, "12.34", color='yellow', verticalalignment='center',)
# Mark the jump gap
    if jump_gap is not None:
       for mark_date_range in jump_gap:
            mark_date_cur_index = DS_CMN_FUNC.date2Date(mark_date_range[0])
            mark_date_next_index = DS_CMN_FUNC.date2Date(mark_date_range[2])
            try:
                loc_cur = df_pricing.index.get_loc(mark_date_cur_index)
                loc_next = df_pricing.index.get_loc(mark_date_next_index)
                # Create a Rectangle patch
                if mark_date_range[1] == DS_CMN_DEF.SR_PRICE_TYPE_HIGH:
                    rect = patches.Rectangle((x[loc_cur]-0.1, high_price[loc_cur]-0.15), 1.8, low_price[loc_next]-high_price[loc_cur]+0.3, linewidth=1.5, edgecolor='Magenta', facecolor='none')
                elif mark_date_range[1] == DS_CMN_DEF.SR_PRICE_TYPE_LOW:
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
                loc = df_pricing.index.get_loc(pt_date_index)
                x_pt.append(x[loc])
                if pt[6] == DS_CMN_DEF.SR_PRICE_TYPE_HIGH:
                    y_pt.append(high_price.iloc[loc])
                elif pt[6] == DS_CMN_DEF.SR_PRICE_TYPE_LOW:
                    y_pt.append(low_price.iloc[loc])
                else:
                    raise ValueError("Unkown mark type in trend line: %s" % pt[6])
            last_x_pt = x[-1]
            last_y_pt = get_extended_point_y(x_pt[0], y_pt[0], x_pt[1], y_pt[1], last_x_pt)

            ax1.plot([x_pt[0], last_x_pt], [y_pt[0], last_y_pt])
# Draw main key support and resistance
    if main_key_support_resistance is not None:
        show_main_key_support_resistance = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE]
        if show_main_key_support_resistance in [DS_CMN_DEF.SHOW_MAIN_KEY_SUPPORT_ONLY,DS_CMN_DEF.SHOW_MAIN_KEY_SUPPORT_RESISTANCE_BOTH,]:
            date_index = DS_CMN_FUNC.date2Date(main_key_support_resistance[0])
            stock_price = df_pricing.ix[date_index, 'high']
            ax1.plot([x[0], x[-1] + 1,], [stock_price, stock_price,], color='orange', linewidth=1)
        if show_main_key_support_resistance in [DS_CMN_DEF.SHOW_MAIN_KEY_RESISTANCE_ONLY,DS_CMN_DEF.SHOW_MAIN_KEY_SUPPORT_RESISTANCE_BOTH,]:
            date_index = DS_CMN_FUNC.date2Date(main_key_support_resistance[1])
            stock_price = df_pricing.ix[date_index, 'low']
            ax1.plot([x[0], x[-1] + 1,], [stock_price, stock_price,], color='orange', linewidth=1)

    cur_stock_price = df_pricing.ix[-1, 'close']
    ax1.plot([x[0], x[-1] + 1,], [cur_stock_price, cur_stock_price,], color='gray', linewidth=1)
    # import pdb; pdb.set_trace()
    for overlay in overlays:
        start_index = 0
        # if start_date is not None:
        #     start_index = len(overlay) - df_pricing_len
        ax1.plot(x, overlay[start_index:])

# Keep track of the event date:
# * Volume over the threshold
# * Volume under the thresold
    event_date_list = []
    if over_thres_date_list is not None:
        event_date_list.extend([over_thres_date[0] for over_thres_date in over_thres_date_list])
    if under_thres_date_list is not None:
        event_date_list.extend([under_thres_date[0] for under_thres_date in under_thres_date_list])
# Remove the duplicate dates
    event_date_list = list(set(event_date_list))

    # import pdb; pdb.set_trace()
    event_date_loc_x_list = []
    for event_date in event_date_list:
        event_date_index = DS_CMN_FUNC.date2Date(event_date)
        loc = df_pricing.index.get_loc(event_date_index)
        event_date_loc_x_list.append(loc)
    # import pdb; pdb.set_trace()
    if len(event_date_loc_x_list) != 0:
        x1, x2, y1, y2 = ax1.axis()
        for loc_x in event_date_loc_x_list:
            ax1.text(loc_x, y1, "*", color='yellow', horizontalalignment='center')
    # if draw_key_support_resistance_date:
    #     x1, x2, y1, y2 = ax1.axis()
    #     ax1.axis([x1, x2, y1, y2 + 3])

# Plot volume bars if needed
    if volume_bars:
        ax2 = subplots[1]
        volume = df_pricing['volume']
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

plot_support_resistance = plot_support_resistance_v2


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
        ax.text(2, 2 * line_cnt, data_str, style='italic', bbox={'facecolor':'red', 'alpha':0.5, 'pad':1})
        line_cnt -= 1
    ax.axis([0, 10, 0, 2* price_statistics_len + 1])
    # plt.show()


def plot_312_month_yoy_revenue_growth(
    df, 
    month_yoy_growth_3, 
    month_yoy_growth_12, 
    sign_change_positive_index=None, 
    sign_change_negative_index=None, 
    title=None):
    # import pdb; pdb.set_trace()
    fig, ax = plt.subplots()
    draw_revenue_growth(ax, df, month_yoy_growth_3, month_yoy_growth_12, sign_change_positive_index=sign_change_positive_index, sign_change_negative_index=sign_change_negative_index, title=title, show_xaxis_tick=True)


def plot_312_month_revenue_growth_and_stock_price_relation(
    df_month_pricing, 
    df_revenue_growth,
    month_yoy_growth_3, 
    month_yoy_growth_12, 
    sign_change_positive_index=None, 
    sign_change_negative_index=None,
    title=None,
    color_function=None):
    # import pdb; pdb.set_trace()
    subplot_count = 2
    ratios = np.insert(np.full(subplot_count - 1, 1), 0, 3)
    fig, subplots = plt.subplots(subplot_count, 1, sharex=True, gridspec_kw={'height_ratios': ratios})
    ax_candle_stick = subplots[0]
    ax_revenue_growth = subplots[1]
    # import pdb; pdb.set_trace()
# Draw candle stick
    draw_candle_stick(ax_candle_stick, df_month_pricing, x_axis=np.arange(len(df_month_pricing)), df_time_unit=CMN.DEF.DATA_TIME_UNIT_MONTH, title=title, candle_colors=None, color_function=color_function, show_xaxis_tick=True)
# Draw revenue growth
    diff_len = len(df_revenue_growth) - len(df_month_pricing)
    sign_change_positive_offset_index = filter(lambda x : x >= 0, (sign_change_positive_index - diff_len)) if sign_change_positive_index is not None else None
    sign_change_negative_offset_index = filter(lambda x : x >= 0, (sign_change_negative_index - diff_len)) if sign_change_negative_index is not None else None
    draw_revenue_growth(ax_revenue_growth, df_revenue_growth[diff_len:], month_yoy_growth_3[diff_len:], month_yoy_growth_12[diff_len:], sign_change_positive_index=sign_change_positive_offset_index, sign_change_negative_index=sign_change_negative_offset_index, title='312 revenue growth', show_xaxis_tick=False)


def save_plot(filepath, data_count=0):
    dpi = 100
    if data_count > 0 and data_count > 80:
        dpi = 100 + int(10.0 * data_count / 80)
    plt.savefig(filepath, format='png', dpi=dpi)


def show_plot():
    plt.show()
