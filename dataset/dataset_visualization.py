# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import seaborn as sns

import libs.common as CMN
# from libs.common.common_variable import GlobalVar as GV
# import common_definition as DS_CMN_DEF
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
    [x_tick, x_tick_lable] = zip(*[(x[index], date.strftime(time_format)) for index, date in enumerate(pricing.index) if date.weekday() == 0])
    plt.xticks(x_tick, x_tick_lable, rotation='vertical')
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
                 mark_dates=None):
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

    color_function = color_function or default_color
    overlays = overlays or []
    technicals = technicals or []
    technicals_titles = technicals_titles or []
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
# Mark the important candle stick
    if mark_dates is not None:
        if type(mark_dates) is not list:
            mark_dates = [mark_dates,]
        for mark_date in mark_dates:
            try:
                loc = pricing.index.get_loc(mark_date)
                # Create a Rectangle patch
                rect = patches.Rectangle((x[loc]-0.1, low_price[loc]-0.15), 1, high_price[loc]-low_price[loc]+0.3, linewidth=2, edgecolor='y', facecolor='none')
                # Add the patch to the Axes
                ax1.add_patch(rect)
            except KeyError:
                g_logger.warn("The data on the date[%s] does NOT exsit" % mark_date)

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
    [x_tick, x_tick_lable] = zip(*[(x[index], date.strftime(time_format)) for index, date in enumerate(pricing.index) if date.weekday() == 0])
    plt.xticks(x_tick, x_tick_lable, rotation='vertical')
#     plt.xticks(x, [date.strftime(time_format) for date in pricing.index], rotation='vertical')
    for overlay in overlays:
        ax1.plot(x, overlay)
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

    # ax.text(3, 8, 'boxed italics text in data coords', style='italic',
    #         bbox={'facecolor':'red', 'alpha':0.5, 'pad':10})
    line_cnt = price_statistics_len
    for price, df_data in price_statistics:
        data_str = ",".join([row['date'].strftime("%y%m%d")+row['type'] for index, row in df_data.iterrows()])    
        # if not cur_price_print and cur_price < price:
        #     print "\033[1;31;47m" + "CUR: %f" % cur_price
        #     cur_price_print = True
        # print "\033[1;30;47m" + "Price: %f, Data: %s" % (price,data_str)
        ax.text(2, 2* line_cnt, data_str, style='italic',
            bbox={'facecolor':'red', 'alpha':0.5, 'pad':1})
        line_cnt -= 1

    ax.axis([0, 10, 0, 2* price_statistics_len + 1])

    # plt.show()
