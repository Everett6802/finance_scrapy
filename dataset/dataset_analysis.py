# -*- coding: utf8 -*-

import numpy as np
import pandas as pd

import libs.common as CMN
import common_definition as DS_CMN_DEF
# import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
import dataset_loader as DS_LD
import dataset_visualization as DS_VS
from dataset.common_variable import DatasetVar as DV
g_logger = CMN.LOG.get_logger()


def find_support_resistance(company_number, cur_price=None, show_marked_only=DS_CMN_DEF.DEF_SUPPORT_RESISTANCE_SHOW_MARKED_ONLY, group_size_thres=DS_CMN_DEF.DEF_SUPPORT_RESISTANCE_GROUP_SIZE_THRES):
    # import pdb; pdb.set_trace()
    stock_price_statistics_config = DS_CMN_FUNC.parse_stock_price_statistics_config(company_number)

    # df, column_description_list = DS_LD.load_stock_hybrid([9,], company_number)
    # df.rename(columns={'0903': 'open', '0904': 'high', '0905': 'low', '0906': 'close', '0908': 'volume'}, inplace=True)
    df, column_description_list = DS_LD.load_stock_price_history(company_number)
    # DS_CMN_FUNC.print_dataset_metadata(df, column_description_list)
    if cur_price is None:
        cur_price = df.ix[-1]['close']
        cur_date = df.index[-1].strftime("%y%m%d")
        print "** Date: %s Price: %s **\n" % (cur_date, cur_price) 

    SMA = None
    if DV.CAN_VISUALIZE:
        # import pdb; pdb.set_trace()
        SMA = DS_CMN_FUNC.get_dataset_sma(df, "close")

# Only handle the data after the start date
    start_date = stock_price_statistics_config.get(DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_START_DATE, None)
    if start_date is not None: 
        start_date_index = DS_CMN_FUNC.date2Date(start_date)
        df = df[df.index >= start_date_index]
        if SMA is not None:
            df_len = len(df)
            SMA_len = len(SMA)
            if SMA_len > df_len:
                start_index = SMA_len - df_len
                SMA = SMA[start_index:]

    # import pdb; pdb.set_trace()
    DS_CMN_FUNC.print_stock_price_statistics(
        df, 
        cur_price, 
        stock_price_statistics_config=stock_price_statistics_config, 
        show_stock_price_statistics_fiter=
            {
                "show_marked_only": show_marked_only,
                "group_size_thres": group_size_thres,
            }
        )

    if DV.CAN_VISUALIZE:
        DS_VS.plot_candles(df, title=company_number, volume_bars=True, overlays=[SMA], stock_price_statistics_config=stock_price_statistics_config)
        DS_VS.show_plot()


def find_jump_gap(company_number, start_date=None, tick_for_jump_gap=2):
    df, column_description_list = DS_LD.load_stock_price_history(company_number)
    if start_date is not None: 
        start_date_index = DS_CMN_FUNC.date2Date(start_date)
        df = df[df.index >= start_date_index]

    jump_gap_list = DS_CMN_FUNC.find_stock_price_jump_gap(df, tick_for_jump_gap=tick_for_jump_gap)
    DS_CMN_FUNC.print_stock_price_jump_gap(df, jump_gap_list)

    if DV.CAN_VISUALIZE:
        stock_price_statistics_config = {DS_CMN_DEF.SUPPORT_RESISTANCE_CONF_FIELD_JUMP_GAP: jump_gap_list}
        DS_VS.plot_candles(df, title=company_number, volume_bars=True, stock_price_statistics_config=stock_price_statistics_config)
        DS_VS.show_plot()
