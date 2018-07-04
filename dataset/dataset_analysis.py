# -*- coding: utf8 -*-

import numpy as np
import pandas as pd

import libs.common as CMN
import common_definition as DS_CMN_DEF
# import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
# import common_class as DS_CMN_CLS
from common_class import StockPrice as PRICE
from common_class import HTMLReportBuilder as REPORT
import dataset_loader as DS_LD
import dataset_visualization as DS_VS
from dataset.common_variable import DatasetVar as DV
g_logger = CMN.LOG.get_logger()


def find_support_resistance(company_number, cur_price=None, show_marked_only=DS_CMN_DEF.DEF_SR_SHOW_MARKED_ONLY, group_size_thres=DS_CMN_DEF.DEF_SR_GROUP_SIZE_THRES):
    # import pdb; pdb.set_trace()
    stock_price_statistics_config = DS_CMN_FUNC.parse_stock_price_statistics_config(company_number)
    overwrite_dataset = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_OVERWRITE_DATASET, None)
    save_figure = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE]
    generate_report = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT]
    html_report = None
    if generate_report:
# The report-generation object initialization
        output_folder_path = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH]
        html_report = REPORT(company_number, save_figure, output_folder_path)

    df, column_description_list = DS_LD.load_stock_price_history(company_number, overwrite_stock_price_list=overwrite_dataset)
    # DS_CMN_FUNC.print_dataset_metadata(df, column_description_list)
    dataset_visualization_title = None
    if cur_price is None:
        row = df.ix[-1]
        cur_price = row['close']
        cur_date = df.index[-1].strftime("%y%m%d")
        print "*** %s ***\n" % cur_date
        if html_report: html_report.text("*** %s ***" % cur_date, tail_newline_count=2)
        if save_figure:
            cur_rise_or_fall = df.ix[-1]['rise_or_fall']
            dataset_visualization_title = "%s     %s   O:%s  H:%s  L:%s  C:%s  %s" % (company_number, cur_date, PRICE(row['open']), PRICE(row['high']), PRICE(row['low']), PRICE(row['close']), cur_rise_or_fall)

    SMA = None
    if save_figure:
        # import pdb; pdb.set_trace()
        SMA = DS_CMN_FUNC.get_dataset_sma(df, "close")

# Only handle the data after the start date
    start_date = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_START_DATE, None)
    if start_date is not None: 
        start_date_index = DS_CMN_FUNC.date2Date(start_date)
        df = df[df.index >= start_date_index]
        if SMA is not None:
            df_len = len(df)
            SMA_len = len(SMA)
            if SMA_len > df_len:
                start_index = SMA_len - df_len
                SMA = SMA[start_index:]
# Find the main key support and resistance
    show_main_key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE, DS_CMN_DEF.SHOW_MAIN_KEY_SR_DEFAULT)
    if show_main_key_support_resistance != DS_CMN_DEF.SHOW_MAIN_NO_KEY_SUPPORT_RESISTANCE:
        stock_price_statistics_config[DS_CMN_DEF.SR_CONF_MAIN_KEY_SUPPORT_RESISTANCE] = DS_CMN_FUNC.find_stock_price_main_key_supprot_resistance(df)

    # import pdb; pdb.set_trace()
    DS_CMN_FUNC.print_stock_price_statistics(
        df, 
        cur_price, 
        stock_price_statistics_config=stock_price_statistics_config,
        show_stock_price_statistics_fiter=
            {
                "show_marked_only": show_marked_only,
                "group_size_thres": group_size_thres,
            },
        html_report=html_report,
        )

    if save_figure:
        DS_VS.plot_candles(df, title=dataset_visualization_title, volume_bars=True, overlays=[SMA], stock_price_statistics_config=stock_price_statistics_config)
        filepath = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH] + "/%s.png" % company_number
        DS_VS.save_plot(filepath, len(df))
    if DV.CAN_VISUALIZE:
        DS_VS.show_plot()

    if html_report is not None:
        html_report.flush()


def find_jump_gap(company_number, start_date=None, tick_for_jump_gap=2):
    df, column_description_list = DS_LD.load_stock_price_history(company_number)
    if start_date is not None: 
        start_date_index = DS_CMN_FUNC.date2Date(start_date)
        df = df[df.index >= start_date_index]

    jump_gap_list = DS_CMN_FUNC.find_stock_price_jump_gap(df, tick_for_jump_gap=tick_for_jump_gap)
    DS_CMN_FUNC.print_stock_price_jump_gap(df, jump_gap_list)

    if DV.CAN_VISUALIZE:
        stock_price_statistics_config = {DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP: jump_gap_list}
        DS_VS.plot_candles(df, title=company_number, volume_bars=True, stock_price_statistics_config=stock_price_statistics_config)
        DS_VS.show_plot()


# def lookup_support_resistance_price(company_number):
#     # import pdb; pdb.set_trace()
#     stock_price_statistics_config = DS_CMN_FUNC.parse_stock_price_statistics_config(company_number)

#     df, _ = DS_LD.load_stock_price_history(company_number)
#     key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_KEY_SUPPORT_RESISTANCE, None)
#     if key_support_resistance is None:
#         print "***** No Key Support Resistance Data !!! *****\n"
#         return

#     print "\n***** %s Key Support Resistance Price *****" % company_number
#     for key_date in key_support_resistance:
#         key_date_index = DS_CMN_FUNC.date2Date(key_date)
#         row = df.ix[key_date_index]
#         print "%s O:%s H:%s L:%s C:%s" % (key_date, PRICE(row['open']), PRICE(row['high']), PRICE(row['low']), PRICE(row['close']))
#     print "*********************************************\n"
