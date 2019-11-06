# -*- coding: utf8 -*-

import numpy as np
import pandas as pd

from scrapy import common as CMN
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
# Parse the config
    stock_price_statistics_config = DS_CMN_FUNC.parse_stock_price_statistics_config(company_number)
    overwrite_dataset = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_OVERWRITE_DATASET, None)
    save_figure = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_SAVE_FIGURE]
    generate_report = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_GENERATE_REPORT]
    detect_abnormal_volume_enable = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_DETECT_ABNORMAL_VOLUME][DS_CMN_DEF.SR_CONF_DETECT_ABNORMAL_VOLUME_SUB_FIELD_ENABLE]
    detect_abnormal_volume_show_result = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_DETECT_ABNORMAL_VOLUME][DS_CMN_DEF.SR_CONF_DETECT_ABNORMAL_VOLUME_SUB_FIELD_SHOW_RESULT]

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
            # import pdb; pdb.set_trace()
            change = row['change']
            dataset_visualization_title = "%s     %s   O:%s  H:%s  L:%s  C:%s" % (company_number, cur_date, PRICE(row['open']), PRICE(row['high']), PRICE(row['low']), PRICE(row['close']))
            isnan = False
            try:
# To avoid the TypeError exception while the change 
# variable is a minus string
                isnan = np.isnan(change)
            except TypeError:
# Filter the situation when the change is minus
                pass
            if not isnan:
                dataset_visualization_title += "  %s" % change
         
# Detect the abnormal volume
    # volume_over_thres_date_list = None
    # volume_under_thres_date_list = None
    if detect_abnormal_volume_enable:
        # import pdb; pdb.set_trace()
        volume_over_thres_date_list, volume_under_thres_date_list = DS_CMN_FUNC.get_dataset_abnormal_value(df, "volume", timeperiod=20) #, start_detect_date=start_date)
        if detect_abnormal_volume_show_result in [DS_CMN_DEF.SHOW_ABNORMAL_VALUE_RESULT_OVER_THRES_ONLY, DS_CMN_DEF.SHOW_ABNORMAL_VALUE_RESULT_ALL,]:
            stock_price_statistics_config[DS_CMN_DEF.SR_CONF_OVER_THRES_DATE_LIST] = volume_over_thres_date_list
        if detect_abnormal_volume_show_result in [DS_CMN_DEF.SHOW_ABNORMAL_VALUE_RESULT_UNDER_THRES_ONLY, DS_CMN_DEF.SHOW_ABNORMAL_VALUE_RESULT_ALL,]:
            stock_price_statistics_config[DS_CMN_DEF.SR_CONF_UNDER_THRES_DATE_LIST] = volume_under_thres_date_list

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
        # import pdb; pdb.set_trace()
        start_date_timestamp = pd.to_datetime(start_date_index)
# Filter the data of volume_over_thres_date_list
        volume_over_thres_date_list = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_OVER_THRES_DATE_LIST, None)
        if volume_over_thres_date_list is not None:
            over_thres_date_start_index = 0
            for volume_over_thres_date in volume_over_thres_date_list:
                date_timestamp = pd.to_datetime(DS_CMN_FUNC.date2Date(volume_over_thres_date[0]))
                if date_timestamp >= start_date_timestamp:
                    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_OVER_THRES_DATE_LIST] = volume_over_thres_date_list[over_thres_date_start_index:]
                    break
                over_thres_date_start_index += 1
        volume_under_thres_date_list = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_UNDER_THRES_DATE_LIST, None)
        if volume_under_thres_date_list is not None:
            under_thres_date_start_index = 0
            for volume_under_thres_date in volume_under_thres_date_list:
                date_timestamp = pd.to_datetime(DS_CMN_FUNC.date2Date(volume_under_thres_date[0]))
                if date_timestamp >= start_date_timestamp:
                    stock_price_statistics_config[DS_CMN_DEF.SR_CONF_UNDER_THRES_DATE_LIST] = volume_under_thres_date_list[under_thres_date_start_index:]
                    break
                under_thres_date_start_index += 1
    # import pdb; pdb.set_trace()
# Find the main key support and resistance
    show_main_key_support_resistance = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_SHOW_MAIN_KEY_SUPPORT_RESISTANCE, DS_CMN_DEF.DEF_SHOW_MAIN_KEY_SUPPORT_RESISTANCE)
    if show_main_key_support_resistance != DS_CMN_DEF.SHOW_MAIN_NO_KEY_SUPPORT_RESISTANCE:
        main_key_support_resistance_start_date = stock_price_statistics_config.get(DS_CMN_DEF.SR_CONF_FIELD_MAIN_KEY_SUPPORT_RESISTANCE_START_DATE, None)
        stock_price_statistics_config[DS_CMN_DEF.SR_CONF_MAIN_KEY_SUPPORT_RESISTANCE] = DS_CMN_FUNC.find_stock_price_main_key_supprot_resistance(df, main_key_support_resistance_start_date=main_key_support_resistance_start_date)

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
        DS_VS.plot_support_resistance(df, title=dataset_visualization_title, volume_bars=True, overlays=[SMA], stock_price_statistics_config=stock_price_statistics_config)
        filepath = stock_price_statistics_config[DS_CMN_DEF.SR_CONF_FIELD_OUTPUT_FOLDER_PATH] + "/%s.png" % company_number
        DS_VS.save_plot(filepath, len(df))
# Generate the HTML report
    if html_report is not None:
        html_report.flush()
# Show the figure
    if DV.CAN_VISUALIZE:
        DS_VS.show_plot()


def find_jump_gap(company_number, start_date=None, tick_for_jump_gap=2):
    df, column_description_list = DS_LD.load_stock_price_history(company_number)
    if start_date is not None: 
        start_date_index = DS_CMN_FUNC.date2Date(start_date)
        df = df[df.index >= start_date_index]

    jump_gap_list = DS_CMN_FUNC.find_stock_price_jump_gap(df, tick_for_jump_gap=tick_for_jump_gap)
    DS_CMN_FUNC.print_stock_price_jump_gap(df, jump_gap_list)

    if DV.CAN_VISUALIZE:
        stock_price_statistics_config = {DS_CMN_DEF.SR_CONF_FIELD_JUMP_GAP: jump_gap_list}
        DS_VS.plot_support_resistance(df, title=company_number, volume_bars=True, stock_price_statistics_config=stock_price_statistics_config)
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


def find_312_month_yoy_revenue_growth(company_number, show_relation=False):
    df_revenue_growth, _ = DS_LD.load_revenue_history(company_number)
    month_yoy_growth_3 = df_revenue_growth['monthly YOY growth'].rolling(window=3).mean()
    month_yoy_growth_12 = df_revenue_growth['monthly YOY growth'].rolling(window=12).mean()
    '''
    First it calculates f - g and the corresponding signs 
    using np.sign. Applying np.diff reveals all the 
    positions, where the sign changes (e.g. the lines cross). 
    Using np.argwhere gives us the exact indices.
    ''' 
    # import pdb; pdb.set_trace()
    month_yoy_growth_diff = month_yoy_growth_3 - month_yoy_growth_12
# Detect two line crossover
    sign_change = np.nan_to_num(np.diff(np.sign(month_yoy_growth_diff)))
    sign_change_index = np.argwhere(sign_change != 0).flatten() + 1
    sign_change_positive_index = np.argwhere(sign_change > 0).flatten() + 1
    sign_change_negative_index = np.argwhere(sign_change < 0).flatten() + 1
    DS_CMN_FUNC.print_312_month_yoy_revenue_growth(df_revenue_growth, month_yoy_growth_3, month_yoy_growth_12, month_yoy_growth_diff, sign_change_index, sign_change_positive_index, sign_change_negative_index)

    if DV.CAN_VISUALIZE:
        if show_relation:
            df_month_stock_price, _ = DS_LD.load_stock_price_history(company_number, data_time_unit=CMN.DEF.DATA_TIME_UNIT_MOMTH)
            DS_VS.plot_312_month_revenue_growth_and_stock_price_relation(df_month_stock_price, df_revenue_growth, month_yoy_growth_3=month_yoy_growth_3, month_yoy_growth_12=month_yoy_growth_12, sign_change_positive_index=sign_change_positive_index, sign_change_negative_index=sign_change_negative_index, title=company_number)
        else:
            DS_VS.plot_312_month_yoy_revenue_growth(df_revenue_growth, month_yoy_growth_3=month_yoy_growth_3, month_yoy_growth_12=month_yoy_growth_12, sign_change_positive_index=sign_change_positive_index, sign_change_negative_index=sign_change_negative_index, title=company_number)
        DS_VS.show_plot()


def find_investment_entry_point(company_number=None, data_time_unit=CMN.DEF.DATA_TIME_UNIT_DAY):
    df = None
    if company_number is not None:
       df, _ = DS_LD.load_stock_price_history(company_number, data_time_unit=data_time_unit)
    else:
        raise ValueError("Not Implemented Yet !!!")

    df = add_ma(df)
    df = add_kd(df)

    if DV.CAN_VISUALIZE:
        DS_VS.plot_investment_entry_point(df, title=company_number)
        DS_VS.show_plot()


def find_future_index_amplitude_statistics(show_amplitude=True):
    df, _ = DS_LD.load_future_index_history()
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.diff.html
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.shift.html
    # import pdb; pdb.set_trace()
    if show_amplitude:
        df_amplitude = pd.DataFrame(
            {
                'open': df['open'],
                'high': df['high'],
                'low': df['low'],
                'close': df['close'],
                'volume': df['volume (general hours trading)'],
                'amp': df.apply(lambda x : (x['high'] - x['low']), axis=1)
            }
        )
        df_amplitude['prev_close'] = df['close'].shift(periods=1)
        df_amplitude['open_amp'] = df_amplitude.apply(lambda x : abs(x['open'] - x['prev_close']), axis=1)
        # import pdb; pdb.set_trace()
        print "amplitude/volume corr: %f" % df_amplitude['amp'].corr(df_amplitude['volume'])
        print "open amptitude/volume corr: %f" % df_amplitude['open_amp'].corr(df_amplitude['volume'])
        print "open amptitude/amptitude corr: %f" % df_amplitude['open_amp'].corr(df_amplitude['amp'])
        # df_amplitude.sort_values('volume', ascending=False, inplace=True)
        # import pdb; pdb.set_trace()
        df_amplitude_columns = ['open','high','low','close','volume','amp','open_amp']
        for index, row in df_amplitude.nlargest(20, 'volume').iterrows():
            print "%s  %5d  %5d  %5d  %5d  %5d  %3d  %3d" % (index.strftime('%Y-%m-%d'), row['open'], row['high'], row['low'], row['close'], row['volume'], row['amp'], row['open_amp'])
  
    # if DV.CAN_VISUALIZE:
    #     DS_VS.plot_future_index_amplitude_statistics(df_amplitude)
    #     DS_VS.show_plot()


def check_value_investment(company_number):
    # import pdb; pdb.set_trace()
# Load data
# stock price
    df_stock_price, _ = DS_LD.load_stock_price_history(company_number)
    df_stock_price_len = len(df_stock_price)
# dividend
    df_dividend, _ = DS_LD.load_dividend_history(company_number)
    df_dividend_len = len(df_dividend)
# quarterly cashflow statement
    df_quarterly_cashflow_statement, _ = DS_LD.load_quarterly_cashflow_statement_history(company_number)
    df_quarterly_cashflow_statement_len = len(df_quarterly_cashflow_statement)
# yearly cashflow statement
    df_yearly_cashflow_statement, _ = DS_LD.load_yearly_cashflow_statement_history(company_number)
    df_yearly_cashflow_statement_len = len(df_yearly_cashflow_statement)
# revenue
    df_revenue, _ = DS_LD.load_revenue_history(company_number)
    df_revenue_len = len(df_revenue)
# quarterly financial ratio
    df_quarterly_financial_ratio, _ = DS_LD.load_quarterly_financial_ratio_history(company_number)
    df_quarterly_financial_ratio_len = len(df_quarterly_financial_ratio)
# yearly financial ratio
    df_yearly_financial_ratio, _ = DS_LD.load_yearly_financial_ratio_history(company_number)
    df_yearly_financial_ratio_len = len(df_yearly_financial_ratio)
# eps growth rate
    df_eps_growth_rate, _ = DS_LD.load_yearly_financial_ratio_history(company_number)
    df_eps_growth_rate_len = len(df_eps_growth_rate)

    # import pdb; pdb.set_trace()
# stock price
    stock_price_row_index = df_stock_price.index[-1]
    stock_price_row_data = df_stock_price.ix[-1]
    print "%s" % stock_price_row_index.strftime('%Y-%m-%d')
    print "%s   %.2f   %d" % (PRICE(stock_price_row_data["close"]), stock_price_row_data["change"], int(stock_price_row_data["trade volume (share)"]/1000))
    latest_four_quarterly_eps_sum = df_quarterly_financial_ratio["earnings per share"].ix[-4:].sum()
    # import pdb; pdb.set_trace()
    if latest_four_quarterly_eps_sum > 0.000:
        # dividend_row_index = df_dividend.index[-1]
        dividend_row_data = df_dividend.ix[-1]
        # quarterly_financial_ratio_row_index = df_quarterly_financial_ratio.index[-1]
        quarterly_financial_ratio_row_data = df_quarterly_financial_ratio.ix[-1]
        revenue_row_data = df_revenue.ix[-1]
        print "%.2f   %.2f   %.2f   %.2f%%   %.1f%%   %.1f%%   %.2f%%   %.2f   %.2f" % ((100.0 * dividend_row_data["dividend"] / stock_price_row_data["close"]), (stock_price_row_data["close"] / latest_four_quarterly_eps_sum), (stock_price_row_data["close"] / quarterly_financial_ratio_row_data["net asset value per share"]), quarterly_financial_ratio_row_data["debt ratio"], revenue_row_data["monthly MOM growth"], revenue_row_data["monthly YOY growth"], quarterly_financial_ratio_row_data["gross profit margin"], quarterly_financial_ratio_row_data["earnings per share"], latest_four_quarterly_eps_sum)
    print ""
# dividend
    last_row_start_index = -1 * min(8, df_dividend_len)
    print "*** Dividend ***"
    print "cash dividend/stock dividend/dividend"
    for i in range(last_row_start_index, 0):
        row_index = df_dividend.index[i]
        row_data = df_dividend.ix[i]
        print "%s: %.1f   %.1f   %.1f" % (row_index.strftime('%Y'), row_data["cash dividend"], row_data["stock dividend"], row_data["dividend"])
    print ""
# EPS
    last_row_start_index = -1 * min(8, df_yearly_financial_ratio_len)
    print "*** Yearly EPS ***"
    for i in range(last_row_start_index, 0):
        row_index = df_yearly_financial_ratio.index[i]
        row_data = df_yearly_financial_ratio.ix[i]
        print "%s: %.2f" % (row_index.strftime('%Y'), row_data["earnings per share"])
    print ""
    last_row_start_index = -1 * min(8, df_quarterly_financial_ratio_len)
    print "*** Quarterly EPS ***"
    for i in range(last_row_start_index, 0):
        row_index = df_quarterly_financial_ratio.index[i]
        row_data = df_quarterly_financial_ratio.ix[i]
        print "%dq%d: %.2f" % (row_index.year, row_index.quarter, row_data["earnings per share"])
    print ""
# revenue
    # import pdb; pdb.set_trace()
    last_row_start_index = -1 * min(12, df_revenue_len)
    print "*** Revenue ***"
    print "monthly revenue/monthly MOM growth/monthly YOY growth"
    for i in range(last_row_start_index, 0):
        row_index = df_revenue.index[i]
        row_data = df_revenue.ix[i]
        print "%s: %.1f   %.1f%%   %.1f%%" % (row_index.strftime('%Y-%m'), row_data["monthly revenue"], row_data["monthly MOM growth"], row_data["monthly YOY growth"])

    latest_row_index = df_revenue.index[-1]
    latest_row_data = df_revenue.ix[-1]
    latest_row_yoy_growth = latest_row_data["monthly YOY growth"]
    revenue_msg = None
    if latest_row_yoy_growth > 9.99999:
        negative2positive_growth = False
        positive_growth_cnt = 0
        for i in range(0, df_revenue_len):
            index = -1 * (i + 1)
            if df_revenue.ix[index]["monthly YOY growth"] > 9.99999:
                positive_growth_cnt += 1
            else:
                break
        if positive_growth_cnt == 1 and df_revenue_len >= 2:
            if latest_row_data[-2]["monthly YOY growth"] < 0.00000:
                negative2positive_growth = True
        if negative2positive_growth:
            revenue_msg = "\nSwitch to Postive Growth\n"
        elif positive_growth_cnt > 1:
            revenue_msg = "\nConsecutive Postive Growth: %d\n" % positive_growth_cnt
    elif latest_row_yoy_growth < -9.99999:
        positive2negative_growth = False
        negative_growth_cnt = 0
        for i in range(0, df_revenue_len):
            index = -1 * (i + 1)
            if df_revenue.ix[index]["monthly YOY growth"] < -9.99999:
                negative_growth_cnt += 1
            else:
                break
        if negative_growth_cnt == 1 and df_revenue_len >= 2:
            if latest_row_data[-2]["monthly YOY growth"] > 0.00000:
                positive2negative_growth = True
        if positive2negative_growth:
            revenue_msg = "\nSwitch to Negative Growth\n"
        elif negative_growth_cnt > 1:
            revenue_msg = "\nConsecutive Negative Growth: %d\n" % negative_growth_cnt
    if revenue_msg is not None:
        print revenue_msg
    else:
        print ""

# cash flow statement
    # import pdb; pdb.set_trace()
    last_row_start_index = -1 * min(8, df_yearly_cashflow_statement_len)
    print "*** Yearly Cash Flow Statement ***"
    print "cash flow from operating activities/cash flow from investing activities/cash flow from financing activities/net cash flow/free cash flow"
    for i in range(last_row_start_index, 0):
        row_index = df_yearly_cashflow_statement.index[i]
        row_data = df_yearly_cashflow_statement.ix[i]
# row_index.show_stock_price_statistics_fiterime('%Yq%q') doesn't work
        print "%s: %.1f   %.1f   %.1f   %.1f   %.1f" % (row_index.strftime('%Y'), row_data["cash flow from operating activities"], row_data["cash flow from investing activities"], row_data["cash flow from financing activities"],  row_data["net cash flow"], row_data["free cash flow"])
    print ""
    last_row_start_index = -1 * min(8, df_quarterly_cashflow_statement_len)
    print "*** Quarterly Cash Flow Statement ***"
    print "cash flow from operating activities/cash flow from investing activities/cash flow from financing activities/net cash flow/free cash flow"
    for i in range(last_row_start_index, 0):
        row_index = df_quarterly_cashflow_statement.index[i]
        row_data = df_quarterly_cashflow_statement.ix[i]
# row_index.show_stock_price_statistics_fiterime('%Yq%q') doesn't work
        print "%dq%d: %.1f   %.1f   %.1f   %.1f   %.1f" % (row_index.year, row_index.quarter, row_data["cash flow from operating activities"], row_data["cash flow from investing activities"], row_data["cash flow from financing activities"],  row_data["net cash flow"], row_data["free cash flow"])
    print ""

# ROE
    last_row_start_index = -1 * min(8, df_yearly_financial_ratio_len)
    print "*** Yearly ROE ***"
    print "ROE/net profit margin/total assets turnover ratio"
    for i in range(last_row_start_index, 0):
        row_index = df_yearly_financial_ratio.index[i]
        row_data = df_yearly_financial_ratio.ix[i]
        print "%s: %.2f   %.2f%%   %.2f" % (row_index.strftime('%Y'), row_data["return on equity"], row_data["net profit margin"], row_data["total assets turnover ratio"])
    print ""
    last_row_start_index = -1 * min(8, df_quarterly_financial_ratio_len)
    print "*** Quarterly ROE ***"
    print "ROE/net profit margin/total assets turnover ratio"
    for i in range(last_row_start_index, 0):
        row_index = df_quarterly_financial_ratio.index[i]
        row_data = df_quarterly_financial_ratio.ix[i]
        print "%dq%d: %.2f   %.2f%%   %.2f" % (row_index.year, row_index.quarter, row_data["return on equity"], row_data["net profit margin"], row_data["total assets turnover ratio"])
    print ""

# financial statement
    last_row_start_index = -1 * min(8, df_yearly_financial_ratio_len)
    print "*** Yearly Financial Statement ***"
    print "net asset value per share/gross profit margin/operating profit margin/earnings per share/debt ratio"
    for i in range(last_row_start_index, 0):
        row_index = df_yearly_financial_ratio.index[i]
        row_data = df_yearly_financial_ratio.ix[i]
        print "%s: %.2f   %.2f%%   %.2f%%   %.2f   %.2f%%" % (row_index.strftime('%Y'), row_data["net asset value per share"], row_data["gross profit margin"], row_data["operating profit margin"], row_data["earnings per share"], row_data["debt ratio"])
    print ""
    last_row_start_index = -1 * min(8, df_quarterly_financial_ratio_len)
    print "*** Quarterly Financial Statement ***"
    print "net asset value per share/gross profit margin/operating profit margin/earnings per share/debt ratio"
    for i in range(last_row_start_index, 0):
        row_index = df_quarterly_financial_ratio.index[i]
        row_data = df_quarterly_financial_ratio.ix[i]
        print "%dq%d: %.2f   %.2f%%   %.2f%%   %.2f   %.2f%%" % (row_index.year, row_index.quarter, row_data["net asset value per share"], row_data["gross profit margin"], row_data["operating profit margin"], row_data["earnings per share"], row_data["debt ratio"])
    print ""
