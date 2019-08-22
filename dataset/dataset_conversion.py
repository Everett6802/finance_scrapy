# -*- coding: utf8 -*-

import re
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.patches as patches
# import seaborn as sns

from scrapy import common as CMN
# from libs.common.common_variable import GlobalVar as GV
import common_definition as DS_CMN_DEF
# import common_variable as DS_CMN_VAR
import common_function as DS_CMN_FUNC
from dataset.common_variable import DatasetVar as DV
g_logger = CMN.LOG.get_logger()


# data source: 
# http://198.160.148.216/products/vix-index-volatility/vix-options-and-futures/vix-index/vix-historical-data
def dataset_conversion_vix(src_filepath, dst_filepath):
    def transform_date_str(orig_date_str):
        [month_value, day_value, year_value] = map(int, orig_date_str.split("/"))
        return CMN.FUNC.transform_date_str(year_value, month_value, day_value)
#     import pdb; pdb.set_trace()
# Read data from the source file
    kwargs = {
        "names": ["Date", "Open", "High", "Low", "Close",], 
        "skiprows": 1,
        "index_col": "Date",
        "parse_dates": [0,],
        "date_parser": lambda x: transform_date_str(x),
    }
    # import pdb; pdb.set_trace()
# Read data from the source file
    df = pd.read_csv(src_filepath, **kwargs)
    df["Change"] = df["Close"].pct_change()
    df.drop(df.index[[0,]], inplace=True)
    # df["Date New"] = df["Date"].apply(lambda x: transform_date_str(x))
    df["Change%"] = df["Change"].apply(lambda x: "%.2f" % (x * 100.0))
# Write data into the destination file
    df.to_csv(dst_filepath, columns=["Close", "Change%",])


# data source: 
# https://goodinfo.tw/StockInfo/ShowBuySaleChart.asp?STOCK_ID=2458&CHT_CAT=DATE
# 顯示範圍: 一年=> export and copy the data from *.xls to *.csv manually
def dataset_conversion_institutional_investor_net_buy_sell(src_filepath, dst_filepath):
    def transform_date_str(orig_date_str):
        # import pdb; pdb.set_trace()
        mobj = re.match("([\d]{2})'([\d]{2})/([\d]{1,2})", orig_date_str)
        if mobj is None:
            raise RuntimeError("Incorrect date string format: %s" % orig_date_str)
        year_value = int(mobj.group(1)) + 2000
        month_value = int(mobj.group(2))
        day_value = int(mobj.group(3))
        return CMN.FUNC.transform_date_str(year_value, month_value, day_value)
    # import pdb; pdb.set_trace()
    COLUMN_NUM = 19
    column_name_list = ["Date",]
    column_name_list.extend(["col%d" % i for i in range(1, COLUMN_NUM)])
# Read data from the source file
    kwargs = {
        "names": column_name_list, 
        "sep": "\s+",
        "index_col": "Date",
        "parse_dates": [0,],
        "date_parser": lambda x: transform_date_str(x),
    }
    # import pdb; pdb.set_trace()
# Read data from the source file
    df = pd.read_csv(src_filepath, **kwargs)
    df.sort_index(inplace=True)
# Write data into the destination file
    df.to_csv(dst_filepath)
