# -*- coding: utf8 -*-

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
# # Read data from the source file
#     df = pd.read_csv(src_filepath, **kwargs)
#     df_pct_change = df["Close"].pct_change()
#     df["Change%"] = df_pct_change["Close"]
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
