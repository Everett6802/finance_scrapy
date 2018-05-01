#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
# from datetime import datetime, timedelta
from libs import common as CMN
from libs.common.common_variable import GlobalVar as GV
from libs import base as BASE
from dataset import dataset_loader as DSL
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "--is_pynb\nDescription: Run script on Jupyter Notebook\n"
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    sys.exit(0)


def show_debug(msg):
    g_logger.debug(msg)
    if not param_cfg["silent"]:
        sys.stdout.write(msg + "\n")
def show_info(msg):
    g_logger.info(msg)
    if not param_cfg["silent"]:
        sys.stdout.write(msg + "\n")
def show_warn(msg):
    g_logger.warn(msg)
    if not param_cfg["silent"]:
        sys.stdout.write(msg + "\n")
def show_error(msg):
    g_logger.error(msg)
    if not param_cfg["silent"]:
        sys.stderr.write(msg + "\n")


def show_error_and_exit(errmsg):
    show_error(errmsg)
    sys.exit(1)


def init_param():
    param_cfg["silent"] = False
    param_cfg["is_pynb"] = False


def parse_param(early_parse=False):
    argc = len(sys.argv)
    index = 1
    index_offset = None
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        if re.match("(-h|--help)", sys.argv[index]):
            if not early_parse:
                param_cfg["help"] = True
            index_offset = 1
        elif re.match("--is_pynb", sys.argv[index]):
            if not early_parse:
                param_cfg["is_pynb"] = True
                break
            index_offset = 1
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():    
    pass


def setup_param():
    pass





import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


if __name__ == "__main__":
    # line_list = CMN.FUNC.read_file_lines_ex("/home/super/source/finance_scrapy/dataset/.180414/market/stock_top3_legal_persons_net_buy_or_sell.csv.bak")
    # new_line_list = []
    # for line in line_list:
    #     line_element_list = line.split(',')
    #     if len(line_element_list) > 13 :
    #         new_line = ",".join(line_element_list[:13])   
    #         new_line_list.append(new_line)
    #     else:
    #         new_line_list.append(line)
    # CMN.FUNC.write_file_lines_ex(new_line_list, "/home/super/source/finance_scrapy/dataset/.180414/market/stock_top3_legal_persons_net_buy_or_sell.csv")
    # df0, _ = DSL.load_raw(0)
    # df3, _ = DSL.load_raw(3)
    # df0 = df0.merge(df3, right_index=True, left_index=True)
    # df, column_description_list = DSL.load_market_hybrid([0, 1])
    df, column_description_list = DSL.load_market_hybrid([0, 1], {0:[5,], 1:[3, 6, 9, 12]})
    # import pdb; pdb.set_trace()
    # # my_test_class = MyTestClass()
    # # print MyTestClass.CUR_DATASET_SELECTION
    # # df = pd.DataFrame([[0,1],[2,3],[4,5]],index=['a','b','c'],columns=[u'可以',u'使用'])
    # df.info()
    # print df.index
    # print df.columns

    sys.exit(0)
# Parse the parameters and apply to manager class
    init_param()
    parse_param(True)
    parse_param()

    if param_cfg["help"]:
        show_usage_and_exit()
    # import pdb; pdb.set_trace()
# Check the parameters for the manager
    check_param()
# Setup the parameters for the manager
    setup_param()
