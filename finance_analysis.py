#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
# from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from libs import common as CMN
from libs.common.common_variable import GlobalVar as GV
from libs import base as BASE
from dataset import dataset_loader as DSL
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    print "-v | --visualize\nDescription: Visualize the graphes while running script on Jupyter Notebook\n"
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
    param_cfg["help"] = False
    param_cfg["visualize"] = False


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
        elif re.match("(-v|--visualize)", sys.argv[index]):
            if not early_parse:
                param_cfg["visualize"] = True
                break
            index_offset = 1
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():    
    pass


def setup_param():
    pass


def find_correlation(df, column_description_list, visualize=True, figsize=None):
    print "*** Time Period ***"
    print "%s - %s" % (df.index[0].strftime("%Y-%m-%d"), df.index[-1].strftime("%Y-%m-%d"))
    print "*** Column Mapping ***"
    for index in range(1, len(column_description_list)):
        print u"%s: %s" % (df.columns[index - 1], column_description_list[index])
    plt.show()
    if param_cfg["visualize"]:
        ax = None
        if figsize is not None:
            plt.figure(figsize=figsize)
            ax = plt.subplot(111)
        sns.heatmap(df.corr(), annot=True, ax=ax)


def find_dataset_correlation0():
    df, column_description_list = DSL.load_market_hybrid([0, 1], {0:[5,], 1:[3,6,9,12,]})
    df['total'] = df.apply(lambda x : x['0103']+x['0106']+x['0109']+x['0112'], axis=1)
    find_correlation(df, column_description_list, param_cfg["visualize"])
    return df, column_description_list

def find_dataset_correlation1():
    df, column_description_list = DSL.load_market_hybrid([0, 2], {0:[5,], 2:[3,4,5,8,9,10,13,14,15]})
    df['diff1'] = df.apply(lambda x : x['0205']-x['0204'], axis=1)
    df['diff2'] = df.apply(lambda x : x['0210']-x['0209'], axis=1)
    df['diff3'] = df.apply(lambda x : x['0215']-x['0214'], axis=1)
    find_correlation(df, column_description_list, param_cfg["visualize"])
    return df, column_description_list

def find_dataset_correlation2():
    df, column_description_list = DSL.load_market_hybrid([0, 3], {0:[5,], 3:[5,6,11,12,17,18]})
    # df['diff1'] = df.apply(lambda x : x['0205']-x['0204'], axis=1)
    # df['diff2'] = df.apply(lambda x : x['0210']-x['0209'], axis=1)
    # df['diff3'] = df.apply(lambda x : x['0215']-x['0214'], axis=1)
    # import pdb; pdb.set_trace()
    find_correlation(df, column_description_list, param_cfg["visualize"])
    return df, column_description_list

def find_dataset_correlation5():
    df, column_description_list = DSL.load_market_hybrid([0, 5], {0:[5,],})
    # df['sum1'] = df.apply(lambda x : x['0501']+x['0507']+x['0513'], axis=1)
    # df['sum2'] = df.apply(lambda x : x['0502']+x['0508']+x['0514'], axis=1)
    # df['sum3'] = df.apply(lambda x : x['0503']+x['0509']+x['0515'], axis=1)
    # df['sum4'] = df.apply(lambda x : x['0504']+x['0510']+x['0516'], axis=1)
    # df['sum5'] = df.apply(lambda x : x['0505']+x['0511']+x['0517'], axis=1)
    # df['sum6'] = df.apply(lambda x : x['0506']+x['0512']+x['0518'], axis=1)

    # df['sum7'] = df.apply(lambda x : x['0519']+x['0525']+x['0531'], axis=1)
    # df['sum8'] = df.apply(lambda x : x['0520']+x['0526']+x['0532'], axis=1)
    # df['sum9'] = df.apply(lambda x : x['0521']+x['0527']+x['0533'], axis=1)
    # df['sum10'] = df.apply(lambda x : x['0522']+x['0528']+x['0534'], axis=1)
    # df['sum11'] = df.apply(lambda x : x['0523']+x['0529']+x['0535'], axis=1)
    # df['sum12'] = df.apply(lambda x : x['0524']+x['0530']+x['0536'], axis=1)
    df['sum1'] = df.apply(lambda x : x['0501']+x['0521'], axis=1)
    df['sum2'] = df.apply(lambda x : x['0502']+x['0522'], axis=1)
    df['sum3'] = df.apply(lambda x : x['0503']+x['0519'], axis=1)
    df['sum4'] = df.apply(lambda x : x['0504']+x['0520'], axis=1)
    # import pdb; pdb.set_trace()
    find_correlation(df, column_description_list, param_cfg["visualize"], (30, 25))
    return df, column_description_list

def find_dataset_correlation6():
    df, column_description_list = DSL.load_market_hybrid([0, 6], {0:[5,], 6:[6,]})
    find_correlation(df, column_description_list, param_cfg["visualize"])
    return df, column_description_list

def find_dataset_correlation7():
    df, column_description_list = DSL.load_market_hybrid([0, 7], {0:[5,], 7:[3,4,7,8,12,13,16,17,]})
    find_correlation(df, column_description_list, param_cfg["visualize"])
    return df, column_description_list


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
    # sys.exit(0)
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

    # df, column_description_list = DSL.load_market_hybrid([0, 1], {0:[5,], 1:[3, 6, 9, 12]})
    # df['total'] = df.apply(lambda x : x['0103']+x['0106']+x['0109']+x['0112'], axis=1)
    # find_correlation(df, column_description_list, param_cfg["visualize"])
    # # # import pdb; pdb.set_trace()
    # # print "*** Time Period ***"
    # # print "%s - %s" % (df.index[0].strftime("%Y-%m-%d"), df.index[-1].strftime("%Y-%m-%d"))
    # # print "*** Column Mapping ***"
    # # for index in range(1, len(column_description_list)):
    # #     print u"%s: %s" % (df.columns[index - 1], column_description_list[index])
    # # # plt.show()
    # # if param_cfg["visualize"]:
    # #     sns.heatmap(df.corr(), annot=True)
    # df, column_description_list = find_dataset_correlation0()
    # df, column_description_list = DSL.load_market_hybrid([0, 1], {0:[5,], 1:[3,6,9,12,]})

    df, column_description_list = DSL.load_stock_hybrid([9,],"2367")
    print "*** Time Period ***"
    print "%s - %s" % (df.index[0].strftime("%Y-%m-%d"), df.index[-1].strftime("%Y-%m-%d"))
    print "*** Column Mapping ***"
    for index in range(1, len(column_description_list)):
        print u"%s: %s" % (df.columns[index - 1], column_description_list[index])
    plt.show()

    # # import pdb; pdb.set_trace()
    # print column_description_list
