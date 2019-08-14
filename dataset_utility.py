#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import scrapy.common as CMN
from scrapy.common.common_variable import GlobalVar as GV
import dataset as DS

param_cfg = {}
g_logger = CMN.LOG.get_logger()


def show_usage_and_exit():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "--convert_vix\nDescription: Convert VIX history data to the standard dataset format"
    print "--convert_institutional_investor_net_buy_sell\nDescription: Convert Institutional Investor Net Buy Sell history data to the standard dataset format"
    print "  Format: source_filepath destination_filepath (ex. /home/super/Downloadsvixcurrent.csv /home/super/source/finance_dataset/data/market/vix_history.csv)"
    sys.exit(0)


def show_error_and_exit(errmsg):
    sys.stderr.write(errmsg)
    sys.stderr.write("\n")
    g_logger.error(errmsg)
    sys.exit(1)  


def convert_vix_and_exit():
    [src_filepath, dst_filepath] = param_cfg["convert_vix"]
    DS.CV.dataset_conversion_vix(src_filepath, dst_filepath)
    sys.exit(0)


def convert_institutional_investor_net_buy_sell_and_exit():
    [src_filepath, dst_filepath] = param_cfg["convert_institutional_investor_net_buy_sell"]
    DS.CV.dataset_conversion_institutional_investor_net_buy_sell(src_filepath, dst_filepath)
    sys.exit(0)


def init_param():
    # import pdb; pdb.set_trace()
    global param_cfg
    param_cfg["help"] = False
    param_cfg["convert_vix"] = None
    param_cfg["convert_institutional_investor_net_buy_sell"] = None


def parse_param():
    argc = len(sys.argv)
    index = 1
    index_offset = None
    global param_cfg
    param_cfg = {}

    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        # if re.match("(-h|--help)", sys.argv[index]):
        if sys.argv[index] == "--help" or sys.argv[index] == "-h":
            show_usage()
            sys.exit(0)
        elif sys.argv[index] == "--convert_vix":
            param_cfg["convert_vix"] = []
            param_cfg["convert_vix"].append(sys.argv[index + 1])
            param_cfg["convert_vix"].append(sys.argv[index + 2])
            index_offset = 3
        elif sys.argv[index] == "--convert_institutional_investor_net_buy_sell":
            param_cfg["convert_institutional_investor_net_buy_sell"] = []
            param_cfg["convert_institutional_investor_net_buy_sell"].append(sys.argv[index + 1])
            param_cfg["convert_institutional_investor_net_buy_sell"].append(sys.argv[index + 2])
            index_offset = 3
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset
    return param_cfg


def update_global_variable():
    GV.GLOBAL_VARIABLE_UPDATED = True 


if __name__ == "__main__":
    # import pdb; pdb.set_trace()
# Parse the parameters
    init_param()
    parse_param()

    # import pdb; pdb.set_trace()
    update_global_variable()

# Renew the company data
    if param_cfg.get("convert_vix", None) is not None:
        convert_vix_and_exit()
    if param_cfg.get("convert_institutional_investor_net_buy_sell", None) is not None:
        convert_institutional_investor_net_buy_sell_and_exit()
