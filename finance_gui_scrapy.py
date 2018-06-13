#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
from libs import common as CMN
# from libs.common.common_variable import GlobalVar as GV
# from libs import base as BASE
from libs import selenium as SL
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
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
    param_cfg["help"] = False


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
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():    
    pass


def setup_param():
    pass


# def update_global_variable():
#     print DV.GLOBAL_VARIABLE_UPDATED
#     DV.CAN_VISUALIZE = param_cfg["visualize"]
#     DV.GLOBAL_VARIABLE_UPDATED = True


# def analyze_stock_and_exit(company_number, cur_price=None, show_marked_only=DS.DEF.DEF_SUPPORT_RESISTANCE_SHOW_MARKED_ONLY, group_size_thres=DS.DEF.DEF_SUPPORT_RESISTANCE_GROUP_SIZE_THRES, ignore_jump_gap=DS.DEF.DEF_SUPPORT_RESISTANCE_IGNORE_JUMP_GAP):
#     # import pdb; pdb.set_trace()
#     DS.AS.analyze_stock(company_number, cur_price)
#     sys.exit(0)



if __name__ == "__main__":
    # import pdb; pdb.set_trace()
# Parse the parameters and apply to manager class
    init_param()
    # parse_param(True)
    parse_param()

    if param_cfg["help"]:
        show_usage_and_exit()
# Check the parameters for the manager
    check_param()
# # Update the dataset global variables
#     update_global_variable()
# Setup the parameters for the manager
    setup_param()

    SL.SLS.scrape_company("2367")
