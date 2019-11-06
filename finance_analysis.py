#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
from scrapy import common as CMN
from scrapy.common.common_variable import GlobalVar as GV
from scrapy import libs as LIBS
# from dataset import dataset_loader as DSL
import dataset as DS
from dataset.common_variable import DatasetVar as DV
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    print "-v | --visualize\nDescription: Visualize the graphes while running script on Jupyter Notebook\n"
    print "-c | --company\nDescription: The company to be analyzed"
    print "  Format: Company code number (ex. 2347)"
    print ""
    print "-a | --analyze\nDescription: Analyze the dataset for the follow purpose:"
    print " 0: Find the support and resistance of a company"
    # print " 1: Find the jump gap of a company"
    # print " 2: Find the 3/12 monthly YOY revenue growth of a company"
    print " 1: Check the value investment of a company"
    print "Default: 0\n"
    # print "--show_relation\nDescription: Show the releation with candle stick\nCation: Only take effect when the analyze argument is 2"
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
    param_cfg["company"] = None
    param_cfg["analyze"] = None
    # param_cfg["show_relation"] = False


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
            index_offset = 1
        elif re.match("(-c|--company)", sys.argv[index]):
            if not early_parse:
                param_cfg["company"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("(-a|--analyze)", sys.argv[index]):
            if not early_parse:
                param_cfg["analyze"] = int(sys.argv[index + 1])
            index_offset = 2
        # elif re.match("(--show_relation)", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["show_relation"] = True
        #     index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():    
    if param_cfg["company"] is None:
        param_cfg["company"] = None
        show_error_and_exit("The 'company' argument is NOT set")
    if param_cfg['analyze'] is None:
        param_cfg["analyze"] = DS.DEF.ANALYZE_DATASET_DEFAULT
        g_logger.info("Set the 'analyze' argument to default: %d" % DS.DEF.ANALYZE_DATASET_DEFAULT)
    # if param_cfg['analyze'] not in [DS.DEF.ANALYZE_DATASET_FIND_312_MONTHLY_YOY_REVENUE_GROWTH,]:
    #     if param_cfg['show_relation']:
    #         param_cfg['show_relation'] = False
    #         g_logger.info("Set the 'show_relation' argument to False since the analyze argument is : %d" % param_cfg['analyze'])


def setup_param():
    pass


def update_global_variable():
    # print DV.GLOBAL_VARIABLE_UPDATED
    DV.CAN_VISUALIZE = param_cfg["visualize"]
    DV.GLOBAL_VARIABLE_UPDATED = True


def analyze_and_exit():
    FUNC_PTR_ARRAY = [
        DS.AS.find_support_resistance, 
        # DS.AS.find_jump_gap, 
        # DS.AS.find_312_month_yoy_revenue_growth,
        DS.AS.check_value_investment,
    ]
    kwargs = {
        "company_number": param_cfg["company"],
    }
    (FUNC_PTR_ARRAY[param_cfg["analyze"]])(**kwargs)
    sys.exit(0)


if __name__ == "__main__":
    # # # # DS.AS.find_future_index_amplitude_statistics()
    # # # print CMN.FUNC.get_week_account_day()
    # # # print CMN.FUNC.get_month_account_day()
    # keyday_calendar = LIBS.KC.KeydayCalendar.Instance()
    # # print keyday_calendar.get_first_keyday(keyday_calendar.TFEI_ACCOUNTING_DAY)
    # # print keyday_calendar.get_last_keyday(keyday_calendar.TFEI_ACCOUNTING_DAY)
    # time_start = keyday_calendar.get_first_keyday(keyday_calendar.TFEI_ACCOUNTING_DAY) - 2
    # time_end = keyday_calendar.get_last_keyday(keyday_calendar.TFEI_ACCOUNTING_DAY) + 3
    # for time_cur in LIBS.KC.KeydayNearestIterator(keyday_calendar.TFEI_ACCOUNTING_DAY, time_start, time_end):
    #     print time_cur
    # import pdb; pdb.set_trace()
    # sys.exit(0)

    # import pdb; pdb.set_trace()
# Parse the parameters and apply to manager class
    init_param()
    # parse_param(True)
    parse_param()

    if param_cfg["help"]:
        show_usage_and_exit()
# Check the parameters for the manager
    check_param()
# Update the dataset global variables
    update_global_variable()
# Setup the parameters for the manager
    setup_param()

    analyze_and_exit()
