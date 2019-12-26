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
    print "-c | --company\nDescription: The company list for analysis"
    print "  Format: Company code number (ex. 2347)"
    print ""
    print "-a | --analyze\nDescription: Analyze the dataset for the follow purpose:"
    for index, method_description in enumerate(DS.DEF.ANALYZE_METHOD_DESCRITPION):
        print "  %d: %s" % (index, method_description)
    print " Default: 0\n"
    print "--email_address\nDescription: Email address"
    print "--email_password\nDescription: Email password"
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
    param_cfg["email_address"] = None
    param_cfg["email_password"] = None


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
        elif re.match("--email_address", sys.argv[index]):
            if not early_parse:
                param_cfg["email_address"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--email_password", sys.argv[index]):
            if not early_parse:
                param_cfg["email_password"] = sys.argv[index + 1]
            index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():    
    if param_cfg['analyze'] is None:
        param_cfg["analyze"] = DS.DEF.ANALYZE_METHOD_DEFAULT
        g_logger.info("Set the 'analyze' argument to default: %d" % DS.DEF.ANALYZE_METHOD_DEFAULT)
    if param_cfg["analyze"] in [DS.DEF.ANALYZE_SHOW_VALUE_INVESTMENT_REPORT, DS.DEF.ANALYZE_EMAIL_VALUE_INVESTMENT_REPORT,]:
        if param_cfg["company"] is None:
            show_error_and_exit("The 'company' argument is NOT set")
        if param_cfg["analyze"] == DS.DEF.ANALYZE_EMAIL_VALUE_INVESTMENT_REPORT:
            if param_cfg["email_address"] is None:
                show_error_and_exit("The 'email_address' argument is NOT set")
            if param_cfg["email_password"] is None:
                show_error_and_exit("The 'email_password' argument is NOT set")
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


def analyze_show_value_investment_report(**kwargs):
    company_number_list = kwargs["company"].split(",")
    company_number_list_len = len(company_number_list)
    for index, company_number in enumerate(company_number_list):
        if index != 0:
            print "\n################################################################################\n################################################################################\n\n"
        DS.AS.show_value_investment_report(company_number)


def analyze_email_value_investment_report(**kwargs):
    company_number_list = kwargs["company"].split(",")
    company_number_list_len = len(company_number_list)
# Generate the temporary file about the value investment report
    # import pdb; pdb.set_trace()
    temporary_filepath = None
    for index, company_number in enumerate(company_number_list):
        if index == 0:
            temporary_filepath = DS.AS.write_value_investment_report(company_number)
        else:
            DS.AS.write_value_investment_report(
                company_number, 
                file_attribute="a", 
                splitter="\n--------------------------------------------------------------------------------\n\n"
            )
# Email the report
    # import pdb; pdb.set_trace()
    email_address = kwargs["email_address"]
    email_password = kwargs["email_password"]
    with LIBS.MH.MailHandler(address=email_address, password=email_password) as mail_handler:
        LIBS.MH.MailHandler.parse_value_investment_report(mail_handler, temporary_filepath, )
        mail_handler.Subject = "Stock Daily Report"
        mail_handler.send("html")
    CMN.FUNC.remove_file_if_exist(temporary_filepath)


def analyze_and_exit():
    FUNC_PTR_ARRAY = [
        # DS.AS.find_support_resistance, 
        # DS.AS.find_jump_gap, 
        # DS.AS.find_312_month_yoy_revenue_growth,
        analyze_show_value_investment_report,
        analyze_email_value_investment_report,
    ]
    kwargs = {
        "company": param_cfg["company"],
        "email_address": param_cfg.get("email_address", None),
        "email_password": param_cfg.get("email_password", None),
    }
    (FUNC_PTR_ARRAY[param_cfg["analyze"]])(**kwargs)
    sys.exit(0)


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
# Update the dataset global variables
    update_global_variable()
# Setup the parameters for the manager
    setup_param()

    analyze_and_exit()
