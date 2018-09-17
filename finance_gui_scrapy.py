#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
from libs import common as CMN
from libs.common.common_variable import GlobalVar as GV
# from libs import base as BASE
from libs import selenium as SL
g_mgr = None
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    print "--reserve_old\nDescription: Reserve the old destination finance folders if exist\nDefault exmaples: %s, %s\n" % (CMN.DEF.CSV_ROOT_FOLDERPATH, CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web\n"
    print "--finance_folderpath\nDescription: The finance root folder\nDefault: %s\n" % CMN.DEF.CSV_ROOT_FOLDERPATH
    print "--dataset_finance_folderpath\nDescription: Set the finance root folder to the dataset folder\n"
    print "--config_from_file\nDescription: The methods, time_duration_range, company from config: %s\n" % CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME
    print "Scrapy Method:"
    for method_index in range(SL.DEF.SCRAPY_METHOD_LEN):
        print "  %d: %s" % (method_index, SL.DEF.SCRAPY_METHOD_DESCRIPTION[method_index])
    print "  Format 1: Method (ex. 1,3,5)"
    print "  Format 2: Method range (ex. 2-6)"
    print "  Format 3: Method/Method range hybrid (ex. 1,3-4,6)"
    print ""
    print "-c | --company\nDescription: The list of the company code number\nDefault: All company code nubmers\nCaution: Only take effect when config_from_file is NOT set"
    print "  Format1: Company code number (ex. 2347)"
    print "  Format2: Company code number range (ex. 2100-2200)"
    print "  Format3: Company group number (ex. [Gg]12)"
    print "  Format4: Company code number/number range/group hybrid (ex. 2347,2100-2200,G12,2362,g2,1500-1510)"
    print ""
    print "--max_data_count\nDescription: Only scrape the latest N data\n"
    print "--enable_company_not_found_exception\nDescription: Enable the mechanism that the exception is rasied while encoutering the unknown company code number\n"
# Combination argument
    print "--update_company_revenue\nDescription: Update the revenue of specific companies\nCaution: This arugment is equal to the argument combination as below: --method 0 --dataset_finance_folderpath --reserve_old --company xxxx\n"
    print "--update_company_revenue_from_file\nDescription: Update the revenue of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method 0 --dataset_finance_folderpath --reserve_old --config_from_file\n"
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
    param_cfg["reserve_old"] = False
    param_cfg["dry_run"] = False
    param_cfg["dataset_finance_folderpath"] = False
    param_cfg["finance_folderpath"] = None
    param_cfg["config_from_file"] = False
    param_cfg["method"] = None
    param_cfg["company"] = None
    param_cfg["max_data_count"] = None
    param_cfg["enable_company_not_found_exception"] = False
    param_cfg["update_company_revenue"] = None
    param_cfg["update_company_revenue_from_file"] = False


def parse_param():
    argc = len(sys.argv)
    index = 1
    index_offset = None
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        if re.match("(-h|--help)", sys.argv[index]):
            param_cfg["help"] = True
            index_offset = 1
        elif re.match("--reserve_old", sys.argv[index]):
            param_cfg["reserve_old"] = True
            index_offset = 1
        elif re.match("--dry_run", sys.argv[index]):
            param_cfg["dry_run"] = True
            index_offset = 1
        elif re.match("--dataset_finance_folderpath", sys.argv[index]):
            param_cfg["dataset_finance_folderpath"] = True
            index_offset = 1
        elif re.match("--finance_folderpath", sys.argv[index]):
            param_cfg["finance_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--config_from_file", sys.argv[index]):
            param_cfg["config_from_file"] = True
            index_offset = 1
        elif re.match("--method", sys.argv[index]):
            param_cfg["method"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("(-c|--company)", sys.argv[index]):
            param_cfg["company"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--max_data_count", sys.argv[index]):
            param_cfg["max_data_count"] = int(sys.argv[index + 1])
            index_offset = 2
        elif re.match("--enable_company_not_found_exception", sys.argv[index]):
            param_cfg["enable_company_not_found_exception"] = True
            index_offset = 1
        elif re.match("--update_company_revenue_from_file", sys.argv[index]):
            param_cfg["update_company_revenue_from_file"] = True
            index_offset = 1
        elif re.match("--update_company_revenue", sys.argv[index]):
            param_cfg["update_company_revenue"] = sys.argv[index + 1]
            index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset

# Adjust the parameters setting...
    update_company_revenue_company_list = None
    if param_cfg["update_company_revenue_from_file"] and param_cfg["update_company_revenue"] is not None:
        show_warn("The 'update_company_revenue' argument won't take effect since 'update_company_revenue_from_file' is set")
        param_cfg["update_company_revenue"] = None
    if param_cfg["update_company_revenue_from_file"]:
        update_company_revenue_company_list = SL.CONF.GUIScrapyConfigurer.Instance().Company
    elif param_cfg["update_company_revenue"] is not None:
        update_company_revenue_company_list = param_cfg["update_company_revenue"]

    if update_company_revenue_company_list is not None:
        if param_cfg["config_from_file"]:
            show_warn("The 'config_from_file' argument won't take effect since 'update_company_revenue' is set")
            param_cfg["config_from_file"] = False

        if param_cfg["method"] is not None:
            show_warn("The 'method' argument won't take effect since 'update_company_revenue' is set")
        param_cfg["method"] = "%d" % SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX
        if not param_cfg["dataset_finance_folderpath"]:
            show_warn("dataset_finance_folderpath' argument should be TRUE since 'update_company_revenue' is set")
            param_cfg["dataset_finance_folderpath"] = True
        if not param_cfg["reserve_old"]:
            show_warn("reserve_old' argument should be TRUE since 'update_company_revenue' is set")
            param_cfg["reserve_old"] = True
        if not param_cfg["company"]:
            show_warn("company' argument won't take effect since 'update_company_revenue' is set")
        param_cfg["company"] = update_company_revenue_company_list # param_cfg["update_company_revenue"]


def check_param():    
    if param_cfg["config_from_file"]:
        if param_cfg["method"] is not None:
            param_cfg["method"] = None
            show_warn("The 'method' argument is ignored since 'config_from_file' is set")
        if param_cfg["company"] is not None:
            param_cfg["company"] = None
            show_warn("The 'company' argument is ignored since 'config_from_file' is set")

    if param_cfg["dataset_finance_folderpath"]:
        if param_cfg["finance_folderpath"] is not None:
            show_warn("The 'finance_folderpath' argument is invalid since dataset_finance_folderpath is set")


def setup_param():
# Set method/compnay
    if param_cfg["config_from_file"]:
        g_mgr.set_config_from_file()
    else:
# Set method
        method_index_list = None
        if param_cfg["method"] is not None:
            method_index_list = CMN.FUNC.parse_method_str_to_list(param_cfg["method"])
            g_mgr.set_method(method_index_list)
# Set Company
        if param_cfg["company"] is not None:
            g_mgr.set_company(param_cfg["company"])

    g_mgr.reserve_old_finance_folder(param_cfg["reserve_old"])
    g_mgr.enable_dry_run(param_cfg["dry_run"])
    if param_cfg["dataset_finance_folderpath"]:
        g_mgr.set_finance_root_folderpath(update_dataset=param_cfg["dataset_finance_folderpath"])
    elif param_cfg["finance_folderpath"] is not None:
        g_mgr.set_finance_root_folderpath(csv_root_folderpath=param_cfg["finance_folderpath"])


def update_global_variable():
    # import pdb; pdb.set_trace()
    GV.ENABLE_COMPANY_NOT_FOUND_EXCEPTION = param_cfg["enable_company_not_found_exception"]
    GV.GLOBAL_VARIABLE_UPDATED = True


def record_exe_time(action):
    def decorator(func):
        def wrapper(*args, **kwargs):
            time_lapse_msg = u"################### %s ###################" % action
            show_info(time_lapse_msg)
            time_range_start_second = int(time.time())
            result = func(*args, **kwargs)
            time_range_end_second = int(time.time())
            time_lapse_msg = u"######### Time Lapse: %d second(s) #########\n" % (time_range_end_second - time_range_start_second)
            show_info(time_lapse_msg)
            return result
        return wrapper
    return decorator


@record_exe_time("SCRAPE")
def do_scrapy():
    show_info("* Scrape the data from the website......")
    g_mgr.do_scrapy()
    show_info("* Scrape the data from the website...... DONE!!!")
    # if g_mgr.NoScrapyCSVFound:
    #     show_warn("No Web Data while scraping:")
    #     g_mgr.show_no_scrapy()


if __name__ == "__main__":
    # date_obj = CMN.CLS.FinanceDate("2018-08-18")
    # import pdb; pdb.set_trace()
    # month_obj = CMN.CLS.FinanceMonth("2018-08")

# Parse the parameters and apply to manager class
    init_param()
    parse_param()
    update_global_variable()

    update_cfg = {
        "reserve_old_finance_folder": param_cfg["reserve_old"],
        "dry_run_only": param_cfg["dry_run"],
        # "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
        "max_data_count": param_cfg["max_data_count"],
    }
    g_mgr = SL.MGR.GUIScrapyMgr(**update_cfg)

    if param_cfg["help"]:
        show_usage_and_exit()
# Check the parameters for the manager
    check_param()
# # Update the dataset global variables
#     update_global_variable()
# Setup the parameters for the manager
    setup_param()

    # import pdb; pdb.set_trace()
    do_scrapy()
