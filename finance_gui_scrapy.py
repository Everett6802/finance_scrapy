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
combination_param_cfg = {}

update_dataset_errmsg = None


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    print "--update_csv_field\nDescription: Update the CSV file description\n"
    print "--no_scrapy\nDescription: Don't scrape Web data\n"
    print "--reserve_old\nDescription: Reserve the old destination finance folders if exist\nDefault exmaples: %s, %s\n" % (CMN.DEF.CSV_ROOT_FOLDERPATH, CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web\n"
    print "--finance_folderpath\nDescription: The finance root folder\nDefault: %s\n" % CMN.DEF.CSV_ROOT_FOLDERPATH
    print "--dataset_finance_folderpath\nDescription: Set the finance root folder to the dataset folder\n"
    print "--config_from_file\nDescription: The methods, time_duration_range, company from config: %s\n" % CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME
    print "-m | --method\nDescription: The list of the methods\nDefault: All finance methods\nCaution: Only take effect when config_from_file is NOT set"
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
    print "Combination argument:\n Caution: Exclusive. Only the first combination argument takes effect. Some related arguments may be overwriten"
    print "--update_company_revenue\nDescription: Update the revenue of specific companies\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --company xxxx\n" % SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX
    print "--update_company_revenue_from_file\nDescription: Update the revenue of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --config_from_file\n" % SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX
    print "--update_company_profitability\nDescription: Update the profitability of specific companies\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --company xxxx\n" % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX
    print "--update_company_profitability_from_file\nDescription: Update the profitability of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --config_from_file\n" % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX
    print "--update_company_cashflow_statement\nDescription: Update the cashflow statement of specific companies\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --company xxxx\n" % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX
    print "--update_company_cashflow_statement_from_file\nDescription: Update the cashflow statement of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --config_from_file\n" % SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX
    print "--update_company_dividend\nDescription: Update the dividend of specific companies\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --company xxxx\n" % SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX
    print "--update_company_dividend_from_file\nDescription: Update the dividend of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --config_from_file\n" % SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX
    print "--update_company_institutional_investor_net_buy_sell\nDescription: Update the institutional investor net buy sell of specific companies\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --company xxxx\n" % SL.DEF.SCRAPY_MEMTHOD_INSTITUTIONAL_INESTOR_NET_BUY_SELL_INDEX
    print "--update_company_institutional_investor_net_buy_sell_from_file\nDescription: Update the institutional investor net buy sell of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method %d --dataset_finance_folderpath --reserve_old --config_from_file\n" % SL.DEF.SCRAPY_MEMTHOD_INSTITUTIONAL_INESTOR_NET_BUY_SELL_INDEX
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
    param_cfg["no_scrapy"] = False
    param_cfg["reserve_old"] = False
    param_cfg["dry_run"] = False
    param_cfg["dataset_finance_folderpath"] = False
    param_cfg["finance_folderpath"] = None
    param_cfg["config_from_file"] = False
    param_cfg["update_csv_field"] = False
    param_cfg["method"] = None
    param_cfg["company"] = None
    param_cfg["max_data_count"] = None
    param_cfg["enable_company_not_found_exception"] = False
# combination arguments
    combination_param_cfg["update_dataset_method"] = None
    combination_param_cfg["update_dataset_config_from_file"] = False
    combination_param_cfg["update_dataset_company_list"] = None
    combination_param_cfg['update_company_multiple_dataset'] = False


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
        elif re.match("--no_scrapy", sys.argv[index]):
            param_cfg["no_scrapy"] = True
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
        elif re.match("--update_csv_field", sys.argv[index]):
            param_cfg["update_csv_field"] = True
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
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX)
                combination_param_cfg['update_dataset_config_from_file'] = True
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_revenue_from_file"] = True
            index_offset = 1
        elif re.match("--update_company_revenue", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_REVENUE_INDEX)
                combination_param_cfg['update_dataset_company_list'] = sys.argv[index + 1]
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_revenue"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--update_company_profitability_from_file", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX)
                combination_param_cfg['update_dataset_config_from_file'] = True
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability_from_file"] = True
            index_offset = 1
        elif re.match("--update_company_profitability", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_PROFITABILITY_INDEX)
                combination_param_cfg['update_dataset_company_list'] = sys.argv[index + 1]
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--update_company_cashflow_statement_from_file", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX)
                combination_param_cfg['update_dataset_config_from_file'] = True
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability_from_file"] = True
            index_offset = 1
        elif re.match("--update_company_cashflow_statement", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_CASHFLOW_STATEMENT_INDEX)
                combination_param_cfg['update_dataset_company_list'] = sys.argv[index + 1]
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--update_company_dividend_from_file", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX)
                combination_param_cfg['update_dataset_config_from_file'] = True
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability_from_file"] = True
            index_offset = 1
        elif re.match("--update_company_dividend", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_DIVIDEND_INDEX)
                combination_param_cfg['update_dataset_company_list'] = sys.argv[index + 1]
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--update_company_institutional_investor_net_buy_sell_from_file", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_INSTITUTIONAL_INESTOR_NET_BUY_SELL_INDEX)
                combination_param_cfg['update_dataset_config_from_file'] = True
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability_from_file"] = True
            index_offset = 1
        elif re.match("--update_company_institutional_investor_net_buy_sell", sys.argv[index]):
            if combination_param_cfg['update_dataset_method'] is None:
                combination_param_cfg['update_dataset_method'] = str(SL.DEF.SCRAPY_MEMTHOD_INSTITUTIONAL_INESTOR_NET_BUY_SELL_INDEX)
                combination_param_cfg['update_dataset_company_list'] = sys.argv[index + 1]
            else:
                combination_param_cfg['update_company_multiple_dataset'] = True
            # param_cfg["update_company_profitability"] = sys.argv[index + 1]
            index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():
    if param_cfg['help']:
        show_warn("Show Usage and Exit. Other parameters are ignored")
    if param_cfg['update_csv_field']:
        show_warn("Update CSV field description and Exit. Other parameters are ignored")

# Adjust the parameters setting for combination arguments
    if combination_param_cfg["update_dataset_method"] is not None:
        if combination_param_cfg["update_dataset_config_from_file"]:
            method_index = int(combination_param_cfg["update_dataset_method"])
            if SL.FUNC.is_stock_scrapy_method(method_index):
                combination_param_cfg["update_dataset_company_list"] = SL.CONF.GUIScrapyConfigurer.Instance().Company

        if param_cfg["config_from_file"]:
            show_warn("The 'config_from_file' argument won't take effect since 'combination argument' is set")
        param_cfg["config_from_file"] = combination_param_cfg["update_dataset_config_from_file"]
        if param_cfg["method"] is not None:
            show_warn("The 'method' argument won't take effect since 'combination argument' is set")
        param_cfg["method"] = combination_param_cfg["update_dataset_method"]
        if not param_cfg["dataset_finance_folderpath"]:
            show_warn("dataset_finance_folderpath' argument should be TRUE since 'combination argument' is set")
        param_cfg["dataset_finance_folderpath"] = True
        if not param_cfg["reserve_old"]:
            show_warn("reserve_old' argument should be TRUE since 'combination argument' is set")
        param_cfg["reserve_old"] = True
        if param_cfg["company"] is not None:
            show_warn("company' argument won't take effect since 'combination argument' is set")
        param_cfg["company"] = combination_param_cfg["update_dataset_company_list"]
# Show error message to nofity the user
        if combination_param_cfg["update_company_multiple_dataset"]:
            show_warn("Only the first argument of updating dataset takes effect")
    else:
        if param_cfg["config_from_file"]:
            if param_cfg["method"] is not None:
                param_cfg["method"] = None
                show_warn("The 'method' argument is ignored since 'config_from_file' is set")
            if param_cfg["company"] is not None:
                param_cfg["company"] = None
                show_warn("The 'company' argument is ignored since 'config_from_file' is set")
        else:
            if param_cfg["method"] is None:
                # import pdb; pdb.set_trace()
                param_cfg["method"] = ",".join(map(str, range(SL.DEF.SCRAPY_METHOD_END)))

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


def update_csv_field_and_exit():
    show_info("*** Update the field descriptions to Dataset: %s ***" % g_mgr.FinanceRootFolderPath)
    g_mgr.update_csv_field()
    sys.exit(0)


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


# @record_exe_time("UPDATE_CSV_FIELD")
# def do_csv_field_update():
#     show_info("* Update the CSV field from the website......")
#     g_mgr.update_csv_field()
#     show_info("* Update the CSV field from the website...... DONE!!!")


import dataset as DS

if __name__ == "__main__":
    # # # df, _ = DS.LD.load_stock_price_history("2458", data_time_unit=CMN.DEF.DATA_TIME_UNIT_QUARTER)
    # df, _ = DS.LD.load_revenue_history("2458")
    # # df, _ = DS.LD.load_stock_price_history("2458")
    week_str = '2018W40'
    day_str = '2018-11-14'
    import pdb; pdb.set_trace()
    week_obj = CMN.CLS.FinanceWeek(day_str)
    print week_obj
    print (week_obj + 20)
    print (week_obj - 20)

    sys.exit(0)

# Parse the parameters and apply to manager class
    init_param()
    parse_param()
    update_global_variable()

    update_cfg = {
        "reserve_old_finance_folder": param_cfg["reserve_old"],
        # "dry_run_only": param_cfg["dry_run"],
        # "finance_root_folderpath": CMN.DEF.CSV_ROOT_FOLDERPATH,
        "max_data_count": param_cfg["max_data_count"],
    }
    g_mgr = SL.MGR.GUIScrapyMgr(**update_cfg)
# Check the parameters for the manager
    check_param()
    if param_cfg["help"]:
        show_usage_and_exit()
    # import pdb; pdb.set_trace()
# Setup the parameters for the manager
    setup_param()
    # import pdb; pdb.set_trace()
    if param_cfg["update_csv_field"]:
        update_csv_field_and_exit()

    show_info("*** Update the Dataset: %s ***" % g_mgr.FinanceRootFolderPath)
# Try to scrap the web data
    if not param_cfg["no_scrapy"]:
        do_scrapy()
