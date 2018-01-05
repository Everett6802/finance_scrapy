#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
from datetime import datetime, timedelta
from libs import common as CMN
from libs import base as BASE
# g_configurer = None
g_mgr = None
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "--show_command_example\nDescription: Show command example\nCaution: Ignore other parameters when set"
    print "--update_workday_calendar\nDescription: Update the workday calendar only\nCaution: Ignore other parameters when set"
    # print "--market_mode --stock_mode\nDescription: Switch the market/stock mode\nCaution: Read parameters from %s when NOT set" % CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME
    print "--silent\nDescription: Disable print log on console"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "--check_url\nDescription: Check URL of every source type\nCaution: Ignore other parameters when set"
    print "--debug_scrapy_class\nDescription: Debug a specific scrapy class only\nCaution: Ignore other parameters when set"
    scrapy_class_index_list = CMN.FUNC.get_scrapy_class_index_range_list()
    for scrapy_class_index in scrapy_class_index_list:
        print "  %d: %s" % (scrapy_class_index, CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_index])
    print "  Format: Scrapy class index (ex. 1)"
    print "--no_scrapy\nDescription: Don't scrape Web data"
    print "--show_progress\nDescription: Show the progress of scraping Web data\nCaution: Only take effect when the no_scrapy flag is NOT set"
    # print "--no_check\nDescription: Don't check the CSV files after scraping Web data"
    print "--clone\nDescription: Clone the CSV files if no error occurs\nCaution: Only take effect when --check is set"
    print "--reserve_old\nDescription: Reserve the old destination finance folders if exist\nDefault exmaples: %s, %s" % (CMN.DEF.CSV_ROOT_FOLDERPATH, CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web"
    print "--finance_folderpath\nDescription: The finance root folder\nDefault: %s" % CMN.DEF.CSV_ROOT_FOLDERPATH
    # print "--method_from_all_time_range_default_file\nDescription: The finance data source in all time range from file: %s\nCaution: source/source_from_xxx_file/time_duration_range are ignored when set" % (CMN.DEF.MARKET_ALL_TIME_RANGE_CONFIG_FILENAME if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.STOCK_ALL_TIME_RANGE_CONFIG_FILENAME)
    # print "--method_from_today_file\nDescription: The today's finance data source from file\nCaution: source/time_duration_range are ignored when set"
    # print "--method_from_last_file\nDescription: The last finance data source from file\nCaution: source/time_duration_range are ignored when set"
    # print "--method_from_time_range_file\nDescription: The finance data source in time range from file\nCaution: source/time_duration_range are ignored when set"
    print "--config_from_file\nDescription: The methods, time_duration_range, company from config: %s" % CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME
    print "--method\nDescription: The list of the methods\nDefault: All finance methods\nCaution: Only take effect when config_from_file is NOT set"
    method_index_list = CMN.FUNC.get_method_index_range_list()
    for method_index in method_index_list:
        print "  %d: %s" % (method_index, CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index])
    print "  Format 1: Method (ex. 1,3,5)"
    print "  Format 2: Method range (ex. 2-6)"
    print "  Format 3: Method/Method range hybrid (ex. 1,3-4,6)"
    print "--time_today\nDescription: The today's data of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    print "--time_last\nDescription: The last data of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    print "--time_duration_range\nDescription: The data in the time range of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    # print "--time_duration_range_all\nDescription: The data in the all time range of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (,end_time): ,2015-01-01"
    print "  Format 3 (start_time,end_time): 2015-01-01,2015-09-04"
    print "--time_today --time_last --time_duration_range\nCaution: Shuold NOT be set simultaneously. Will select the first one"
    if CMN.DEF.IS_FINANCE_STOCK_MODE:
        # print "--company_list_in_default_folderpath\nDescription: Show the company number list in the default finance folder"
        # print "--company_list_in_folderpath\nDescription: Show the company number list in the finance folder" 
        # print "--company_from_file\nDescription: The company code number from file\nDefault: All company code nubmers\nCaution: company is ignored when set"
        print "-c --company\nDescription: The list of the company code number\nDefault: All company code nubmers\nCaution: Only take effect when config_from_file is NOT set"
        print "  Format1: Company code number (ex. 2347)"
        print "  Format2: Company code number range (ex. 2100-2200)"
        print "  Format3: Company group number (ex. [Gg]12)"
        print "  Format4: Company code number/number range/group hybrid (ex. 2347,2100-2200,G12,2362,g2,1500-1510)"
        print "--multi_thread\nDescription: Scrape web data in multi-thread"
        print "  Format: multi-thread number (ex. 4)"
        print "--renew_statement_field\nDescription: Renew the statment field\nCaution: Exit after renewing the statement field"
        print "--enable_company_not_found_exception\nDescription: Enable the mechanism that the exception is rasied while encoutering the unknown company code number"
    print "--merge_finance_folderpath_src_list\nDescription: The list of source folderpaths to be merged\nCaution: The CSV file in different finance folder can NOT be duplicate. If so, the merge progress aborts"
    print "  Format 1 (folderpath): /var/tmp/finance"
    print "  Format 2 (folderpath1,folderpath2,folderpath3): /var/tmp/finance1,/var/tmp/finance2,/var/tmp/finance3"
    print "--merge_finance_folderpath_dst\nDescription: The destination folderpath after merging\nDefault: %s" % CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH
    # print "--run_daily\nDescription: Run daily web-scrapy\nCaution: Ignore other parameters when set"
    print "============================================================="
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


def snapshot_result(run_result_str):
    if not os.path.exists(CMN.DEF.SNAPSHOT_FOLDER):
        os.makedirs(CMN.DEF.SNAPSHOT_FOLDER)
    with open(CMN.DEF.RUN_RESULT_FILENAME, 'w') as fp:
        fp.write(run_result_str.encode('utf8'))
    datetime_now = datetime.today()
    snapshot_filename = CMN.DEF.SNAPSHOT_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
# -v is for verbose. If you don't use it then it won't display
    # subprocess.call(["tar", "cvzf", snapshot_filename, CMN.DEF.RUN_RESULT_FILENAME, g_mgr.FinanceRootFolderPath, CMN.WSL.LOG_FILE_PATH])
    subprocess.call(["tar", "czf", snapshot_filename, CMN.DEF.RUN_RESULT_FILENAME, g_mgr.FinanceRootFolderPath, CMN.WSL.LOG_FILE_PATH])
    subprocess.call(["mv", snapshot_filename, CMN.DEF.SNAPSHOT_FOLDER])
    subprocess.call(["rm", CMN.DEF.RUN_RESULT_FILENAME])


def update_workday_calendar_and_exit():
    workday_calendar = BASE.WC.WorkdayCanlendar.Instance()
    sys.exit(0)


def show_command_example_and_exit():
    project_folderpath = CMN.FUNC.get_project_folderpath()
    # print project_folderpath
    project_config_folderpath = "%s/%s" % (project_folderpath, CMN.DEF.CONF_FOLDER)
    os.chdir(project_config_folderpath)
    cmd = "cat %s" % CMN.DEF.COMMAND_EXAMPLE_FILENAME
    p = subprocess.Popen(cmd, shell=True)
    os.waitpid(p.pid, 0)
    sys.exit(0)


def check_url_and_exit():
    scrapy_class_index_list = CMN.FUNC.get_scrapy_class_index_range_list()
    error_found = False
    errmsg = "**************** Check %s URL ****************\n" % CMN.DEF.FINANCE_MODE_DESCRIPTION[CMN.DEF.FINANCE_MODE]
    for scrapy_class_index in scrapy_class_index_list:
        try:
            g_mgr.do_scrapy_debug(scrapy_class_index, True)
        except Exception as e:
            error_found = True
            errmsg += " %d: %s %s" % (scrapy_class_index, CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_index], str(e))
    if error_found:
        show_error_and_exit(errmsg)
    sys.exit(0)


def debug_scrapy_class_and_exit(scrapy_class_index):
    if not CMN.FUNC.check_scrapy_class_index_in_range(scrapy_class_index):
        errmsg = "Unsupported scrapy class index: %d" % scrapy_class_index
        show_error_and_exit(errmsg)
    g_mgr.do_scrapy_debug(scrapy_class_index)
    sys.exit(0)


def show_company_list_in_folerpath_and_exit():
    if CMN.DEF.IS_FINANCE_MARKET_MODE:
        raise ValueError("Not Support in Market mode")
    g_mgr.set_finance_root_folderpath(param_cfg["company_list_in_folderpath"])
    g_mgr.show_company_list_in_finance_folder()
    sys.exit(0)


def merge_finance_folder_and_exit(merge_finance_folderpath_src_list, merge_finance_folderpath_dst):
    show_info("Merge several finance folders to: %s" % merge_finance_folderpath_dst)
    merge_finance_folderpath_src_list = param_cfg["merge_finance_folderpath_src_list"].split(",")
    g_mgr.merge_finance_folder(merge_finance_folderpath_src_list, merge_finance_folderpath_dst)
    sys.exit(0)


@CMN.FUNC.deprecated("This function is deprecated")
def renew_statement_field_and_exit():
    show_info("Renew the field of the statement")
    g_mgr.renew_statement_field()
    sys.exit(0)


# def switch_mode(mode):
# # MARKET: 0
# # STOCK: 1
#     project_folderpath = CMN.FUNC.get_project_folderpath()
#     # print project_folderpath
#     project_config_folderpath = "%s/%s" % (project_folderpath, CMN.DEF.CONF_FOLDER)
#     os.chdir(project_config_folderpath)
#     cmd = None
#     if mode == CMN.DEF.FINANCE_MODE_MARKET:
#         cmd = "sed -i s/%d/%d/g %s" % (CMN.DEF.FINANCE_MODE_STOCK, CMN.DEF.FINANCE_MODE_MARKET, CMN.DEF.MARKET_STOCK_SWITCH_CONF_FILENAME)
#     elif mode == CMN.DEF.FINANCE_MODE_STOCK:
#         cmd = "sed -i s/%d/%d/g %s" % (CMN.DEF.FINANCE_MODE_MARKET, CMN.DEF.FINANCE_MODE_STOCK, CMN.DEF.MARKET_STOCK_SWITCH_CONF_FILENAME)
#     else:
#         raise ValueError("Unknown mode: %d", mode)
#     p = subprocess.Popen(cmd, shell=True)
#     os.waitpid(p.pid, 0)


def init_param():
    # import pdb; pdb.set_trace()
    param_cfg["update_workday_calendar"] = False
    param_cfg["show_command_example"] = False
    # param_cfg["finance_mode"] = None
    param_cfg["help"] = False
    param_cfg["check_url"] = False
    param_cfg["debug_scrapy_class"] = None
    param_cfg["silent"] = False
    param_cfg["no_scrapy"] = False
    param_cfg["show_progress"] = False
    # param_cfg["no_check"] = False
    param_cfg["clone"] = False
    param_cfg["reserve_old"] = False
    param_cfg["dry_run"] = False
    param_cfg["finance_folderpath"] = None
    # param_cfg['method_from_all_time_range_default_file'] = False
    param_cfg["config_from_file"] = False
    param_cfg["method"] = None
    param_cfg["time_duration_type"] = None # Should be check in check_param()
    param_cfg["time_duration_range"] = None
    # param_cfg["company_list_in_default_folderpath"] = False
    # param_cfg["company_list_in_folderpath"] = None
    param_cfg["company"] = None
    # param_cfg["company_from_file"] = None
    param_cfg["renew_statement_field"] = False
    param_cfg["enable_company_not_found_exception"] = False
    param_cfg["multi_thread"] = None
    param_cfg["merge_finance_folderpath_src_list"] = None
    param_cfg["merge_finance_folderpath_dst"] = None


def parse_param(early_parse=False):
    argc = len(sys.argv)
    index = 1
    index_offset = None
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        if re.match("--show_command_example", sys.argv[index]):
            param_cfg["show_command_example"] = True
            return
        elif re.match("--update_workday_calendar", sys.argv[index]):
            param_cfg["update_workday_calendar"] = True
            index_offset = 1
        # elif re.match("--market_mode", sys.argv[index]):
        #     if early_parse:
        #         if param_cfg["finance_mode"] is not None:
        #             raise ValueError("The finance mode has already been set to: %s" % CMN.DEF.FINANCE_MODE_DESCRIPTION[param_cfg["finance_mode"]])
        #         param_cfg["finance_mode"] = CMN.DEF.FINANCE_MODE_MARKET
        #     index_offset = 1
        # elif re.match("--stock_mode", sys.argv[index]):
        #     if early_parse:
        #         if param_cfg["finance_mode"] is not None:
        #             raise ValueError("The finance mode has already been set to: %s" % CMN.DEF.FINANCE_MODE_DESCRIPTION[param_cfg["finance_mode"]])
        #         param_cfg["finance_mode"] = CMN.DEF.FINANCE_MODE_STOCK
        #     index_offset = 1
        elif re.match("--silent", sys.argv[index]):
            if early_parse:
                param_cfg["silent"] = True
                CMN.DEF.CAN_PRINT_CONSOLE = not param_cfg["silent"]
            index_offset = 1
        elif re.match("(-h|--help)", sys.argv[index]):
            if not early_parse:
                param_cfg["help"] = True
            index_offset = 1
        elif re.match("--check_url", sys.argv[index]):
            if not early_parse:
                param_cfg["check_url"] = True
            index_offset = 1 
        elif re.match("--debug_scrapy_class", sys.argv[index]):
            if not early_parse:
                param_cfg["debug_scrapy_class"] = int(sys.argv[index + 1])
            index_offset = 2 
        elif re.match("--no_scrapy", sys.argv[index]):
            if not early_parse:
                param_cfg["no_scrapy"] = True
            index_offset = 1
        elif re.match("--show_progress", sys.argv[index]):
            if not early_parse:
                param_cfg["show_progress"] = True
            index_offset = 1
        # elif re.match("--no_check", sys.argv[index]):
        #     param_cfg["no_check"] = True
        #     index_offset = 1
        elif re.match("--clone", sys.argv[index]):
            if not early_parse:
                param_cfg["clone"] = True
            index_offset = 1
        elif re.match("--reserve_old", sys.argv[index]):
            if not early_parse:
                param_cfg["reserve_old"] = True
            index_offset = 1
        elif re.match("--dry_run", sys.argv[index]):
            if not early_parse:
                param_cfg["dry_run"] = True
            index_offset = 1
        elif re.match("--finance_folderpath", sys.argv[index]):
            if not early_parse:
                param_cfg["finance_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        # elif re.match("--method_from_all_time_range_default_file", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["method_from_all_time_range_default_file"] = True
        #         param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_RANGE
        #     index_offset = 1
        # elif re.match("--method_from_today_file", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["config_from_file"] = sys.argv[index + 1]
        #         param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
        #     index_offset = 2
        # elif re.match("--method_from_last_file", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["config_from_file"] = sys.argv[index + 1]
        #         param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST
        #     index_offset = 2
        # elif re.match("--method_from_time_range_file", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["config_from_file"] = sys.argv[index + 1]
        #         param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
        #     index_offset = 2
        elif re.match("--config_from_file", sys.argv[index]):
            if not early_parse:
                param_cfg["config_from_file"] = True
            index_offset = 1
        elif re.match("--method", sys.argv[index]):
            if not early_parse:
                param_cfg["method"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--time_today", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_today attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
            index_offset = 1
        elif re.match("--time_last", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_last attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST
            index_offset = 1
        # elif re.match("--time_duration_range_all", sys.argv[index]):
        #     if not early_parse:
        #         if param_cfg["time_duration_type"] is not None:
        #             g_logger.debug("Time duration has already been set to: %d, ignore the time_duration_range_all attribute...", param_cfg["time_duration_type"])
        #         else:
        #             param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
        #     index_offset = 1
        elif re.match("--time_duration_range", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_duration_range attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
                    param_cfg["time_duration_range"] = sys.argv[index + 1]
                # g_logger.debug("Param time range: %s", param_cfg["time_duration_range"])
            index_offset = 2
        # elif re.match("--company_list_in_default_folderpath", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["company_list_in_default_folderpath"] = True
        #     index_offset = 1
        # elif re.match("--company_list_in_folderpath", sys.argv[index]):
        #     if not early_parse:
        #         param_cfg["company_list_in_folderpath"] = sys.argv[index + 1]
        #     index_offset = 2
        # elif re.match("--company_from_file", sys.argv[index]):
        #     if not early_parse:
        #         if CMN.DEF.IS_FINANCE_MARKET_MODE:
        #             g_logger.warn("The company_from_file arguemnt is ignored in the Market mode")
        #         else:
        #             param_cfg["company_from_file"] = sys.argv[index + 1]
        #     index_offset = 2
        elif re.match("(-c|--company)", sys.argv[index]):
            if not early_parse:
                if CMN.DEF.IS_FINANCE_MARKET_MODE:
                    g_logger.warn("The company arguemnt is ignored in the Market mode")
                else:
                    param_cfg["company"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--renew_statement_field", sys.argv[index]):
            if not early_parse:
                param_cfg["renew_statement_field"] = True
            index_offset = 1
        elif re.match("--enable_company_not_found_exception", sys.argv[index]):
            if not early_parse:
                param_cfg["enable_company_not_found_exception"] = True
            index_offset = 1
        elif re.match("--multi_thread", sys.argv[index]):
            if not early_parse:
                param_cfg["multi_thread"] = int(sys.argv[index + 1])
            index_offset = 2
        elif re.match("--merge_finance_folderpath_src_list", sys.argv[index]):
            if not early_parse:
                param_cfg["merge_finance_folderpath_src_list"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--merge_finance_folderpath_dst", sys.argv[index]):
            if not early_parse:
                param_cfg["merge_finance_folderpath_dst"] = sys.argv[index + 1]
            index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():
    # if param_cfg["method_from_all_time_range_default_file"]:
    #     if param_cfg["config_from_file"] is not None:
    #         show_warn("The 'config_from_file' argument is ignored since 'method_from_all_time_range_default_file' is set")
    #     if CMN.DEF.IS_FINANCE_MARKET_MODE:
    #         param_cfg["config_from_file"] = CMN.DEF.MARKET_ALL_TIME_RANGE_CONFIG_FILENAME
    #     elif CMN.DEF.IS_FINANCE_STOCK_MODE:
    #         param_cfg["config_from_file"] = CMN.DEF.STOCK_ALL_TIME_RANGE_CONFIG_FILENAME
    if param_cfg["config_from_file"] is not False:
        if param_cfg["method"] is not None:
            param_cfg["method"] = None
            show_warn("The 'method' argument is ignored since 'config_from_file' is set")
        if param_cfg["time_duration_type"] is None:
            param_cfg["time_duration_type"] = None
            show_warn("The 'time_duration_type' argument is ignored since 'config_from_file' is set")
        if param_cfg["time_duration_range"] is None:
            param_cfg["time_duration_range"] = None
            show_warn("The 'time_duration_range' argument is ignored since 'config_from_file' is set")
        if param_cfg["company"] is None:
            param_cfg["company"] = None
            show_warn("The 'company' argument is ignored since it's 'Market' mode")
    else:
        if param_cfg["time_duration_type"] is None:
            param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
            show_warn("Set the 'time_duration_type' argument to DATA_TIME_DURATION_TODAY as default")
        if param_cfg["time_duration_type"] != CMN.DEF.DATA_TIME_DURATION_RANGE:
            param_cfg["time_duration_range"] = None
            show_warn("The 'time_duration_range' argument is ignored since 'time_duration_type' is NOT 'DATA_TIME_DURATION_TODAY'")
        if CMN.DEF.IS_FINANCE_MARKET_MODE:
            if param_cfg["company"] is not None:
                param_cfg["company"] = None
                show_warn("The 'company' argument is ignored since it's 'Market' mode")
    if CMN.DEF.IS_FINANCE_MARKET_MODE:
        # if param_cfg["company_list_in_default_folderpath"]:
        #     param_cfg["company_list_in_default_folderpath"] = False
        #     show_warn("The 'company_list_in_default_folderpath' argument is ignored since it's 'Market' mode")
        # if param_cfg["company_list_in_folderpath"] is not None:
        #     param_cfg["company_list_in_folderpath"] = None
        #     show_warn("The 'company_list_in_folderpath' argument is ignored since it's 'Market' mode")
        # if param_cfg["company"] is not None:
        #     param_cfg["company"] = None
        #     show_warn("The 'company' argument is ignored since it's 'Market' mode")
        # if param_cfg["company_from_file"] is not None:
        #     param_cfg["company_from_file"] = None
        #     show_warn("The 'company_from_file' argument is ignored since it's 'Market' mode")
        if param_cfg["renew_statement_field"]:
            param_cfg["renew_statement_field"] = False
            show_warn("The 'renew_statement_field' argument is ignored since it's 'Market' mode")
        if param_cfg["enable_company_not_found_exception"]:
            param_cfg["enable_company_not_found_exception"] = False
            show_warn("The 'enable_company_not_found_exception' argument is ignored since it's 'Market' mode")
        if param_cfg["multi_thread"] is not None:
            param_cfg["multi_thread"] = None
            show_warn("The 'multi_thread' argument is invalid since it's 'Market' mode")
    else:
        # if param_cfg["company_list_in_default_folderpath"]:
        #     if param_cfg["company_list_in_folderpath"] is not None:
        #         show_warn("The 'company_list_in_folderpath' argument is ignored since 'company_list_in_default_folderpath' is set")
        #     else:
        #         param_cfg["company_list_in_folderpath"] = CMN.DEF.CSV_ROOT_FOLDERPATH
        # if param_cfg["company_from_file"] is not None:
        #     if param_cfg["company"] is not None:
        #         param_cfg["company"] = None
        #         show_warn("The 'company' argument is ignored since 'company_from_file' is set")
        if param_cfg["multi_thread"] is not None:
            if param_cfg["renew_statement_field"]:
                param_cfg["multi_thread"] = None
                show_warn("The 'multi_thread' argument is invalid when the 'renew_statement_field' argument is true")

    if param_cfg["merge_finance_folderpath_src_list"] is not None and param_cfg["merge_finance_folderpath_dst"] is None:
        param_cfg["merge_finance_folderpath_dst"] = CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH
        show_warn("Set the 'merge_finance_folderpath_dst' argument to default destination folderpath: %s" % CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
    if param_cfg["show_progress"] and param_cfg["no_scrapy"]:
        param_cfg["show_progress"] = False
        show_warn("Set the 'show_progress' argument to False since 'no_scrapy' is set")
    if param_cfg["clone"] and param_cfg["no_scrapy"]:
        param_cfg["clone"] = False
        show_warn("Set the 'clone' argument to False since 'no_scrapy' is set")
# Special check
# renew_statement_field
    if param_cfg["renew_statement_field"]:
        # import pdb; pdb.set_trace()
        param_cfg["config_from_file"] = False
        param_cfg["method"] = "{0}-{1}".format(*CMN.FUNC.get_statement_scrapy_method_index_range())
        # param_cfg["company_from_file"] = None
        # param_cfg["company"] = None
        param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST


def setup_param():
    # import pdb; pdb.set_trace()
# Set method/time range/compnay
    if param_cfg["config_from_file"]:
        g_mgr.set_config_from_file()
    else:
# Set method
        method_index_list = None
        if param_cfg["method"] is not None:
            method_index_str_list = param_cfg["method"].split(",")
            # import pdb; pdb.set_trace()
            method_index_list = []
            for method_index_str in method_index_str_list:
                mobj = re.match("([\d]+)-([\d]+)", method_index_str)
                if mobj is not None:
# The method index range
                    method_index_list += range(int(mobj.group(1)), int(mobj.group(2)))
                else:
# The method index
                    method_index_list.append(int(method_index_str))
# Set time range
        time_range_start = None
        time_range_end = None
        if param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_RANGE:
            if param_cfg["time_duration_range"] is not None:
                time_duration_range_list = param_cfg["time_duration_range"].split(",")
                time_duration_range_list_len = len(time_duration_range_list)
                if time_duration_range_list_len == 2:
                    if not param_cfg["time_duration_range"].startswith(","):
# For time range
                        time_range_start = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_range_list[0])
                    time_range_end = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_range_list[1])
                elif time_duration_range_list_len == 1:
                    time_range_start = CMN.CLS.FinanceTimeBase.from_time_string(time_duration_range_list[0])
                else:
                    errmsg = "Incorrect time range format: %s" % param_cfg["time_duration_range"]
                    show_error_and_exit(errmsg)
        # import pdb; pdb.set_trace()
        g_mgr.set_method_time_duration(method_index_list, param_cfg["time_duration_type"], time_range_start, time_range_end)
# Set Company
        if CMN.DEF.IS_FINANCE_STOCK_MODE:
    # Set company list. For stock mode only
            # if param_cfg["company_from_file"] is not None:
            #     g_mgr.set_company_from_file(param_cfg["company_from_file"])
            # else:
            company_word_list = None
                if param_cfg["company"] is not None:
                    g_mgr.set_company(param_cfg["company"])

    g_mgr.enable_old_finance_folder_reservation(param_cfg["reserve_old"])
    g_mgr.enable_dry_run(param_cfg["dry_run"])
    if param_cfg["finance_folderpath"] is not None:
        g_mgr.set_finance_root_folderpath(param_cfg["finance_folderpath"])


def determine_finance_mode():
    parse_param(True)
    CMN.DEF.FINANCE_MODE = CMN.FUNC.get_finance_mode()
    if CMN.DEF.FINANCE_MODE == CMN.DEF.FINANCE_MODE_MARKET:
        # from libs.market import web_scrapy_market_configurer as CONF
        # g_configurer = CONF.MarketConfigurer.Instance()
        CMN.DEF.IS_FINANCE_MARKET_MODE = True
        CMN.DEF.IS_FINANCE_STOCK_MODE = False
        show_info("Instantiate in MARKET mode.......")
    elif CMN.DEF.FINANCE_MODE == CMN.DEF.FINANCE_MODE_STOCK:
        # from libs.stock import web_scrapy_stock_configurer as CONF
        # g_configurer = CONF.StockConfigurer.Instance()
        CMN.DEF.IS_FINANCE_MARKET_MODE = False
        CMN.DEF.IS_FINANCE_STOCK_MODE = True
        show_info("Instantiate in STOCK mode.......")
    else:
        raise ValueError("Unknown finance mode !!!")


def get_manager(update_cfg):
    # assert g_configurer is None, "g_configurer should NOT be None"
    mgr_obj = None
    if CMN.DEF.FINANCE_MODE == CMN.DEF.FINANCE_MODE_MARKET:
        from libs.market import market_mgr as MGR
        mgr_obj = MGR.MarketMgr(**update_cfg)
    elif CMN.DEF.FINANCE_MODE == CMN.DEF.FINANCE_MODE_STOCK:
        from libs.stock import stock_mgr as MGR
        mgr_obj = MGR.StockMgr(**update_cfg)
    else:
        raise ValueError("Unknown finance mode !!!")
    return mgr_obj


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
    if g_mgr.NoScrapyCSVFound:
        show_warn("No Web Data while scraping:")
        g_mgr.show_no_scrapy()


# @record_exe_time("CHECK")
# def do_check():
#     show_info("* Check errors in finance folder: %s" % g_mgr.FinanceRootFolderPath)
#     error_msg = g_mgr.check_scrapy_to_string()
#     if error_msg is not None:
#         show_error(error_msg)
#         # run_result_str = time_lapse_msg + error_msg
#         snapshot_result(error_msg)
#     else:
#         show_debug("Not errors found")
#     return True if error_msg is not None else False


@record_exe_time("CLONE")
def do_clone():
    show_info("* Clone the finance folder: %s" % g_mgr.FinanceRootFolderPath)
    datetime_now = datetime.today()
    clone_foldername = g_mgr.FinanceRootFolderPath + "_ok" + CMN.DEF.TIME_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
    show_debug("Clone the finance folder to %s" % clone_foldername)
    subprocess.call(["cp", "-r", g_mgr.FinanceRootFolderPath, clone_foldername])


# def calculate_profit(total_count):
#     import random
#     filename = "profit"
#     money = 100
#     win_count = 0
#     loss_count = 0
#     with open(filename, "w") as fp:
#         for index in range(1, total_count + 1):
#             dice_index = random.randint(1, 6)
#             money_old = money
#             if dice_index % 2 == 1:
#                 loss_count += 1
#                 money -= dice_index
#                 msg = "%03d %03d * - - %03d" % (index, money_old, money)
#                 print msg
#                 fp.write(msg + "\n")
#             else:
#                 win_count += 1
#                 dice_index = random.randint(1, 6)
#                 money += dice_index
#                 msg = "%03d %03d - * %d %03d" % (index, money_old, dice_index, money)
#                 print msg
#                 fp.write(msg + "\n")
#         msg = "Total: %d, Win: %d, Loss: %d" % (total_count, win_count, loss_count)
#         print msg
#         fp.write(msg + "\n")


# from libs.stock import web_scrapy_company_profile as CompanyProfile

if __name__ == "__main__":
    # calculate_profit(262)
    # company_profile = CompanyProfile.CompanyProfile.Instance()
    # import pdb; pdb.set_trace()
    # # print company_profile.lookup_company_first_data_date("3709", CMN.DEF.DATA_TIME_UNIT_DAY)
    # # print company_profile.lookup_company_first_data_date("3709", CMN.DEF.DATA_TIME_UNIT_WEEK)
    # # print company_profile.lookup_company_first_data_date("3709", CMN.DEF.DATA_TIME_UNIT_MONTH)
    # print company_profile.lookup_company_first_data_date("3709", CMN.DEF.DATA_TIME_UNIT_QUARTER)
    # sys.exit(0)
    # import pdb; pdb.set_trace()
# Parse the parameters and apply to manager class
    init_param()
    parse_param()
    if param_cfg["update_workday_calendar"]:
        update_workday_calendar_and_exit()
    if param_cfg["show_command_example"]:
        show_command_example_and_exit()
# Determine the mode and initialize the manager class
    determine_finance_mode()

    if param_cfg["help"]:
        show_usage_and_exit()
# Prepare the param for initializing the manager class
    update_cfg = {}
    if param_cfg["multi_thread"] is not None:
        update_cfg["multi_thread_amount"] = param_cfg["multi_thread"]
    if param_cfg["show_progress"]:
        update_cfg["show_progress"] = param_cfg["show_progress"]
    # import pdb; pdb.set_trace()
    g_mgr = get_manager(update_cfg)

# RUN the argument that will return after the execution is done
    if param_cfg["check_url"]:
        check_url_and_exit()
    if param_cfg["debug_scrapy_class"] is not None:
        debug_scrapy_class_and_exit(param_cfg["debug_scrapy_class"])
# Check the parameters for the manager
    check_param()
    # import pdb; pdb.set_trace()
    if param_cfg["company_list_in_folderpath"] is not None:
        show_company_list_in_folerpath_and_exit()
# Setup the parameters for the manager
    setup_param()
# Merge the finance folders...
    if param_cfg["merge_finance_folderpath_src_list"] is not None:
        merge_finance_folder_and_exit(param_cfg["merge_finance_folderpath_src_list"], param_cfg["merge_finance_folderpath_dst"])
    if param_cfg["renew_statement_field"]:
        renew_statement_field_and_exit()
# Start to do something about scrapy......
# Reset the file positon of the log file to 0
    # if param_cfg["check"]:
    #     CMN.WSL.reset_web_scrapy_logger_content()
# Try to scrap the web data
    if not param_cfg["no_scrapy"]:
        do_scrapy()
# Clone the csv files if necessary
    if param_cfg["clone"]:
        if not g_mgr.NoScrapyCSVFound:
            do_clone()
        else:
            show_error("Find errors while scraping... Stop the Clone action")
