#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import subprocess
from datetime import datetime, timedelta
from libs import common as CMN
from libs.common.common_variable import GlobalVar as GV
from libs import base as BASE
g_mgr = None
g_logger = CMN.LOG.get_logger()
param_cfg = {}


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "--silent\nDescription: Disable print log on console\n"
    print "--force_switch_finance_mode\nDescription: Force to switch the finance mode. Overwrite the setting in %s. 0: Market, 1: Stock\n" % CMN.DEF.FINANCE_MODE_SWITCH_CONF_FILENAME
    print "--show_command_example\nDescription: Show command example\nCaution: Ignore other parameters when set\n"
    print "--update_workday_calendar\nDescription: Update the workday calendar only\nCaution: Ignore other parameters when set\n"
    print "--show_workday_calendar_range\nDescription: Show the date range of the workday calendar only\nCaution: The canlendar is updated before display. Ignore other parameters when set"
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    print "--check_url\nDescription: Check URL of every source type\nCaution: Ignore other parameters when set\n"
    print "--debug_scrapy_class\nDescription: Debug a specific scrapy class only\nCaution: Ignore other parameters when set"
    print "Scrapy Class:"
    scrapy_class_index_list = CMN.FUNC.get_scrapy_class_index_range_list()
    for scrapy_class_index in scrapy_class_index_list:
        print "  %d: %s" % (scrapy_class_index, CMN.DEF.SCRAPY_CLASS_DESCRIPTION[scrapy_class_index])
    print "  Format: Scrapy class index (ex. 1)"
    print ""
    print "--merge_finance_folderpath\nDescription: Merge a list of source folderpaths to a destination folderpath\nCaution: The CSV file in different finance folder can NOT be duplicate. If so, the merge progress aborts !!!"
    print "  Format (src_folderpath1[,src_folderpath2,src_folderpath3,...]:[dst_folderpath]) (ex. /var/tmp/finance1[,/var/tmp/finance2,/var/tmp/finance3,...]:/var/tmp/merge_finance)\nCaution: Exploit the default destination folderpath[%s] when not set" % CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH
    print ""
    print "--no_scrapy\nDescription: Don't scrape Web data\n"
    print "--disable_auto_update_workday_calendar\nDescription: Disable automatically updating the workday calendar every time when running\n"
    print "--show_progress\nDescription: Show the progress of scraping Web data\nCaution: Only take effect when the no_scrapy flag is NOT set\n"
    print "--clone\nDescription: Clone the CSV files if no error occurs\nCaution: Only clone the folder when scrapy is successful\n"
    print "--clone_finance_foldername\nDescription: Clone folder name\nCaution: Only take effect when --clone is set\n"
    print "--reserve_old\nDescription: Reserve the old destination finance folders if exist\nDefault exmaples: %s, %s\n" % (CMN.DEF.CSV_ROOT_FOLDERPATH, CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
    print "--disable_flush_scrapy\nDescription: Disable the flag of flushing web scrapy data in buffer into CSV files while exception occurs during scraping"
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web\n"
    print "--finance_folderpath\nDescription: The finance root folder\nDefault: %s\n" % CMN.DEF.CSV_ROOT_FOLDERPATH
    print "--dataset_finance_folderpath\nDescription: Set the finance root folder to the dataset folder\n"
    print "--config_from_file\nDescription: The methods, time_duration_range, company from config: %s\n" % CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME
    print "--method\nDescription: The list of the methods\nDefault: All finance methods\nCaution: Only take effect when config_from_file is NOT set"
    print "Scrapy Method:"
    method_index_list = CMN.FUNC.get_method_index_range_list()
    for method_index in method_index_list:
        print "  %d: %s" % (method_index, CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index])
    print "  Format 1: Method (ex. 1,3,5)"
    print "  Format 2: Method range (ex. 2-6)"
    print "  Format 3: Method/Method range hybrid (ex. 1,3-4,6)"
    print ""
    print "--time_duration_range\nDescription: The data in the time range of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (,end_time): ,2015-01-01"
    print "  Format 3 (start_time,end_time): 2015-01-01,2015-09-04"
    # print "--time_today\nDescription: The today's data of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    print "--time_last\nDescription: The last data of the selected finance data source\nCaution: Only take effect when config_from_file is NOT set"
    print "--time_until_last\nDescription: The data of the selected finance data source until last day\nCaution: Only take effect when config_from_file is NOT set"
    print "--time_today\nDescription: The selected finance data source today\nCaution: Only take effect when config_from_file is NOT set"
    print "--time_until_today\nDescription: The data of the selected finance data source until today\nCaution: Only take effect when config_from_file is NOT set"
    print "*** --time_last --time_today ***\nCaution: Different date sources are updated at different time in a day. Only the same after %02d:%02d" % (CMN.DEF.TODAY_DATA_EXIST_HOUR, CMN.DEF.TODAY_DATA_EXIST_MINUTE)
    print "*** --time_duration_range --time_last --time_until_last --time_today --time_until_today ***\nCaution: Shuold NOT be set simultaneously. Will select the first one"
    print ""
    if GV.IS_FINANCE_STOCK_MODE:
        print "-c | --company\nDescription: The list of the company code number\nDefault: All company code nubmers\nCaution: Only take effect when config_from_file is NOT set"
        print "  Format1: Company code number (ex. 2347)"
        print "  Format2: Company code number range (ex. 2100-2200)"
        print "  Format3: Company group number (ex. [Gg]12)"
        print "  Format4: Company code number/number range/group hybrid (ex. 2347,2100-2200,G12,2362,g2,1500-1510)"
        print ""
        print "--multi_thread\nDescription: Scrape web data in multi-thread"
        print "  Format: multi-thread number (ex. 4)"
        print ""
        print "--enable_company_not_found_exception\nDescription: Enable the mechanism that the exception is rasied while encoutering the unknown company code number\n"
        print "--update_company_stock_price\nDescription: Update the stock price of specific companies\nCaution: This arugment is equal to the argument combination as below: --force_switch_finance_mode 1 --method 9 --time_until_today --dataset_finance_folderpath --reserve_old --company xxxx\n"
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


def show_workday_calendar_range_and_exit():
    workday_calendar = BASE.WC.WorkdayCanlendar.Instance()
    msg = "The time range of the workday calendar: %s - %s" % (workday_calendar.FirstWorkday, workday_calendar.LastWorkday)
    show_info(msg)
    sys.exit(0)


def show_command_example_and_exit():
    project_folderpath = GV.PROJECT_FOLDERPATH
    # print project_folderpath
    # project_config_folderpath = "%s/%s" % (project_folderpath, CMN.DEF.CONF_FOLDER)
    os.chdir(GV.PROJECT_CONF_FOLDERPATH)
    cmd = "cat %s" % CMN.DEF.COMMAND_EXAMPLE_FILENAME
    p = subprocess.Popen(cmd, shell=True)
    os.waitpid(p.pid, 0)
    sys.exit(0)


def check_url_and_exit():
    scrapy_class_index_list = CMN.FUNC.get_scrapy_class_index_range_list()
    error_found = False
    errmsg = "**************** Check %s URL ****************\n" % CMN.DEF.FINANCE_MODE_DESCRIPTION[GV.FINANCE_MODE]
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


def merge_finance_folder_and_exit(merge_finance_folderpath):
    split_str = merge_finance_folderpath.split(CMN.DEF.COLON_DATA_SPLIT)
    if len(split_str) != 2:
        raise ValueError("Incorrect format for merging folder: %s" % merge_finance_folderpath)
    (src_folderpath, dst_folderpath) = split_str
    if len(dst_folderpath) == 0:
        show_warn("Set the destination folderpath as default: %s" % CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
        dst_folderpath = CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH
    src_folderpath_list = src_folderpath.split(CMN.DEF.COMMA_DATA_SPLIT)
    show_info("Merge several finance folders%s to: %s" % (src_folderpath_list, dst_folderpath))
    g_mgr.merge_finance_folder(src_folderpath_list, dst_folderpath)
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
    param_cfg["silent"] = False
    param_cfg["force_switch_finance_mode"] = None
    param_cfg["update_workday_calendar"] = False
    param_cfg["show_workday_calendar_range"] = False
    param_cfg["show_command_example"] = False
    param_cfg["help"] = False
    param_cfg["check_url"] = False
    param_cfg["debug_scrapy_class"] = None
    param_cfg["merge_finance_folderpath"] = None
    param_cfg["no_scrapy"] = False
    param_cfg["disable_auto_update_workday_calendar"] = False
    param_cfg["show_progress"] = False
    param_cfg["clone"] = False
    param_cfg["clone_finance_foldername"] = None
    param_cfg["reserve_old"] = False
    param_cfg["disable_flush_scrapy"] = False
    param_cfg["dry_run"] = False
    param_cfg["dataset_finance_folderpath"] = False
    param_cfg["finance_folderpath"] = None
    param_cfg["config_from_file"] = False
    param_cfg["method"] = None
    param_cfg["time_duration_type"] = None # Should be check in check_param()
    param_cfg["time_duration_range"] = None
    param_cfg["company"] = None
    # param_cfg["renew_statement_field"] = False
    param_cfg["enable_company_not_found_exception"] = False
    param_cfg["multi_thread"] = None
    param_cfg["update_company_stock_price"] = None


def parse_param(early_parse=False):
    argc = len(sys.argv)
    index = 1
    index_offset = None
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        if re.match("--show_command_example", sys.argv[index]):
            if not early_parse:
                param_cfg["show_command_example"] = True
                break
            index_offset = 1
        elif re.match("--update_workday_calendar", sys.argv[index]):
            if not early_parse:
                param_cfg["update_workday_calendar"] = True
                break
            index_offset = 1
        elif re.match("--show_workday_calendar_range", sys.argv[index]):
            if not early_parse:
                param_cfg["show_workday_calendar_range"] = True
                break
            index_offset = 1
        elif re.match("--silent", sys.argv[index]):
            if early_parse:
                param_cfg["silent"] = True
                CMN.DEF.CAN_PRINT_CONSOLE = not param_cfg["silent"]
            index_offset = 1
        elif re.match("--force_switch_finance_mode", sys.argv[index]):
            if early_parse:
                param_cfg["force_switch_finance_mode"] = int(sys.argv[index + 1])
            index_offset = 2
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
        elif re.match("--merge_finance_folderpath", sys.argv[index]):
            if not early_parse:
                param_cfg["merge_finance_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--no_scrapy", sys.argv[index]):
            if not early_parse:
                param_cfg["no_scrapy"] = True
            index_offset = 1
        elif re.match("--disable_auto_update_workday_calendar", sys.argv[index]):
            if not early_parse:
                param_cfg["disable_auto_update_workday_calendar"] = True
            index_offset = 1
        elif re.match("--show_progress", sys.argv[index]):
            if not early_parse:
                param_cfg["show_progress"] = True
            index_offset = 1
        elif re.match("--clone_finance_foldername", sys.argv[index]):
            if not early_parse:
                param_cfg["clone_finance_foldername"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--clone", sys.argv[index]):
            if not early_parse:
                param_cfg["clone"] = True
            index_offset = 1
        elif re.match("--reserve_old", sys.argv[index]):
            if not early_parse:
                param_cfg["reserve_old"] = True
            index_offset = 1
        elif re.match("--disable_flush_scrapy", sys.argv[index]):
            if not early_parse:
                param_cfg["disable_flush_scrapy"] = True
            index_offset = 1
        elif re.match("--dry_run", sys.argv[index]):
            if not early_parse:
                param_cfg["dry_run"] = True
            index_offset = 1
        elif re.match("--dataset_finance_folderpath", sys.argv[index]):
            if not early_parse:
                param_cfg["dataset_finance_folderpath"] = True
            index_offset = 1
        elif re.match("--finance_folderpath", sys.argv[index]):
            if not early_parse:
                param_cfg["finance_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--config_from_file", sys.argv[index]):
            if not early_parse:
                param_cfg["config_from_file"] = True
            index_offset = 1
        elif re.match("--method", sys.argv[index]):
            if not early_parse:
                param_cfg["method"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--time_duration_range", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_duration_range attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
                    param_cfg["time_duration_range"] = sys.argv[index + 1]
                # g_logger.debug("Param time range: %s", param_cfg["time_duration_range"])
            index_offset = 2
        # elif re.match("--time_today", sys.argv[index]):
        #     if not early_parse:
        #         if param_cfg["time_duration_type"] is not None:
        #             g_logger.debug("Time duration has already been set to: %d, ignore the time_today attribute...", param_cfg["time_duration_type"])
        #         else:
        #             param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
        #     index_offset = 1
        elif re.match("--time_last", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_last attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST
            index_offset = 1
        elif re.match("--time_until_last", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_last attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_UNTIL_LAST
            index_offset = 1
        elif re.match("--time_today", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_last attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
            index_offset = 1
        elif re.match("--time_until_today", sys.argv[index]):
            if not early_parse:
                if param_cfg["time_duration_type"] is not None:
                    g_logger.debug("Time duration has already been set to: %d, ignore the time_last attribute...", param_cfg["time_duration_type"])
                else:
                    param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_UNTIL_TODAY
            index_offset = 1
        elif re.match("(-c|--company)", sys.argv[index]):
            if not early_parse:
                if GV.IS_FINANCE_MARKET_MODE:
                    g_logger.warn("The company arguemnt is ignored in the Market mode")
                else:
                    param_cfg["company"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--enable_company_not_found_exception", sys.argv[index]):
            if early_parse:
                param_cfg["enable_company_not_found_exception"] = True
            index_offset = 1
        elif re.match("--multi_thread", sys.argv[index]):
            if not early_parse:
                param_cfg["multi_thread"] = int(sys.argv[index + 1])
            index_offset = 2
        elif re.match("--update_company_stock_price", sys.argv[index]):
            if early_parse:
                param_cfg["update_company_stock_price"] = sys.argv[index + 1]
            index_offset = 2            
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset
# Adjust the parameters setting...
        if param_cfg["update_company_stock_price"] is not None:
            if param_cfg["config_from_file"]:
                show_warn("The 'config_from_file' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["config_from_file"] = False
            if early_parse:
                if param_cfg["force_switch_finance_mode"] is not None:
                    show_warn("The 'force_switch_finance_mode' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["force_switch_finance_mode"] = 1
            else:
                if param_cfg["method"] is not None:
                    show_warn("The 'method' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["method"] = "9"
                if param_cfg["time_duration_type"] is not None:
                    show_warn("The 'time_duration_type' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_UNTIL_TODAY
                if not param_cfg["dataset_finance_folderpath"]:
                    show_warn("dataset_finance_folderpath' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["dataset_finance_folderpath"] = True
                if not param_cfg["reserve_old"]:
                    show_warn("reserve_old' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["reserve_old"] = True
                if not param_cfg["company"]:
                    show_warn("company' argument won't take effect since 'update_company_stock_price' is set")
                param_cfg["company"] = param_cfg["update_company_stock_price"]


def check_param():    
    if param_cfg["config_from_file"] is not False:
        if param_cfg["method"] is not None:
            param_cfg["method"] = None
            show_warn("The 'method' argument is ignored since 'config_from_file' is set")
        if param_cfg["time_duration_type"] is not None:
            param_cfg["time_duration_type"] = None
            show_warn("The 'time_duration_type' argument is ignored since 'config_from_file' is set")
        if param_cfg["time_duration_range"] is not None:
            param_cfg["time_duration_range"] = None
            show_warn("The 'time_duration_range' argument is ignored since 'config_from_file' is set")
        if param_cfg["company"] is not None:
            param_cfg["company"] = None
            show_warn("The 'company' argument is ignored since it's 'Market' mode")
    else:
        if param_cfg["time_duration_type"] is None:
            param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_UNTIL_LAST
            show_warn("Set the 'time_duration_type' argument to DATA_TIME_DURATION_LAST as default")
        if param_cfg["time_duration_type"] != CMN.DEF.DATA_TIME_DURATION_RANGE:
            param_cfg["time_duration_range"] = None
            show_warn("The 'time_duration_range' argument is ignored since 'time_duration_type' is NOT 'DATA_TIME_DURATION_RANGE'")
        if GV.IS_FINANCE_MARKET_MODE:
            if param_cfg["company"] is not None:
                param_cfg["company"] = None
                show_warn("The 'company' argument is ignored since it's 'Market' mode")
# # Check time range
#     time_range_start = None
#     if param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_TODAY:
#         time_range_start = CMN.CLS.FinanceDate.get_today_finance_date()
#     elif param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_LAST:
#         time_range_start = CMN.CLS.FinanceDate.get_last_finance_date()
#     elif param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_RANGE:
#         (time_range_start, _) = CMN.FUNC.parse_time_duration_range_str_to_object(param_cfg["time_duration_range"])
#     else:
#         raise ValueError("Unknown time duration type: %d" % self.xcfg["time_duration_type"])

#     if time_range_start is not None:
#         workday_calendar = BASE.WC.WorkdayCanlendar.Instance()
#         if time_range_start > workday_calendar.LastWorkday:
#             errmsg = "ERROR!! The start date[%s] is later than the last workday[%s]" % (time_range_start, workday_calendar.LastWorkday)
#             show_error_and_exit(errmsg)
    if param_cfg["dataset_finance_folderpath"]:
        if param_cfg["finance_folderpath"] is not None:
            show_warn("The 'finance_folderpath' argument is invalid since dataset_finance_folderpath is set")

    if GV.IS_FINANCE_MARKET_MODE:
        # if param_cfg["renew_statement_field"]:
        #     param_cfg["renew_statement_field"] = False
        #     show_warn("The 'renew_statement_field' argument is ignored since it's 'Market' mode")
        if param_cfg["enable_company_not_found_exception"]:
            param_cfg["enable_company_not_found_exception"] = False
            show_warn("The 'enable_company_not_found_exception' argument is invalid since it's 'Market' mode")
        if param_cfg["multi_thread"] is not None:
            param_cfg["multi_thread"] = None
            show_warn("The 'multi_thread' argument is invalid since it's 'Market' mode")
    # else:
    #     if param_cfg["multi_thread"] is not None:
    #         if param_cfg["renew_statement_field"]:
    #             param_cfg["multi_thread"] = None
    #             show_warn("The 'multi_thread' argument is invalid when the 'renew_statement_field' argument is true")

    if param_cfg["show_progress"] and param_cfg["no_scrapy"]:
        param_cfg["show_progress"] = False
        show_warn("Set the 'show_progress' argument to False since 'no_scrapy' is set")
    if param_cfg["clone"] and param_cfg["no_scrapy"]:
        param_cfg["clone"] = False
        show_warn("Set the 'clone' argument to False since 'no_scrapy' is set")
    if param_cfg["clone_finance_foldername"] is not None:
        if not param_cfg["clone"]:
            param_cfg["clone_finance_foldername"] = None
            show_warn("The 'clone_finance_foldername' argument is invalid since clone is not set")
# # Special check
# # renew_statement_field
#     if param_cfg["renew_statement_field"]:
#         # import pdb; pdb.set_trace()
#         param_cfg["config_from_file"] = False
#         param_cfg["method"] = "{0}-{1}".format(*CMN.FUNC.get_statement_scrapy_method_index_range())
#         param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST


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
        elif param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_LAST:
            time_range_start = time_range_end = CMN.CLS.FinanceDate.get_last_finance_date()
        elif param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_UNTIL_LAST:
            time_range_end = CMN.CLS.FinanceDate.get_last_finance_date()
        elif param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_TODAY:
            time_range_start = time_range_end = CMN.CLS.FinanceDate.get_today_finance_date()
        elif param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_UNTIL_TODAY:
            time_range_end = CMN.CLS.FinanceDate.get_today_finance_date()
        else:
            raise ValueError("Unknown time duration type: %d" % param_cfg["time_duration_type"])
        # import pdb; pdb.set_trace()
        g_mgr.set_method_time_duration(method_index_list, param_cfg["time_duration_type"], time_range_start, time_range_end)
# Set Company
        if GV.IS_FINANCE_STOCK_MODE:
            if param_cfg["company"] is not None:
                g_mgr.set_company(param_cfg["company"])

    g_mgr.reserve_old_finance_folder(param_cfg["reserve_old"])
    g_mgr.disable_flush_scrapy_while_exception(param_cfg["disable_flush_scrapy"])
    g_mgr.enable_dry_run(param_cfg["dry_run"])
    if param_cfg["dataset_finance_folderpath"]:
        g_mgr.set_finance_root_folderpath(update_dataset=param_cfg["dataset_finance_folderpath"])
    elif param_cfg["finance_folderpath"] is not None:
        g_mgr.set_finance_root_folderpath(csv_root_folderpath=param_cfg["finance_folderpath"])


def update_finance_mode_global_variable(finance_mode=None):
    # assert not GV.GLOBAL_VARIABLE_UPDATED, "GV.GLOBAL_VARIABLE_UPDATED should NOT be True"
    # import pdb; pdb.set_trace()
    if finance_mode is None:
        if not CMN.FUNC.check_config_file_exist(CMN.DEF.FINANCE_MODE_SWITCH_CONF_FILENAME):
            show_warn("The config file[%s] does NOT exist, set finance mode to %s" % (CMN.DEF.FINANCE_MODE_SWITCH_CONF_FILENAME, CMN.DEF.FINANCE_MODE_DESCRIPTION[CMN.DEF.FINANCE_MODE_MARKET]))
            finance_mode = CMN.DEF.FINANCE_MODE_MARKET
        else:
            finance_mode = CMN.FUNC.get_finance_mode()
    if finance_mode == CMN.DEF.FINANCE_MODE_MARKET:
        # from libs.market import web_scrapy_market_configurer as CONF
        # g_configurer = CONF.MarketConfigurer.Instance()
        GV.IS_FINANCE_MARKET_MODE = True
        GV.IS_FINANCE_STOCK_MODE = False
        show_info("Instantiate in MARKET mode.......")
    elif finance_mode == CMN.DEF.FINANCE_MODE_STOCK:
        # from libs.stock import web_scrapy_stock_configurer as CONF
        # g_configurer = CONF.StockConfigurer.Instance()
        GV.IS_FINANCE_MARKET_MODE = False
        GV.IS_FINANCE_STOCK_MODE = True
        show_info("Instantiate in STOCK mode.......")
    else:
        raise ValueError("Unknown finance mode !!!")
    GV.FINANCE_MODE = finance_mode
    return finance_mode


def update_global_variable(cur_finance_mode):
    # import pdb; pdb.set_trace()
    # assert not GV.GLOBAL_VARIABLE_UPDATED, "GV.GLOBAL_VARIABLE_UPDATED should NOT be True"
    if (param_cfg["force_switch_finance_mode"] is not None) and (param_cfg["force_switch_finance_mode"] != cur_finance_mode):
        show_warn("Force to switch finance mode to %s. Will OVERWRITE the setting in %s" % (CMN.DEF.FINANCE_MODE_DESCRIPTION[param_cfg["force_switch_finance_mode"]], CMN.DEF.FINANCE_MODE_SWITCH_CONF_FILENAME))
        update_finance_mode_global_variable(param_cfg["force_switch_finance_mode"])

    GV.ENABLE_COMPANY_NOT_FOUND_EXCEPTION = param_cfg["enable_company_not_found_exception"]
    GV.GLOBAL_VARIABLE_UPDATED = True


def get_manager(update_cfg):
    # assert g_configurer is None, "g_configurer should NOT be None"
    mgr_obj = None
    if GV.IS_FINANCE_MARKET_MODE:
        from libs.market import market_mgr as MGR
        mgr_obj = MGR.MarketMgr(**update_cfg)
    elif GV.IS_FINANCE_STOCK_MODE:
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
    clone_finance_folderpath = None
    if param_cfg["clone_finance_foldername"] is None:
        datetime_now = datetime.today()
        clone_finance_folderpath = g_mgr.FinanceRootFolderPath + "_ok" + CMN.DEF.TIME_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
    else:
        clone_finance_folderpath = os.path.dirname(g_mgr.FinanceRootFolderPath) + "/" + param_cfg["clone_finance_foldername"]
    CMN.FUNC.remove_folder_if_exist(clone_finance_folderpath)
    show_debug("Clone the finance folder to %s" % clone_finance_folderpath)
    subprocess.call(["cp", "-r", g_mgr.FinanceRootFolderPath, clone_finance_folderpath])



if __name__ == "__main__":
# Parse the parameters and apply to manager class
    init_param()
    cur_finance_mode = update_finance_mode_global_variable()
    parse_param(True)
    update_global_variable(cur_finance_mode)
    parse_param()

    if param_cfg["show_workday_calendar_range"]:
        show_workday_calendar_range_and_exit()
    if param_cfg["update_workday_calendar"]:
        update_workday_calendar_and_exit()
    if param_cfg["show_command_example"]:
        show_command_example_and_exit()
    if param_cfg["help"]:
        show_usage_and_exit()
    # import pdb; pdb.set_trace()
# Initialize the manager class
    g_mgr = get_manager(
        {
            "multi_thread_amount": param_cfg["multi_thread"],
            "show_progress": param_cfg["show_progress"],
        }
    )
# RUN the argument that will return after the execution is done
    if param_cfg["check_url"]:
        check_url_and_exit()
    if param_cfg["debug_scrapy_class"] is not None:
        debug_scrapy_class_and_exit(param_cfg["debug_scrapy_class"])
# Merge the finance folders...
    if param_cfg["merge_finance_folderpath"] is not None:
        merge_finance_folder_and_exit(param_cfg["merge_finance_folderpath"])
# Check the parameters for the manager
    check_param()
    if not param_cfg["disable_auto_update_workday_calendar"]:
        workday_calendar = BASE.WC.WorkdayCanlendar.Instance()
    # import pdb; pdb.set_trace()
# Setup the parameters for the manager
    setup_param()
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
