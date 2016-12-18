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
g_mgr = None
g_logger = CMN.WSL.get_web_scrapy_logger()


param_cfg = {}

def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "--show_command_example\nDescription: Show command example\nCaution: Ignore other parameters when set"
    print "--update_workday_calendar\nDescription: Update the workday calendar only\nCaution: Ignore other parameters when set"
    print "--market_mode --stock_mode\nDescription: Switch the market/stock mode\nCaution: Read parameters from %s when NOT set" % CMN.DEF.DEF_MARKET_STOCK_SWITCH_CONF_FILENAME
    print "--silent\nDescription: Disable print log on console"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "--check_url\nDescription: Check URL of every source type\nCaution: Ignore other parameters when set"
    print "--debug_source\nDescription: Debug a specific source type only\nCaution: Ignore other parameters when set"
    print "--check_result\nDescription: Check the CSV files after scraping Web data"
    print "--clone_result\nDescription: Clone the CSV files if no error occurs\nCaution: Only work when --check_result is set"
    print "--reserve_old\nDescription: Reserve the old destination finance folders if exist\nDefault exmaples: %s, %s" % (CMN.DEF.DEF_CSV_ROOT_FOLDERPATH, CMN.DEF.DEF_CSV_DST_MERGE_ROOT_FOLDERPATH)
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web"
    print "--finance_folderpath\nDescription: The finance root folder\nDefault: %s" % CMN.DEF.DEF_CSV_ROOT_FOLDERPATH
    print "--source_from_all_time_range_default_file\nDescription: The finance data source in all time range from file: %s\nCaution: source/source_from_xxx_file/time_duration_range are ignored when set" % (CMN.DEF.DEF_MARKET_ALL_TIME_RANGE_CONFIG_FILENAME if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_STOCK_ALL_TIME_RANGE_CONFIG_FILENAME)
    print "--source_from_today_file\nDescription: The today's finance data source from file\nCaution: source/time_duration_range are ignored when set"
    print "--source_from_last_file\nDescription: The last finance data source from file\nCaution: source/time_duration_range are ignored when set"
    print "--source_from_time_range_file\nDescription: The finance data source in time range from file\nCaution: source/time_duration_range are ignored when set"
    print "-s --source\nDescription: The list of the finance data sources\nDefault: All finance data sources\nCaution: Only work when source_from_file is NOT set"
    source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
    for source_type_index in source_type_index_list:
        print "  %d: %s" % (source_type_index, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_index])
    print "  Format 1: 1,3,5"
    print "  Format 2: 2-6"
    print "  Format 3: 1,3-4,6"
    print "--time_today\nDescription: The today's data of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "--time_last\nDescription: The last data of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "--time_duration_range_all\nDescription: The data in the all time range of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "--time_duration_range\nDescription: The data in the time range of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (,end_time): ,2015-01-01"
    print "  Format 3 (start_time,end_time): 2015-01-01,2015-09-04"
    print "--time_today --time_last --time_duration_range --time_duration_range_all\nCaution: Shuold NOT be set simultaneously. Will select the first one"
    if CMN.DEF.IS_FINANCE_STOCK_MODE:
        print "--company_list_in_default_folderpath\nDescription: Show the company number list in the default finance folder"
        print "--company_list_in_folderpath\nDescription: Show the company number list in the finance folder" 
        print "--company_from_file\nDescription: The company code number from file\nDefault: All company code nubmers\nCaution: company is ignored when set"
        print "-c --company\nDescription: The list of the company code number\nDefault: All company code nubmers\nCaution: Only work when company_from_file is NOT set"
        print "  Format1: Company code number (ex. 2347)"
        print "  Format2: Company code number range (ex. 2100-2200)"
        print "  Format3: Company group number (ex. [Gg]12)"
        print "  Format4: Company code number/number range/group hybrid (ex. 2347,2100-2200,G12,2362,g2,1500-1510)"
        print "--multi_thread\nDescription: Scrape web data in multi-thread"
        print "  Format: multi-thread number (ex. 4)"
    print "--merge_finance_folderpath_src_list\nDescription: The list of source folderpaths to be merged\nCaution: The CSV file in different finance folder cant NOT be duplicate. If so, the merge progress aborts"
    print "  Format 1 (folderpath): /var/tmp/finance"
    print "  Format 2 (folderpath1,folderpath2,folderpath3): /var/tmp/finance1,/var/tmp/finance2,/var/tmp/finance3"
    print "--merge_finance_folderpath_dst\nDescription: The destination folderpath after merging\nDefault: %s" % CMN.DEF.DEF_CSV_DST_MERGE_ROOT_FOLDERPATH
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
    if not os.path.exists(CMN.DEF.DEF_SNAPSHOT_FOLDER):
        os.makedirs(CMN.DEF.DEF_SNAPSHOT_FOLDER)
    with open(CMN.RUN_RESULT_FILENAME, 'w') as fp:
        fp.write(run_result_str.encode('utf8'))
    datetime_now = datetime.today()
    snapshot_filename = CMN.SNAPSHOT_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
    subprocess.call(["tar", "cvzf", snapshot_filename, CMN.RUN_RESULT_FILENAME, g_mgr.FinanceRootFolderPath, WSL.LOG_FILE_PATH])
    subprocess.call(["mv", snapshot_filename, CMN.DEF.DEF_SNAPSHOT_FOLDER])
    subprocess.call(["rm", CMN.RUN_RESULT_FILENAME])


def update_workday_calendar_and_exit():
    workday_calendar = BASE.WC.WebScrapyWorkdayCanlendar.Instance()
    sys.exit(0)


def show_command_example_and_exit():
    project_folderpath = CMN.FUNC.get_project_folderpath()
    # print project_folderpath
    project_config_folderpath = "%s/%s" % (project_folderpath, CMN.DEF.DEF_CONF_FOLDER)
    os.chdir(project_config_folderpath)
    cmd = "cat %s" % CMN.DEF.DEF_COMMAND_EXAMPLE_FILENAME
    p = subprocess.Popen(cmd, shell=True)
    os.waitpid(p.pid, 0)
    sys.exit(0)


def check_url_and_exit():
    source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
    error_found = False
    errmsg = "**************** Check %s URL ****************\n" % CMN.FUNC.get_finance_mode_description()
    for source_type_index in source_type_index_list:
        try:
            g_mgr.do_scrapy_debug(source_type_index, True)
        except Exception as e:
            error_found = True
            errmsg += " %d: %s %s" % (source_type_index, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_index], str(e))
    if error_found:
        show_error_and_exit(errmsg)
    sys.exit(0)


def debug_source_and_exit(source_type_index):
    if not CMN.FUNC.check_source_type_index_in_range(source_type_index):
        errmsg = "Unsupported source type index: %d" % source_type_index
        show_error_and_exit(errmsg)
    g_mgr.do_scrapy_debug(source_type_index)
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

# def switch_mode(mode):
# # MARKET: 0
# # STOCK: 1
#     project_folderpath = CMN.FUNC.get_project_folderpath()
#     # print project_folderpath
#     project_config_folderpath = "%s/%s" % (project_folderpath, CMN.DEF.DEF_CONF_FOLDER)
#     os.chdir(project_config_folderpath)
#     cmd = None
#     if mode == CMN.DEF.FINANCE_ANALYSIS_MARKET:
#         cmd = "sed s/%d/%d/g %s" % (CMN.DEF.FINANCE_ANALYSIS_STOCK, CMN.DEF.FINANCE_ANALYSIS_MARKET, CMN.DEF.DEF_MARKET_STOCK_SWITCH_CONF_FILENAME)
#     elif mode == CMN.DEF.FINANCE_ANALYSIS_STOCK:
#         cmd = "sed s/%d/%d/g %s" % (CMN.DEF.FINANCE_ANALYSIS_MARKET, CMN.DEF.FINANCE_ANALYSIS_STOCK, CMN.DEF.DEF_MARKET_STOCK_SWITCH_CONF_FILENAME)
#     else:
#         raise ValueError("Unknown mode: %d", mode)
#     p = subprocess.Popen(cmd, shell=True)
#     os.waitpid(p.pid, 0)

def init_param():
    # import pdb; pdb.set_trace()
    param_cfg["update_workday_calendar"] = False
    param_cfg["show_command_example"] = False
    param_cfg["finance_mode"] = None
    param_cfg["help"] = False
    param_cfg["check_url"] = False
    param_cfg["debug_source"] = None
    param_cfg["silent"] = False
    param_cfg["check_result"] = False
    param_cfg["clone_result"] = False
    param_cfg["reserve_old"] = False
    param_cfg["dry_run"] = False
    param_cfg["finance_folderpath"] = None
    param_cfg['source_from_all_time_range_default_file'] = False
    param_cfg["source"] = None
    param_cfg["time_duration_type"] = None # Should be check in check_param()
    param_cfg["time_duration_range"] = None
    param_cfg["source_from_file"] = None
    param_cfg["company_list_in_default_folderpath"] = False
    param_cfg["company_list_in_folderpath"] = None
    param_cfg["company"] = None
    param_cfg["company_from_file"] = None
    param_cfg["multi_thread"] = None
    param_cfg["merge_finance_folderpath_src_list"] = None
    param_cfg["merge_finance_folderpath_dst"] = None


def parse_param():
    argc = len(sys.argv)
    index = 1
    index_offset = None
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        if re.match("--show_command_example", sys.argv[index]):
            param_cfg["show_command_example"] = True
            index_offset = 1
        elif re.match("--update_workday_calendar", sys.argv[index]):
            param_cfg["update_workday_calendar"] = True
            index_offset = 1
        elif re.match("--market_mode", sys.argv[index]):
            param_cfg["finance_mode"] = CMN.DEF.FINANCE_ANALYSIS_MARKET
            index_offset = 1
        elif re.match("--stock_mode", sys.argv[index]):
            param_cfg["finance_mode"] = CMN.DEF.FINANCE_ANALYSIS_STOCK
            index_offset = 1
        elif re.match("(-h|--help)", sys.argv[index]):
            param_cfg["help"] = True
            index_offset = 1
        elif re.match("--check_url", sys.argv[index]):
            param_cfg["check_url"] = True
            index_offset = 1 
        elif re.match("--debug_source", sys.argv[index]):
            param_cfg["debug_source"] = int(sys.argv[index + 1])
            index_offset = 2 
        elif re.match("--silent", sys.argv[index]):
            param_cfg["silent"] = True
            CMN.DEF.CAN_PRINT_CONSOLE = not param_cfg["silent"]
            index_offset = 1
        elif re.match("--check_result", sys.argv[index]):
            param_cfg["check_result"] = True
            index_offset = 1
        elif re.match("--clone_result", sys.argv[index]):
            param_cfg["clone_result"] = True
            index_offset = 1
        elif re.match("--reserve_old", sys.argv[index]):
            param_cfg["reserve_old"] = True
            index_offset = 1
        elif re.match("--dry_run", sys.argv[index]):
            param_cfg["dry_run"] = True
            index_offset = 1
        elif re.match("--finance_folderpath", sys.argv[index]):
            param_cfg["finance_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--source_from_all_time_range_default_file", sys.argv[index]):
            param_cfg["source_from_all_time_range_default_file"] = True
            param_cfg["time_duration_type"] == CMN.DEF.DATA_TIME_DURATION_RANGE
            index_offset = 1
        elif re.match("--source_from_today_file", sys.argv[index]):
            param_cfg["source_from_file"] = sys.argv[index + 1]
            param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
            index_offset = 2
        elif re.match("--source_from_last_file", sys.argv[index]):
            param_cfg["source_from_file"] = sys.argv[index + 1]
            param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST
            index_offset = 2
        elif re.match("--source_from_time_range_file", sys.argv[index]):
            param_cfg["source_from_file"] = sys.argv[index + 1]
            param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
            index_offset = 2
        elif re.match("(-s|--source)", sys.argv[index]):
            param_cfg["source"] = sys.argv[index + 1]
            g_logger.debug("Param source: %s", param_cfg["source"])
            index_offset = 2
        elif re.match("--time_today", sys.argv[index]):
            if param_cfg["time_duration_type"] is not None:
                g_logger.debug("Time duration has already been set to: %d, ignore the time_today attribute...", param_cfg["time_duration_type"])
            else:
                param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
            index_offset = 1
        elif re.match("--time_last", sys.argv[index]):
            if param_cfg["time_duration_type"] is not None:
                g_logger.debug("Time duration has already been set to: %d, ignore the time_last attribute...", param_cfg["time_duration_type"])
            else:
                param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_LAST
            index_offset = 1
        elif re.match("--time_duration_range_all", sys.argv[index]):
            if param_cfg["time_duration_type"] is not None:
                g_logger.debug("Time duration has already been set to: %d, ignore the time_duration_range_all attribute...", param_cfg["time_duration_type"])
            else:
                param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
            index_offset = 1
        elif re.match("--time_duration_range", sys.argv[index]):
            if param_cfg["time_duration_type"] is not None:
                g_logger.debug("Time duration has already been set to: %d, ignore the time_duration_range attribute...", param_cfg["time_duration_type"])
            else:
                param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_RANGE
                param_cfg["time_duration_range"] = sys.argv[index + 1]
            # g_logger.debug("Param time range: %s", param_cfg["time_duration_range"])
            index_offset = 2
        elif re.match("--company_list_in_default_folderpath", sys.argv[index]):
            param_cfg["company_list_in_default_folderpath"] = True
            index_offset = 1
        elif re.match("--company_list_in_folderpath", sys.argv[index]):
            param_cfg["company_list_in_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--company_from_file", sys.argv[index]):
            if CMN.DEF.IS_FINANCE_MARKET_MODE:
                g_logger.warn("The company_from_file arguemnt is ignored in the Market mode")
            else:
                param_cfg["company_from_file"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("(-c|--company)", sys.argv[index]):
            if CMN.DEF.IS_FINANCE_MARKET_MODE:
                g_logger.warn("The company arguemnt is ignored in the Market mode")
            else:
                param_cfg["company"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--multi_thread", sys.argv[index]):
            param_cfg["multi_thread"] = int(sys.argv[index + 1])
            index_offset = 2
        elif re.match("--merge_finance_folderpath_src_list", sys.argv[index]):
            param_cfg["merge_finance_folderpath_src_list"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--merge_finance_folderpath_dst", sys.argv[index]):
            param_cfg["merge_finance_folderpath_dst"] = sys.argv[index + 1]
            index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():
    if param_cfg["source_from_all_time_range_default_file"]:
        if param_cfg["source_from_file"] is not None:
            show_warn("The 'source_from_file' argument is ignored since 'source_from_all_time_range_default_file' is set")
        if CMN.DEF.IS_FINANCE_MARKET_MODE:
            param_cfg["source_from_file"] = CMN.DEF.DEF_MARKET_ALL_TIME_RANGE_CONFIG_FILENAME
        elif CMN.DEF.IS_FINANCE_STOCK_MODE:
            param_cfg["source_from_file"] = CMN.DEF.DEF_STOCK_ALL_TIME_RANGE_CONFIG_FILENAME
    if param_cfg["source_from_file"] is not None:
        if param_cfg["source"] is not None:
            param_cfg["source"] = None
            show_warn("The 'source' argument is ignored since 'source_from_file' is set")
        # if param_cfg["time_duration_type"] is not None:
        #     param_cfg["time_duration_type"] = None
        #     show_warn("The 'time_duration' argument is ignored since 'source_from_file' is set")
    if param_cfg["time_duration_type"] is None:
        param_cfg["time_duration_type"] = CMN.DEF.DATA_TIME_DURATION_TODAY
        show_warn("Set the 'time_duration_type' argument to DATA_TIME_DURATION_TODAY as default")        
    if CMN.DEF.IS_FINANCE_MARKET_MODE:
        if param_cfg["company_list_in_default_folderpath"]:
            param_cfg["company_list_in_default_folderpath"] = False
            show_warn("The 'company_list_in_default_folderpath' argument is ignored since it's 'Market' mode")
        if param_cfg["company_list_in_folderpath"] is not None:
            param_cfg["company_list_in_folderpath"] = None
            show_warn("The 'company_list_in_folderpath' argument is ignored since it's 'Market' mode")
        if param_cfg["company"] is not None:
            param_cfg["company"] = None
            show_warn("The 'company' argument is ignored since it's 'Market' mode")
        if param_cfg["company_from_file"] is not None:
            param_cfg["company_from_file"] = None
            show_warn("The 'company_from_file' argument is ignored since it's 'Market' mode")
        if param_cfg["multi_thread"] is not None:
            param_cfg["multi_thread"] = None
            show_warn("The 'multi_thread' argument is invalid since it's 'Market' mode")
    else:
        if param_cfg["company_list_in_default_folderpath"]:
            if param_cfg["company_list_in_folderpath"] is not None:
                show_warn("The 'company_list_in_folderpath' argument is ignored since 'company_list_in_default_folderpath' is set")
            else:
                param_cfg["company_list_in_folderpath"] = CMN.DEF.DEF_CSV_ROOT_FOLDERPATH
        if param_cfg["company_from_file"] is not None:
            if param_cfg["company"] is not None:
                param_cfg["company"] = None
                show_warn("The 'company' argument is ignored since 'company_from_file' is set")
    if param_cfg["merge_finance_folderpath_src_list"] is not None and param_cfg["merge_finance_folderpath_dst"] is None:
        param_cfg["merge_finance_folderpath_dst"] = CMN.DEF.DEF_CSV_DST_MERGE_ROOT_FOLDERPATH
        show_warn("Set the 'merge_finance_folderpath_dst' argument to default destination folderpath: %s" % CMN.DEF.DEF_CSV_DST_MERGE_ROOT_FOLDERPATH)


def setup_param():
# Set source type and time range
    if param_cfg["source_from_file"] is not None:
        g_mgr.set_source_type_time_duration_from_file(param_cfg["source_from_file"], param_cfg["time_duration_type"])
    else:
# Set source type
        source_type_index_list = None
        if param_cfg["source"] is not None:
            source_type_index_str_list = param_cfg["source"].split(",")
            source_type_index_list = []
            for source_type_index_str in source_type_index_str_list:
                mobj = re.match("([\d]+)-([\d]+)", source_type_index_str)
                if mobj is not None:
                    source_type_start_index = int(mobj.group(1))
                    if not CMN.FUNC.check_source_type_index_in_range(source_type_start_index):
                        errmsg = "Unsupported source type index: %d" % source_type_start_index
                        show_error_and_exit(errmsg)
                    source_type_end_index = int(mobj.group(2))
                    if not CMN.FUNC.check_source_type_index_in_range(source_type_end_index):
                        errmsg = "Unsupported source type index: %d" % source_type_end_index
                        show_error_and_exit(errmsg)
                    for source_type_index in range(source_type_start_index, source_type_end_index + 1):
                        source_type_index_list.append(source_type_index)
                else:
                    source_type_index = int(source_type_index_str)
                    if not CMN.FUNC.check_source_type_index_in_range(source_type_index):
                        errmsg = "Unsupported source type index: %d" % source_type_index
                        show_error_and_exit(errmsg)
                    source_type_index_list.append(source_type_index)
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
                        time_range_start = CMN.CLS.FinanceTimeBase.from_string(time_duration_range_list[0])
                    time_range_end = CMN.CLS.FinanceTimeBase.from_string(time_duration_range_list[1])
                elif time_duration_range_list_len == 1:
                    time_range_start = CMN.CLS.FinanceTimeBase.from_string(time_duration_range_list[0])
                else:
                    errmsg = "Incorrect time range format: %s" % param_cfg["time_duration_range"]
                    show_error_and_exit(errmsg)
        # import pdb; pdb.set_trace()
        g_mgr.set_source_type_time_duration(source_type_index_list, param_cfg["time_duration_type"], time_range_start, time_range_end)

    if CMN.DEF.IS_FINANCE_STOCK_MODE:
# Set company list. For stock mode only
        if param_cfg["company_from_file"] is not None:
            g_mgr.set_company_from_file(param_cfg["company_from_file"])
        elif param_cfg["company"] is not None:
            company_word_list = param_cfg["company"].split(",")
            g_mgr.set_company(company_word_list)
# Swith to mode thread mode. For stock mode only
        if param_cfg["multi_thread"] is not None:
            g_mgr.enable_multithread(param_cfg["multi_thread"])

    g_mgr.enable_old_finance_folder_reservation(param_cfg["reserve_old"])
    g_mgr.enable_dry_run(param_cfg["dry_run"])
    if param_cfg["finance_folderpath"] is not None:
        g_mgr.set_finance_root_folderpath(param_cfg["finance_folderpath"])


class TestClass(object):
    @classmethod
    def get_instance(cls):
        obj = cls()
        print type(obj)
        return obj

    # def __init__(self):
    #     self.test1 = 1
    #     print "fuck"

    def __getattr__(self, name):
        value = "%s added" % name
        setattr(self, name, value)
        return value


class TestClass1(TestClass):
    def __getattr__(self, name):
        return super(TestClass1, self).__getattr__(name)

    def test1(self):
        print "fuck1"

if __name__ == "__main__":
    # TestClass1.get_instance()
    # import pdb; pdb.set_trace()
    # company_group_set = web_scrapy_company_group_set.WebScrapyCompanyGroupSet()
    # company_list = ["1101", "1102", "1103", "1104", "1108", "1109", "1110"]
    # company_group_set.add_company_list(company_list)
    # sub_company_group_list = company_group_set.get_sub_company_group_set_list(3)
    # for company_group, company_number_list in sub_company_group_list[2].items():
    #     print "group: %s, number_list: %s" % (company_group, company_number_list)
    # test_object = TestClass1()
    # print test_object.__dict__
    # print test_object.test2
    # print test_object.__dict__
    # # test = TestClass.get_instance()
    # test_class = TestClass1()
    # getattr(test_class, "test1")()
    # print "True" if isinstance(test_class, TestClass1) else "False"
    # print "True" if isinstance(test_class, TestClass) else "False"
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
    if param_cfg["finance_mode"] is None:
        CMN.DEF.IS_FINANCE_MARKET_MODE = CMN.FUNC.is_market_mode()
        CMN.DEF.IS_FINANCE_STOCK_MODE = CMN.FUNC.is_stock_mode()
    elif param_cfg["finance_mode"] == CMN.DEF.FINANCE_ANALYSIS_MARKET:
        CMN.DEF.IS_FINANCE_MARKET_MODE = True
        CMN.DEF.IS_FINANCE_STOCK_MODE = False
    elif param_cfg["finance_mode"] == CMN.DEF.FINANCE_ANALYSIS_STOCK:
        CMN.DEF.IS_FINANCE_MARKET_MODE = False
        CMN.DEF.IS_FINANCE_STOCK_MODE = True
    else:
        raise ValueError("Unknown mode !!!")

    if param_cfg["help"]:
        show_usage_and_exit()

    if CMN.DEF.IS_FINANCE_MARKET_MODE:
        from libs.market import web_scrapy_market_mgr as MGR
        g_mgr = MGR.WebSracpyMarketMgr()
    else:
        from libs.stock import web_scrapy_stock_mgr as MGR
        g_mgr = MGR.WebSracpyStockMgr()
# RUN the argument that will return after the execution is done
    if param_cfg["check_url"]:
        check_url_and_exit()
    elif param_cfg["debug_source"] is not None:
        debug_source_and_exit(param_cfg["debug_source"])
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

# Start to do something about scrapy......
# Reset the file positon of the log file to 0
    if param_cfg["check_result"]:
        CMN.WSL.reset_web_scrapy_logger_content()

# Start to do something.......
# Try to scrap the web data
    show_info("* Scrap the data from the website......")
    time_range_start_second = int(time.time())
    # import pdb; pdb.set_trace()
    g_mgr.do_scrapy()
    time_range_end_second = int(time.time())
    show_info("* Scrap the data from the website...... DONE!!!")
    time_lapse_msg = u"######### Time Lapse: %d second(s) #########\n" % (time_range_end_second - time_range_start_second)
    show_info(time_lapse_msg)

    error_found = False
# Check if all the csv files are created
    if param_cfg["check_result"]:
        show_info("* Check errors in finance folder: %s" % g_mgr.FinanceRootFolderPath)
        (file_not_found_list, file_is_empty_list) = g_mgr.check_scrapy()
        error_msg_list = []
        for file_not_found in file_not_found_list:
            error_msg = u"FileNotFound: %s, %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[file_not_found['index']], file_not_found['filename'])
            show_error(error_msg)
            error_msg_list.append(error_msg)
        for file_is_empty in file_is_empty_list:
            error_msg = u"FileIsEmpty: %s, %s" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[file_is_empty['index']], file_is_empty['filename'])
            show_error(error_msg)
            error_msg_list.append(error_msg)
        if len(error_msg_list) != 0:
            run_result_str = time_lapse_msg
            run_result_str += "".join(error_msg_list)
            snapshot_result(run_result_str)
            error_found = True

# Clone the csv files if necessary
    if param_cfg["clone_result"]:
        show_info("* Clone the finance folder: %s" % g_mgr.FinanceRootFolderPath)
        if not error_found:
            datetime_now = datetime.today()
            clone_foldername = g_mgr.FinanceRootFolderPath + "_ok" + CMN.DEF.TIME_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
            show_debug("Clone the finance folder to %s" % clone_foldername)
            subprocess.call(["cp", "-r", g_mgr.FinanceRootFolderPath, clone_foldername])
        else:
            show_error("Find errors while checking... Stop the Clone action")
