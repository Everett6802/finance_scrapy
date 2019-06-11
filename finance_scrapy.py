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
import scrapy.scrapy_mgr as MGR

g_mgr = None
g_logger = CMN.LOG.get_logger()
param_cfg = {}
combination_param_cfg = {}

update_dataset_errmsg = None


def show_usage_and_exit():
    print "=========================== Usage ==========================="
    print "-h | --help\nDescription: The usage\nCaution: Ignore other parameters when set\n"
    print "--update_workday_calendar\nDescription: Update the workday calendar only\nCaution: Ignore other parameters when set\n"
    print "--show_workday_calendar_range\nDescription: Show the date range of the workday calendar only\nCaution: The canlendar is updated before display. Ignore other parameters when set"
    print "--update_csv_field\nDescription: Update the CSV file description\n"
    print "--no_scrapy\nDescription: Don't scrape Web data\n"
    print "--reserve_old\nDescription: Reserve the old destination finance folders if exist\nDefault exmaples: %s, %s\n" % (CMN.DEF.CSV_ROOT_FOLDERPATH, CMN.DEF.CSV_DST_MERGE_ROOT_FOLDERPATH)
    print "--append_before\nDescription: Update the earlier scrapy data into database\n"
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web\n"
    print "--finance_folderpath\nDescription: The finance root folder\nDefault: %s\n" % CMN.DEF.CSV_ROOT_FOLDERPATH
    # print "--dataset_finance_folderpath\nDescription: Set the finance root folder to the dataset folder\n"
    print "--config_from_filename\nDescription: The methods, time_range, company from config filie\n"
    print "-m | --method\nDescription: The list of the methods\nDefault: All finance methods\nCaution: Only take effect when config_from_filename is NOT set"
    print "Scrapy Method:"
    for method_index in range(CMN.DEF.SCRAPY_METHOD_LEN):
        print "  %d: %s" % (method_index, CMN.DEF.SCRAPY_METHOD_DESCRIPTION[method_index])
    print "  Format 1: Method (ex. 1,3,5)"
    print "  Format 2: Method range (ex. 2-6)"
    print "  Format 3: Method/Method range hybrid (ex. 1,3-4,6)"
    print ""
    print "-c | --company\nDescription: The list of the company code number\nDefault: All company code nubmers\nCaution: Only take effect when config_from_filename is NOT set"
    print "  Format1: Company code number (ex. 2347)"
    print "  Format2: Company code number range (ex. 2100-2200)"
    print "  Format3: Company group number (ex. [Gg]12)"
    print "  Format4: Company code number/number range/group hybrid (ex. 2347,2100-2200,G12,2362,g2,1500-1510)"
    print ""
    print "-t | --time\nDescription: The time range\nCaution: Some Scrapy Methods can't set time range\nOnly take effect when config_from_filename is NOT set"
    print "  Format1: start_time, end_time, time_slice"
    print "  Format2: start_time, end_time"
    print "--time_at\nDescription: The time\nCaution: Some Scrapy Methods can't set time\nOnly take effect when config_from_filename is NOT set"
    print "  Format1: time, time_slice; Eqaul to --time time, time, time_slice"
    print "  Format2: time; Eqaul to --time time, time"
    print "--time_until_last\nDescription: The time range until last day"
    print ""
    print "--max_data_count\nDescription: Only scrape the latest N data\n"
    print "--enable_company_not_found_exception\nDescription: Enable the mechanism that the exception is rasied while encoutering the unknown company code number\n"
# Combination argument
    print "Combination argument:\n Caution: Exclusive. Only the first combination argument takes effect. Some related arguments may be overwriten"
    print "--update_config_from_filename\nDescription: Update dataset from config file\nCaution: This arugment is equal to the argument combination as below: --finance_folderpath %s --reserve_old --config_from_filename xxx\n" % GV.FINANCE_DATASET_DATA_FOLDERPATH
    print "*** Scrapy Method ***"
    for scrapy_method_index, csv_filename in enumerate(CMN.DEF.SCRAPY_CSV_FILENAME):
        # import pdb; pdb.set_trace()
        scrapy_method = CMN.DEF.SCRAPY_METHOD_NAME[scrapy_method_index]
        scrarpy_method_costant_cfg = CMN.DEF.SCRAPY_METHOD_CONSTANT_CFG[scrapy_method]
        can_set_time_range = scrarpy_method_costant_cfg["can_set_time_range"]
        if not scrarpy_method_costant_cfg["need_company_number"]:
            print "--update_%s\n--update_method%d\nDescription: Update %s\nCaution: This arugment is equal to the argument combination as below: --method %d --finance_folderpath %s %s --reserve_old\n" % (csv_filename, scrapy_method_index, scrapy_method, scrapy_method_index, GV.FINANCE_DATASET_DATA_FOLDERPATH, ("--time_until_last" if can_set_time_range else ""))
            # print "--update_%s_from_file\n--update_method%d_from_file\nDescription: Update %s\nCaution: This arugment is equal to the argument combination as below: --method %d --finance_folderpath %s --reserve_old --config_from_filename\n" % (csv_filename, scrapy_method_index, scrapy_method, scrapy_method_index, GV.FINANCE_DATASET_DATA_FOLDERPATH)
        else:
            print "--update_company_%s\n--update_company_method%d\nDescription: Update %s of specific companies\nCaution: This arugment is equal to the argument combination as below: --method %d --finance_folderpath %s %s --reserve_old --company xxxx\n" % (csv_filename, scrapy_method_index, scrapy_method, scrapy_method_index, GV.FINANCE_DATASET_DATA_FOLDERPATH, ("--time_until_last" if can_set_time_range else ""))
            # print "--update_company_%s_from_file\n--update_company_method%d_from_file\nDescription: Update %s of specific companies. Companies are from file\nCaution: This arugment is equal to the argument combination as below: --method %d --finance_folderpath %s --reserve_old --config_from_filename\n" % (csv_filename, scrapy_method_index, scrapy_method, scrapy_method_index, GV.FINANCE_DATASET_DATA_FOLDERPATH)
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


def update_workday_calendar_and_exit():
    workday_calendar = LIBS.WC.WorkdayCanlendar.Instance()
    sys.exit(0)


def show_workday_calendar_range_and_exit():
    workday_calendar = LIBS.WC.WorkdayCanlendar.Instance()
    msg = "The time range of the workday calendar: %s - %s" % (workday_calendar.FirstWorkday, workday_calendar.LastWorkday)
    show_info(msg)
    sys.exit(0)


def init_param():
    param_cfg["silent"] = False
    param_cfg["help"] = False
    param_cfg["update_workday_calendar"] = False
    param_cfg["show_workday_calendar_range"] = False
    param_cfg["no_scrapy"] = False
    param_cfg["reserve_old"] = False
    param_cfg["append_before"] = False
    param_cfg["dry_run"] = False
    # param_cfg["dataset_finance_folderpath"] = False
    param_cfg["finance_folderpath"] = None
    param_cfg["config_from_filename"] = None
    param_cfg["update_csv_field"] = False
    param_cfg["method"] = None
    param_cfg["company"] = None
    param_cfg["time_at"] = None
    param_cfg["time_until_last"] = False
    param_cfg["time"] = None
    param_cfg["max_data_count"] = None
    param_cfg["enable_company_not_found_exception"] = False
# combination arguments
# parameter from the config file
    combination_param_cfg["update_dataset_config_from_filename"] = None
# parameter from the setup
    combination_param_cfg["update_dataset_method"] = None
    combination_param_cfg["update_dataset_company_list"] = None
    combination_param_cfg["update_dataset_time"] = None
    # combination_param_cfg['update_dataset_duplicate_setting'] = False


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
        elif re.match("--update_workday_calendar", sys.argv[index]):
            param_cfg["update_workday_calendar"] = True
            index_offset = 1
        elif re.match("--show_workday_calendar_range", sys.argv[index]):
            param_cfg["show_workday_calendar_range"] = True
            index_offset = 1
        elif re.match("--no_scrapy", sys.argv[index]):
            param_cfg["no_scrapy"] = True
            index_offset = 1
        elif re.match("--reserve_old", sys.argv[index]):
            param_cfg["reserve_old"] = True
            index_offset = 1
        elif re.match("--append_before", sys.argv[index]):
            param_cfg["append_before"] = True
            index_offset = 1
        elif re.match("--dry_run", sys.argv[index]):
            param_cfg["dry_run"] = True
            index_offset = 1
        # elif re.match("--dataset_finance_folderpath", sys.argv[index]):
        #     param_cfg["dataset_finance_folderpath"] = True
        #     index_offset = 1
        elif re.match("--finance_folderpath", sys.argv[index]):
            param_cfg["finance_folderpath"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--config_from_filename", sys.argv[index]):
            param_cfg["config_from_filename"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--update_csv_field", sys.argv[index]):
            param_cfg["update_csv_field"] = True
            index_offset = 1
        elif re.match("--method", sys.argv[index]):
            param_cfg["method"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("(-c|--company)", sys.argv[index]):
            param_cfg["company"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--time_until_last", sys.argv[index]):
            param_cfg["time_until_last"] = True
            index_offset = 1
        elif re.match("--time_at", sys.argv[index]):
            param_cfg["time_at"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("(-t|--time)", sys.argv[index]):
            param_cfg["time"] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--max_data_count", sys.argv[index]):
            param_cfg["max_data_count"] = int(sys.argv[index + 1])
            index_offset = 2
        elif re.match("--enable_company_not_found_exception", sys.argv[index]):
            param_cfg["enable_company_not_found_exception"] = True
            index_offset = 1
        elif re.match("--update_config_from_filename", sys.argv[index]):
            # import pdb; pdb.set_trace()
            combination_param_cfg['update_dataset_config_from_filename'] = sys.argv[index + 1]
            index_offset = 2
        elif re.match("--update", sys.argv[index]):
            # import pdb; pdb.set_trace()
            mobj = None
            index_offset = 1
            if re.search("company", sys.argv[index]):
                index_offset = 2
                if re.search("method", sys.argv[index]):
                    mobj = re.match("--update_company_method([\d]{1,})", sys.argv[index])
                    if mobj is None: raise ValueError("Incorrect argument format: %s" % sys.argv[index])
                    combination_param_cfg['update_dataset_method'] = mobj.group(1)
                else:
                    mobj = re.match("--update_company_([\w]+)", sys.argv[index])
                    if mobj is None: raise ValueError("Incorrect argument format: %s" % sys.argv[index])
                    combination_param_cfg['update_dataset_method'] = CMN.FUNC.scrapy_method_name_to_index(mobj.group(1).replace("_", " "))
                combination_param_cfg['update_dataset_company_list'] = sys.argv[index + 1]
                if not CMN.FUNC.scrapy_method_need_company_number(int(combination_param_cfg['update_dataset_method'])):
                    raise ValueError("The method[%s:%s] Need Company number" % (combination_param_cfg['update_dataset_method'], CMN.DEF.SCRAPY_METHOD_NAME[int(mobj.group(1))]))
            else:
                if re.search("method", sys.argv[index]):
                    mobj = re.match("--update_method([\d]{1,})", sys.argv[index])
                    if mobj is None: raise ValueError("Incorrect argument format: %s" % sys.argv[index])
                    combination_param_cfg['update_dataset_method'] = mobj.group(1)
                else:
                    mobj = re.match("--update_([\w]+)", sys.argv[index])
                    if mobj is None: raise ValueError("Incorrect argument format: %s" % sys.argv[index])
                    combination_param_cfg['update_dataset_method'] = CMN.FUNC.scrapy_method_name_to_index(mobj.group(1).replace("_", " "))
                if CMN.FUNC.scrapy_method_need_company_number(int(mobj.group(1))):
                    raise ValueError("The method[%s] Don't Need Company number" % combination_param_cfg['update_dataset_method'])
            if mobj is None: raise ValueError("Incorrect argument format: %s" % sys.argv[index])
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


def check_param():
    if param_cfg['help']:
        show_warn("Show Usage and Exit. Other parameters are ignored")
    if param_cfg['update_csv_field']:
        show_warn("Update CSV field description and Exit. Other parameters are ignored")

    # import pdb; pdb.set_trace()
    combination_argument = (combination_param_cfg["update_dataset_config_from_filename"] is not None) or (combination_param_cfg["update_dataset_method"] is not None)
    if combination_argument:
# Disable the other parameters while combination argment is set
        if param_cfg["method"] is not None:
            show_warn("The 'method' argument won't take effect since 'combination argument' is set")
            param_cfg["method"] = None
        if param_cfg["company"] is not None:
            show_warn("company' argument won't take effect since 'combination argument' is set")
            param_cfg["company"] = None
        if param_cfg["time_at"] is not None:
            show_warn("The 'time_at' argument is ignored since 'combination argument' is set")
            param_cfg["time_at"] = None
        if param_cfg["time_until_last"]:
            show_warn("The 'time_until_last' argument is ignored since 'combination argument' is set")
            param_cfg["time_until_last"] = False
        if param_cfg["time"] is not None:
            show_warn("time' argument won't take effect since 'combination argument' is set")
            param_cfg["time"] = None
        # if not param_cfg["dataset_finance_folderpath"]:
        #     show_warn("dataset_finance_folderpath' argument should be TRUE since 'combination argument' is set")
        #     param_cfg["dataset_finance_folderpath"] = False
        if not param_cfg["reserve_old"]:
            show_warn("reserve_old' argument should be TRUE since 'combination argument' is set")
            param_cfg["reserve_old"] = False
        if not param_cfg["append_before"]:
            show_warn("append_before' argument should be TRUE since 'combination argument' is set")
            param_cfg["append_before"] = False
        if param_cfg["config_from_filename"]:
            show_warn("The 'config_from_filename' argument won't take effect since 'update_config_from_filename' is set")
            param_cfg["config_from_filename"] = None
        if param_cfg["finance_folderpath"]:
            show_warn("The 'finance_folderpath' argument won't take effect since 'update_config_from_filename' is set")
            param_cfg["finance_folderpath"] = None
# Setup the parameters for the combination argument
        if combination_param_cfg["update_dataset_config_from_filename"] is not None:
# From config
            if combination_param_cfg["update_dataset_method"] is not None:
                show_warn("The 'update_dataset_method' argument won't take effect since 'update_config_from_filename' is set")
                combination_param_cfg["update_dataset_method"] = None
            param_cfg["config_from_filename"] = combination_param_cfg["update_dataset_config_from_filename"]
        else:
            param_cfg["method"] = combination_param_cfg["update_dataset_method"]
            method_index = int(param_cfg["method"])
            if CMN.FUNC.scrapy_method_need_company_number(method_index):
                param_cfg["company"] = combination_param_cfg["update_dataset_company_list"]
            if CMN.FUNC.scrapy_method_can_set_time_range(method_index):
                param_cfg["time"] = combination_param_cfg["update_dataset_time"]
        # import pdb; pdb.set_trace()
        param_cfg["finance_folderpath"] = GV.FINANCE_DATASET_DATA_FOLDERPATH
        param_cfg["reserve_old"] = True
    else:
        if param_cfg["config_from_filename"]:
            if param_cfg["method"] is not None:
                param_cfg["method"] = None
                show_warn("The 'method' argument is ignored since 'config_from_file' is set")
            if param_cfg["company"] is not None:
                param_cfg["company"] = None
                show_warn("The 'company' argument is ignored since 'config_from_file' is set")
            if param_cfg["time_at"] is not None:
                param_cfg["time_at"] = None
                show_warn("The 'time_at' argument is ignored since 'config_from_file' is set")
            if param_cfg["time_until_last"]:
                param_cfg["time_until_last"] = False
                show_warn("The 'time_until_last' argument is ignored since 'config_from_file' is set")
            if param_cfg["time"] is not None:
                param_cfg["time"] = None
                show_warn("The 'time' argument is ignored since 'config_from_file' is set")
        else:
            # import pdb; pdb.set_trace()
            # if param_cfg["method"] is None:
            #     # import pdb; pdb.set_trace()
            #     param_cfg["method"] = ",".join(map(str, range(CMN.DEF.SCRAPY_METHOD_END)))
            if param_cfg["time_at"] is not None:
                if param_cfg["time"] is not None:
                    param_cfg["time"] = None
                    show_warn("The 'time' argument is ignored since 'time_at' is set")
                param_cfg["time"] = param_cfg["time_at"].split(",")[0] + "," + param_cfg["time_at"]
            if param_cfg["time_until_last"]:
                if param_cfg["time"] is not None:
                    param_cfg["time"] = None
                    show_warn("The 'time' argument is ignored since 'time_until_last' is set")
                    # param_cfg["time"] = ",%s," % CMN.FUNC.generate_today_time_str()

def setup_param():
    # import pdb; pdb.set_trace()
    if param_cfg["finance_folderpath"] is not None:
        g_mgr.set_finance_root_folderpath(param_cfg["finance_folderpath"])
# Set method/compnay/time
    if param_cfg["config_from_filename"]:
        g_mgr.set_config_filename(param_cfg["config_from_filename"])
        g_mgr.set_scrapy_config_from_file()
    else:
        g_mgr.set_scrapy_config(param_cfg["method"], param_cfg["company"], param_cfg["time"])
    g_mgr.reserve_old_finance_folder(param_cfg["reserve_old"])
    g_mgr.set_append_before_mode(param_cfg["append_before"])
    g_mgr.enable_dry_run(param_cfg["dry_run"])


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


# import dataset as DS


if __name__ == "__main__":
    # # # # df, _ = DS.LD.load_stock_price_history("2458", data_time_unit=CMN.DEF.DATA_TIME_UNIT_QUARTER)
    # # df, _ = DS.LD.load_revenue_history("2458")
    # # # df, _ = DS.LD.load_stock_price_history("2458")
    # sys.exit(0)

# Parse the parameters and apply to manager class
    init_param()
    parse_param()
    update_global_variable()

    update_cfg = {
        "reserve_old_finance_folder": param_cfg["reserve_old"],
        # "dry_run_only": param_cfg["dry_run"],
        # "finance_root_folderpath": param_cfg["finance_folderpath"],
        # "config_filename": param_cfg["config_from_filename"],
        "max_data_count": param_cfg["max_data_count"],
    }
    g_mgr = MGR.ScrapyMgr(**update_cfg)
# Check the parameters for the manager
    check_param()
    if param_cfg["help"]:
        show_usage_and_exit()
    if param_cfg["update_workday_calendar"]:
        update_workday_calendar_and_exit()
    if param_cfg["show_workday_calendar_range"]:
        show_workday_calendar_range_and_exit()
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
