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
CMN.DEF.IS_FINANCE_MARKET_MODE = CMN.FUNC.is_market_mode()
CMN.DEF.IS_FINANCE_STOCK_MODE = CMN.FUNC.is_stock_mode()
if CMN.DEF.IS_FINANCE_MARKET_MODE:
    from libs.market import web_scrapy_market_mgr as MGR
    g_mgr = MGR.WebSracpyMarketMgr()
else:
    from libs.stock import web_scrapy_stock_mgr as MGR
    g_mgr = MGR.WebSracpyStockMgr()
g_logger = CMN.WSL.get_web_scrapy_logger()


param_cfg = {}

def show_usage():
    print "=========================== Usage ==========================="
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "--update_workday_calendar\nDescription: Update the workday calendar only\nCaution: Ignore other parameters when set"
    print "--check_url\nDescription: Check URL of every source type\nCaution: Ignore other parameters when set"
    print "--debug_source\nDescription: Debug a specific source type only\nCaution: Ignore other parameters when set"
    print "--silent\nDescription: Disable print log on console"
    print "--check_result\nDescription: Check the CSV files after scraping Web data"
    print "--clone_result\nDescription: Clone the CSV files if no error occurs\nCaution: Only work when --check_result is set"
    print "--reserve_old\nDescription: Reserve the old CSV file in %s" % CMN.DEF.DEF_CSV_FILE_PATH
    print "--dry_run\nDescription: Dry-run only. Will NOT scrape data from the web"
    print "--source_from_all_time_range_default_file\nDescription: The finance data source in all time range from file: %s\nCaution: source/time_duration_range are ignored when set" % (CMN.DEF.DEF_MARKET_ALL_TIME_RANGE_CONFIG_FILENAME if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_STOCK_ALL_TIME_RANGE_CONFIG_FILENAME)
    print "--source_from_today_file\nDescription: The today's finance data source from file\nCaution: source/time_duration_range are ignored when set"
    print "--source_from_last_file\nDescription: The last finance data source from file\nCaution: source/time_duration_range are ignored when set"
    print "--source_from_time_range_file\nDescription: The finance data source in time range from file\nCaution: source/time_duration_range are ignored when set"
    # print "--source_from_file\nDescription: The last finance data source from file: %s\nCaution: source/time_duration_range are ignored when set" % (CMN.DEF.DEF_MARKET_LAST_CONFIG_FILENAME if CMN.DEF.IS_FINANCE_MARKET_MODE else CMN.DEF.DEF_STOCK_LAST_CONFIG_FILENAME)
    print "-s --source\nDescription: The list of the finance data sources\nDefault: All finance data sources\nCaution: Only work when source_from_file is NOT set"
    source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
    for source_type_index in source_type_index_list:
        print "  %d: %s" % (source_type_index, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[source_type_index])
    print "  Format: 1,3,5"
    print "--time_today\nDescription: The today's data of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "--time_last\nDescription: The last data of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "--time_duration_range_all\nDescription: The data in the all time range of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "--time_duration_range\nDescription: The data in the time range of the selected finance data source\nCaution: Only work when source_from_file is NOT set"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (,end_time): ,2015-01-01"
    print "  Format 3 (start_time,end_time): 2015-01-01,2015-09-04"
    print "--time_today --time_last --time_duration_range --time_duration_range_all\nCaution: Shuold NOT be set simultaneously. Will select the first one"
    # print "-m --method\nDescription: The method of setting the parameters\nDefault: TODAY"
    # print "  TODAY: Read the today.conf file and only scrap today's data"
    # print "  HISTORY: Read the history.conf file and scrap data in the specific time interval"
    # print "  USER_DEFINED: User define the data source (1,2,3) and time interval (None for Today)"
    # print "--multi_thread\nDescription: Scrap Web data by using multiple threads\nCaution: Deprecated"
    if CMN.DEF.IS_FINANCE_STOCK_MODE:
        print "--company_from_file\nDescription: The company code number from file\nDefault: All company code nubmers\nCaution: company is ignored when set"
        print "-c --company\nDescription: The list of the company code number\nDefault: All company code nubmers\nCaution: Only work when company_from_file is NOT set"
        print "  Format 1 Company code number: 2347"
        print "  Format 2 Company code number range: 2100-2200"
        print "  Format 3 Company group number: [Gg]12"
        print "  Format 4 Company code number/number range/group hybrid: 2347,2100-2200,G12,2362,g2,1500-1510"
    print "--run_daily\nDescription: Run daily web-scrapy\nCaution: Ignore other parameters when set"
    print "============================================================="


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
    subprocess.call(["tar", "cvzf", snapshot_filename, CMN.RUN_RESULT_FILENAME, CMN.DEF.DEF_CSV_FILE_PATH, WSL.LOG_FILE_PATH])
    subprocess.call(["mv", snapshot_filename, CMN.DEF.DEF_SNAPSHOT_FOLDER])
    subprocess.call(["rm", CMN.RUN_RESULT_FILENAME])


def init_param():
    # import pdb; pdb.set_trace()
    param_cfg["silent"] = False
    param_cfg["check_result"] = False
    param_cfg["clone_result"] = False
    param_cfg["reserve_old"] = False
    param_cfg["dry_run"] = False
    param_cfg["source"] = None
    param_cfg["time_duration_type"] = None # Should be check in check_param()
    param_cfg["time_duration_range"] = None
    param_cfg["source_from_file"] = None
    param_cfg["company"] = None
    param_cfg["company_from_file"] = None


def parse_param():
    argc = len(sys.argv)
    index = 1
    index_offset = None
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])
        if re.match("(-h|--help)", sys.argv[index]):
            show_usage()
            sys.exit(0)
        elif re.match("--update_workday_calendar", sys.argv[index]):
            workday_calendar = BASE.WC.WebScrapyWorkdayCanlendar.Instance()
            workday_calendar.update_workday_canlendar(True)
            sys.exit(0)
        elif re.match("--check_url", sys.argv[index]):
            # import pdb; pdb.set_trace()
            source_type_index_list = CMN.FUNC.get_source_type_index_range_list()
            error_found = False
            errmsg = "**************** Check URL ****************\n"
            for source_type_index in source_type_index_list:
                try:
                    g_mgr.do_scrapy_debug(source_type_index, True)
                except Exception as e:
                    error_found = True
                    errmsg += " %d: %s %s" % (source_type_index, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[index], str(e))
            if error_found:
                show_error_and_exit(errmsg)
            else:
                sys.exit(0)
        elif re.match("--debug_source", sys.argv[index]):
            source_type_index = int(sys.argv[index + 1])
            if not CMN.FUNC.check_source_type_index_in_range(source_type_index):
                errmsg = "Unsupported source type index: %d" % source_type_index
                show_error_and_exit(errmsg)
            g_mgr.do_scrapy_debug(source_type_index)
            sys.exit(0)
        elif re.match("--silent", sys.argv[index]):
            param_cfg["silent"] = True
            index_offset = 1
        # elif re.match("--multi_thread", sys.argv[index]):
        #     multi_thread = True
        #     index_offset = 1
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
        # elif re.match("--run_daily", sys.argv[index]):
        #     # method_index = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX
        #     show_console = False
        #     remove_old = True
        #     check_result = True
        #     break
        elif re.match("--source_from_all_time_range_default_file", sys.argv[index]):
            if CMN.DEF.IS_FINANCE_MARKET_MODE:
                param_cfg["source_from_file"] = CMN.DEF.DEF_MARKET_ALL_TIME_RANGE_CONFIG_FILENAME
            elif CMN.DEF.IS_FINANCE_STOCK_MODE:
                param_cfg["source_from_file"] = CMN.DEF.DEF_STOCK_ALL_TIME_RANGE_CONFIG_FILENAME
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
        # elif re.match("--source_from_file", sys.argv[index]):
        #     param_cfg["source_from_file"] = sys.argv[index + 1]
        #     index_offset = 2
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
        # elif re.match("(-m|--method)", sys.argv[index]):
        #     method = sys.argv[index + 1]
        #     # import pdb; pdb.set_trace()
        #     try:
        #         method_index = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index(method)
        #     except ValueError as e:
        #         errmsg = "Unsupoorted method: %s" % method
        #         show_error_and_exit(errmsg)
        #     g_logger.debug("Param method: %s", method)
        #     index_offset = 2
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
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset

# Create the time range list
    # import pdb; pdb.set_trace()
    # source_type_time_duration_range_list = None
    # if method_index != CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX:
    #     if source_type_index_list is not None or time_range_start is not None:
    #         msg = "Ignore other parameters when the method is %s\n" % CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE[CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX]     
    #         if not show_console:
    #             g_logger.info(msg)
    #         else:
    #             sys.stdout.write(msg)
    #     conf_filename = CMN.DEF.DEF_TODAY_CONFIG_FILENAME if method_index == CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX else CMN.DEF.DEF_HISTORY_CONFIG_FILENAME
    #     source_type_time_duration_range_list = CMN.FUNC.parse_config_file(conf_filename)
    #     if source_type_time_duration_range_list is None:
    #         show_error_and_exit("Fail to parse the config file: %s" % conf_filename)
    # else:
    #     source_type_time_duration_range_list = []
    #     if source_type_index_list is None:
    #         source_type_index_list = range(CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN)
    #     # import pdb; pdb.set_trace()
    #     for source_type_index in source_type_index_list:
    #         source_type_time_duration_range_list.append(
    #             # {
    #             #     "source_type_index": source_type_index,
    #             #     "time_range_start": time_range_start,
    #             #     "time_range_end": time_range_end,
    #             # }
    #             CMN.CLS.SourceTypeTimeRangeTuple(source_type_index, time_range_start, time_range_end)
    #         )

# Adjust the end date since some data of the last day are NOT released at the moment while scraping data
    # datetime_now = datetime.today()
    # datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
    # datetime_yesterday = datetime_today + timedelta(days = -1)
    # datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, CMN.DEF.DEF_TODAY_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_DATA_EXIST_MINUTE)
    # import pdb; pdb.set_trace()
#     datetime_threshold = CMN.get_latest_url_data_datetime(CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_MINUTE)
#     for config in source_type_time_duration_range_list:
#         if config['start'] is None:
#             config['start'] = datetime_today if datetime_now >= datetime_threshold else datetime_yesterday
#         if config['end'] is None:
#             config['end'] = datetime_today if datetime_now >= datetime_threshold else datetime_yesterday
# # Check if the end date should be larger than the start date
#         if config['end'] < config['start']:
#             show_error_and_exit("End Date[%s] should be larger than the Start Date[%s]" % (config['end'], config['start']))
# # Check if the start date is out of range
#         if config['start'] < CMN.DEF.DEF_DATA_SOURCE_START_DATE_CFG[config['index']]:
#             g_logger.warn("Out of range in %s! Chnage start date from %s to %s", CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], CMN.transform_datetime_cfg2string(config['start']), CMN.transform_datetime_cfg2string(CMN.DEF.DEF_DATA_SOURCE_START_DATE_CFG[config['index']]))
#             config['start'] = CMN.DEF.DEF_DATA_SOURCE_START_DATE_CFG[config['index']]
# # Check if the end date is out of range
#         if config['end'] > datetime_threshold:
#             g_logger.warn("Out of range in %s! Chnage end date from %s to %s", CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], CMN.transform_datetime_cfg2string(config['end']), datetime_threshold)
#             config['end'] = datetime_threshold

        # if config['end'] == config['start']:
        #     msg = "%s: %04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], config['start'].year, config['start'].month, config['start'].day)
        # else:
        #     msg = "%s: %04d-%02d-%02d:%04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], config['start'].year, config['start'].month, config['start'].day, config['end'].year, config['end'].month, config['end'].day)
        # g_logger.info(msg)
        # if show_console:
        #     sys.stdout.write("%s\n" % msg)

    # return (source_type_time_duration_range_list, check_result, clone_result)


def check_param():
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
        if param_cfg["company"] is not None:
            param_cfg["company"] = None
            show_warn("The 'company' argument is ignored since it's 'Market' mode")
        if param_cfg["company_from_file"] is not None:
            param_cfg["company_from_file"] = None
            show_warn("The 'company_from_file' argument is ignored since it's 'Market' mode")
    else:
        if param_cfg["company_from_file"] is not None:
            if param_cfg["company"] is not None:
                param_cfg["company"] = None
                show_warn("The 'company' argument is ignored since 'company_from_file' is set")


def setup_param():
# Set source type and time range
    if param_cfg["source_from_file"] is not None:
        g_mgr.set_source_type_time_duration_from_file(param_cfg["source_from_file"], param_cfg["time_duration_type"])
    else:
        source_type_index_list = None
        time_range_start = None
        time_range_end = None
        if param_cfg["source"] is not None:
            source_type_index_str_list = param_cfg["source"].split(",")
            source_type_index_list = []
            for source_type_index_str in source_type_index_str_list:
                source_type_index = int(source_type_index_str)
                if not CMN.FUNC.check_source_type_index_in_range(source_type_index):
                    errmsg = "Unsupported source type index: %d" % source_type_index
                    show_error_and_exit(errmsg)
                source_type_index_list.append(source_type_index)
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
# Set company list. For stock mode only
    if CMN.DEF.IS_FINANCE_STOCK_MODE:
        if param_cfg["company_from_file"] is not None:
            g_mgr.set_company_from_file(param_cfg["company_from_file"])
        elif param_cfg["company"] is not None:
            g_mgr.set_company_from_file(param_cfg["company"])

    g_mgr.need_reserve_old_finance_folder(param_cfg["reserve_old"])
    g_mgr.enable_dry_run(param_cfg["dry_run"])

class MyTestBase(object):

    func_ptr = None
    @classmethod
    def init_func(cls):
        cls.func_ptr = [cls.test1, cls.test2, cls.test3,]
    def __init__(self):
        pass

    @classmethod
    def test1(cls, *args):
        print "base 1: %s" % args[0]
    @classmethod
    def test2(cls, *args):
        print "base 2: %s" % args[0]
    @classmethod
    def test3(cls, *args):
        print "base 3: %s" % args[0]
    def test(self, method, *args):
        self.init_func()
        # print self.__class__.__name__
        (self.func_ptr[method])(*args)

class MyTest(MyTestBase):
    test_value = None
    @classmethod
    def test1(cls, *args):
        print "1: %s" % args[0]
    # def test2(self):
    #     print "2: %s" % args
    @classmethod
    def test3(cls, *args):
        print "3: %s" % args[0]
    def __init__(self):
        self.value = 1
    def test(self,m, *args):
        super(MyTest, self).test(m, *args)
        if self.test_value is None:
            self.test_value = 111111
        print "%d" % self.test_value
        # print self.__class__.__name__
    def test_args1(self, arg1, arg2):
        print "arg1: %s, arg2: %s" % (arg1, arg2)
    def test_args2(self, arg1, arg2, arg3):
        print "arg1: %s, arg2: %s, arg3: %s" % (arg1, arg2, arg3)
    def test_kwargs1(self, arg1, arg2):
        print "arg1: %s, arg2: %s" % (arg1, arg2)
    def test_kwargs2(self, arg1, arg2, arg3):
        print "arg1: %s, arg2: %s, arg3: %s" % (arg1, arg2, arg3)
    @property
    def source_type_index(self):
        print "Fuck"
        return self.source_type_index


def get_ret():
    my_list = [1,2,3]
    return my_list, "fuck"

# import libs.stock.web_scrapy_company_group_set as CompanyGroupSet
if __name__ == "__main__":
    # my_ret = [None] * 5
    # my_ret[2] = 2
    # print my_ret
    # my_list = ["one", "two"]
    # my_test = MyTest()
    # my_test.test(0, "one", "two")
    # my_test.test(1, *my_list)
    # my_list1 = ["one", "two"]
    # my_list2 = ["one", "two", "three"]
    # my_test.test_args1(*my_list1)
    # my_test.test_args2(*my_list2)
    # my_dict1 = {"arg1": "one", "arg2": "two"}
    # my_dict2 = {"arg1": "one", "arg2": "two", "arg3": "three"}
    # my_test.test_kwargs1(**my_dict1)
    # my_test.test_kwargs2(**my_dict2)
    # # print my_test.source_type_index
    # # my_test.test(2, *my_list)
    # # one, two = get_ret()
    # # print one
    # # print two 
    # sys.exit(0)

    # my_list = [1, 2, 3,]
    # my_dict = {"one": 1, "two": 2, "three": 3}
    # my_dict["test"] = "fuck"
    # # my_test(*my_list, **my_dict)
    # my_test1(*my_list, **my_dict)
    # get_whole_company_group_set()
    # import pdb; pdb.set_trace()
    # workday_canlendar_obj = WorkdayCanlendar.WebScrapyWorkdayCanlendar.Instance()
    # datetime_last_cfg = workday_canlendar_obj.get_latest_workday()
    # date_start = workday_canlendar_obj.get_nearest_next_workday(CMN.CLS.FinanceDate(2012, 12, 29))
    # date_end = workday_canlendar_obj.get_nearest_prev_workday(CMN.CLS.FinanceDate(2013, 2, 2))
    # date_iterator = WorkdayCanlendar.WorkdayIterator(date_start, date_end)
    # date_iterator = WorkdayCanlendar.WorkdayNearestIterator(CMN.CLS.FinanceDate(2012, 12, 29), CMN.CLS.FinanceDate(2013, 2, 2))
    # date_iterator = WorkdayCanlendar.WorkdayNearestIterator(CMN.CLS.FinanceDate(2012, 12, 29), CMN.CLS.FinanceDate(2013, 2, 2))
    # for date_cur in date_iterator:
    #     print date_cur
    # timeslice_generator_obj = TimesliceGenerator.WebScrapyTimeSliceGenerator.Instance()
    # import pdb; pdb.set_trace()
    # time_slice_iterable = timeslice_generator_obj.generate_time_slice(0, CMN.CLS.FinanceDate(2015, 4, 21), CMN.CLS.FinanceDate(2016, 6, 22))
    # time_slice_iterable = timeslice_generator_obj.generate_time_slice(1, CMN.CLS.FinanceDate(2015, 4, 21), CMN.CLS.FinanceDate(2016, 6, 22), company_code_number="2347")
    # time_slice_iterable = timeslice_generator_obj.generate_time_slice(2, CMN.CLS.FinanceMonth(2015, 4), CMN.CLS.FinanceMonth(2016, 6))
    # time_slice_iterable = timeslice_generator_obj.generate_time_slice(3, CMN.CLS.FinanceMonth(2015, 4), CMN.CLS.FinanceMonth(2016, 6))
    # time_slice_iterable = timeslice_generator_obj.generate_time_slice(4, CMN.CLS.FinanceQuarter(2015, 4), CMN.CLS.FinanceQuarter(2016, 3))
    # for time_slice in time_slice_iterable:
    #     print time_slice
    # company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_group_set()
    # for company_group_number, company_code_number_list in company_group_set.items():
    #     print "============ company_group_number: %d ============" % company_group_number
    #     for company_code_number in company_code_number_list:
    #         print "%s ;" % company_code_number

    # partial_company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet()
    # company_list = ["3086", "5263",]
    # partial_company_group_set.add_company_list(34, company_list)
    # partial_company_group_set.add_company_group(33)
    # partial_company_group_set.add_company(34, "8450")
    # partial_company_group_set.add_done()
    # for company_group_number, company_code_number_list in partial_company_group_set.items():
    #     print "============ company_group_number: %d ============" % company_group_number
    #     for company_code_number in company_code_number_list:
    #         print "%s ;" % company_code_number

# Parse the parameters and apply to manager class
    init_param()
    parse_param()
    check_param()
    setup_param()

# Reset the file positon of the log file to 0
    if param_cfg["check_result"]:
        CMN.WSL.reset_web_scrapy_logger_content()

# Try to scrap the web data
    show_info("Scrap the data from the website......")
    time_range_start_second = int(time.time())
    # import pdb; pdb.set_trace()
    g_mgr.do_scrapy()
    time_range_end_second = int(time.time())
    show_info("Scrap the data from the website...... DONE!!!")
    time_lapse_msg = u"######### Time Lapse: %d second(s) #########\n" % (time_range_end_second - time_range_start_second)
    show_info(time_lapse_msg)

    error_found = False
# Check if all the csv files are created
    if param_cfg["check_result"]:
        show_info("Let's check error......")
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
        if not error_found:
            datetime_now = datetime.today()
            clone_foldername = CMN.DEF.DEF_CSV_FILE_PATH + "_ok" + CMN.TIME_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
            show_debug("Clone the CSV folder to %s" % clone_foldername)
            subprocess.call(["cp", "-r", CMN.DEF.DEF_CSV_FILE_PATH, clone_foldername])
