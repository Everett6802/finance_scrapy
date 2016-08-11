#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import shutil
import subprocess
from datetime import datetime, timedelta
from libs import common as CMN
CMN.DEF.IS_FINANCE_MARKET_MODE = CMN.FUNC.is_market_mode()
CMN.DEF.IS_FINANCE_STOCK_MODE = not CMN.DEF.IS_FINANCE_MARKET_MODE
if CMN.DEF.IS_FINANCE_MARKET_MODE:
    from libs.market import web_scrapy_market_mgr as MGR
    g_mgr = MGR.WebSracpyMarketMgr()
else:
    from libs.stock import web_scrapy_stock_mgr as MGR
    g_mgr = MGR.WebSracpyStockMgr()
g_logger = CMN.WSL.get_web_scrapy_logger()

# from libs import web_scrapy_workday_canlendar as WorkdayCanlendar
# from libs import web_scrapy_timeslice_generator as TimesliceGenerator


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "-s --source\nDescription: The date source from the website\nDefault: All data sources\nCaution: Only work when Method is USER_DEFINED"
    for index, source in enumerate(CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING):
        print "  %d: %s" % (index, CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[index])
    print "-t --time\nDescription: The time range of the data source\nDefault: Today\nCaution: Only work when Method is USER_DEFINED"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (start_time,end_time): 2015-01-01,2015-09-04"
    print "-m --method\nDescription: The method of setting the parameters\nDefault: TODAY"
    print "  TODAY: Read the today.conf file and only scrap today's data"
    print "  HISTORY: Read the history.conf file and scrap data in the specific time interval"
    print "  USER_DEFINED: User define the data source (1,2,3) and time interval (None for Today)"
    print "--remove_old\nDescription: Remove the old CSV file in %s" % CMN.DEF.DEF_CSV_FILE_PATH
    print "--multi_thread\nDescription: Scrap Web data by using multiple threads\nCaution: Deprecated"
    print "--check_result\nDescription: Check the CSV files after Scraping Web data"
    print "--clone_result\nDescription: Clone the CSV files if no error occurs\nCaution: Only work when --check_result is set"
    print "--do_debug\nDescription: Debug a specific source type only\nCaution: Ignore other parameters when set"
    print "--run_daily\nDescription: Run daily web-scrapy\nCaution: Ignore other parameters when set"
    print "==================================================="


def do_debug(data_source_type_index):
    g_mgr.do_debug(data_source_type_index)
    sys.exit(0)


def show_error_and_exit(errmsg):
    if show_console:
        sys.stderr.write(errmsg)
        sys.stderr.write("\n")
    g_logger.error(errmsg)
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


def parse_param():
    source_type_index_list = None
    datetime_range_start = None
    datetime_range_end = None
    method_index = None

    argc = len(sys.argv)
    index = 1
    index_offset = None
    remove_old = False
    multi_thread = False
    check_result = False
    clone_result = False
    show_console = True
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])

        if re.match("(-h|--help)", sys.argv[index]):
            show_usage()
            sys.exit(0)
        elif re.match("(-s|--source)", sys.argv[index]):
            source = sys.argv[index + 1]
            source_type_index_str_list = source.split(",")
            source_type_index_list = []
            for source_type_index_str in source_type_index_str_list:
                source_type_index = int(source_type_index_str)
                if source_type_index < 0 or source_type_index >= CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN:
                    errmsg = "Unsupported source: %s" % source
                    show_error_and_exit(errmsg)
                source_type_index_list.append(source_type_index)
            g_logger.debug("Param source: %s", source)
            index_offset = 2
        elif re.match("(-t|--time)", sys.argv[index]):
            time = sys.argv[index + 1]
            g_logger.debug("Param time: %s", time)
            mobj = re.match("([\d]{4})-([\d]{1,2})-([\d]{1,2}),([\d]{4})-([\d]{1,2})-([\d]{1,2})", time)
            if mobj is not None:
                datetime_range_start = datetime(int(mobj.group(1)), int(mobj.group(2)), int(mobj.group(3)))
                datetime_range_end = datetime(int(mobj.group(4)), int(mobj.group(5)), int(mobj.group(6)))
            else:
                mobj = re.match("([\d]{4})-([\d]{1,2})-([\d]{1,2})", time)
                if mobj is not None:
                    datetime_range_start = datetime(int(mobj.group(1)), int(mobj.group(2)), int(mobj.group(3)))
                else:
                    errmsg = "Unsupoorted time: %s" % time
                    show_error_and_exit(errmsg)
            index_offset = 2           
        elif re.match("(-m|--method)", sys.argv[index]):
            method = sys.argv[index + 1]
            # import pdb; pdb.set_trace()
            try:
                method_index = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index(method)
            except ValueError as e:
                errmsg = "Unsupoorted method: %s" % method
                show_error_and_exit(errmsg)
            g_logger.debug("Param method: %s", method)
            index_offset = 2
        elif re.match("--remove_old", sys.argv[index]):
            remove_old = True
            index_offset = 1
        elif re.match("--multi_thread", sys.argv[index]):
            multi_thread = True
            index_offset = 1
        elif re.match("--check_result", sys.argv[index]):
            check_result = True
            index_offset = 1
        elif re.match("--clone_result", sys.argv[index]):
            clone_result = True
            index_offset = 1
        elif re.match("--do_debug", sys.argv[index]):
            data_source_type_index = int(sys.argv[index + 1])
            do_debug(data_source_type_index)
            sys.exit(0)
        elif re.match("--run_daily", sys.argv[index]):
            method_index = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX
            show_console = False
            remove_old = True
            check_result = True
            break
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset

# Set the default value if it is None
    if method_index is None:
        method_index = CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX

# Remove the old data if necessary
    if remove_old:
        shutil.rmtree(CMN.DEF.DEF_CSV_FILE_PATH, ignore_errors=True)

# Create the time range list
    # import pdb; pdb.set_trace()
    source_type_time_range_list = None
    if method_index != CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX:
        if source_type_index_list is not None or datetime_range_start is not None:
            msg = "Ignore other parameters when the method is %s\n" % CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE[CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX]     
            if not show_console:
                g_logger.info(msg)
            else:
                sys.stdout.write(msg)
        conf_filename = CMN.DEF.DEF_TODAY_CONFIG_FILENAME if method_index == CMN.DEF.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX else CMN.DEF.DEF_HISTORY_CONFIG_FILENAME
        source_type_time_range_list = CMN.FUNC.parse_config_file(conf_filename)
        if source_type_time_range_list is None:
            show_error_and_exit("Fail to parse the config file: %s" % conf_filename)
    else:
        source_type_time_range_list = []
        if source_type_index_list is None:
            source_type_index_list = range(CMN.DEF.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING_LEN)
        # import pdb; pdb.set_trace()
        for source_type_index in source_type_index_list:
            source_type_time_range_list.append(
                # {
                #     "source_type_index": source_type_index,
                #     "time_start": datetime_range_start,
                #     "time_end": datetime_range_end,
                # }
                CMN.CLS.SourceTypeTimeRangeTuple(source_type_index, datetime_range_start, datetime_range_end)
            )

# Adjust the end date since some data of the last day are NOT released at the moment while scraping data
    # datetime_now = datetime.today()
    # datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
    # datetime_yesterday = datetime_today + timedelta(days = -1)
    # datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, CMN.DEF.DEF_TODAY_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_DATA_EXIST_MINUTE)
    # import pdb; pdb.set_trace()
#     datetime_threshold = CMN.get_latest_url_data_datetime(CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_MARKET_DATA_EXIST_MINUTE)
#     for config in source_type_time_range_list:
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

        if config['end'] == config['start']:
            msg = "%s: %04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], config['start'].year, config['start'].month, config['start'].day)
        else:
            msg = "%s: %04d-%02d-%02d:%04d-%02d-%02d" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], config['start'].year, config['start'].month, config['start'].day, config['end'].year, config['end'].month, config['end'].day)
        g_logger.info(msg)
        if show_console:
            sys.stdout.write("%s\n" % msg)

    return (source_type_time_range_list, multi_thread, check_result, clone_result, show_console)


# from libs import web_scrapy_logging as WSL
# from libs import web_scrapy_company_group_set as CompanyGroupSet

# class MyClass(object):

#     FUNC_PTR = None
#     def __init__(self):
#         self.get_cls()
#     @classmethod
#     def get_cls(cls):
#         if cls.FUNC_PTR is None:
#             cls.FUNC_PTR = [cls.__test1, cls.__test2]
#         return cls.FUNC_PTR
#     @classmethod
#     def __test1(cls):
#         print "test1"
#     @classmethod
#     def __test2(cls):
#         print "test2"
#     def test(self, index):
#         (self.get_cls()[index])()



if __name__ == "__main__":
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
    # my_class = MyClass()
    # my_class.test(0)
    # my_class.test(1)
    # sys.exit(0)

# Parse the parameters
    # import pdb; pdb.set_trace()
    (source_type_time_range_list, multi_thread, check_result, clone_result, show_console) = parse_param()
# Create the folder for CSV files if not exist
    if not os.path.exists(CMN.DEF.DEF_CSV_FILE_PATH):
        os.makedirs(CMN.DEF.DEF_CSV_FILE_PATH)
# Reset the file positon of the log file to 0
    if check_result:
        if os.path.exists(WSL.LOG_FILE_PATH):
            with open(WSL.LOG_FILE_PATH, "w") as fp:
                fp.seek(0, 0)

# Try to scrap the web data
    if not show_console:
        g_logger.info("Scrap the data from the website......")
    else:
        sys.stdout.write("Scrap the data from the website......\n")
    time_start_second = int(time.time())
    g_mgr.do_scrapy(source_type_time_range_list)
    time_end_second = int(time.time())
    time_lapse_msg = u"######### Time Lapse: %d second(s) #########\n" % (time_end_second - time_start_second)
    if not show_console:
        g_logger.info("Scrap the data from the website...... DONE.")
        g_logger.info(time_lapse_msg)
    else:
        sys.stdout.write("Scrap the data from the website...... DONE.\n" + time_lapse_msg)

    if check_result:
        error_msg_list = []
        if not show_console:
            g_logger.info("Let's check error......")
        else:
            sys.stdout.write("Let's check error......\n")
        (file_not_found_list, file_is_empty_list) = g_mgr.check_scrapy(source_type_time_range_list)
        for file_not_found in file_not_found_list:
            error_msg = u"FileNotFound: %s, %s\n" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[file_not_found['index']], file_not_found['filename'])
            if not show_console:
                g_logger.error(error_msg)
            else:
                sys.stderr.write(error_msg)
            error_msg_list.append(error_msg)
        for file_is_empty in file_is_empty_list:
            error_msg = u"FileIsEmpty: %s, %s\n" % (CMN.DEF.DEF_DATA_SOURCE_INDEX_MAPPING[file_is_empty['index']], file_is_empty['filename'])
            if not show_console:
                g_logger.error(error_msg)
            else:
                sys.stderr.write(error_msg)
            error_msg_list.append(error_msg)
        if len(error_msg_list) != 0:
            run_result_str = time_lapse_msg
            run_result_str += "".join(error_msg_list)
            snapshot_result(run_result_str)
        else:
            if clone_result:
                datetime_now = datetime.today()
                clone_foldername = CMN.DEF.DEF_CSV_FILE_PATH + "_ok" + CMN.TIME_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
                g_logger.debug("Clone the CSV folder to %s", clone_foldername)
                subprocess.call(["cp", "-r", CMN.DEF.DEF_CSV_FILE_PATH, clone_foldername])
