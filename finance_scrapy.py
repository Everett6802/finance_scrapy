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
from libs import web_scrapy_mgr as MGR
g_mgr = MGR.WebSracpyMgr()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "-s --source\nDescription: The date source from the website\nDefault: All data sources\nCaution: Only work when Method is USER_DEFINED"
    for index, source in enumerate(CMN.DEF_DATA_SOURCE_INDEX_MAPPING):
        print "  %d: %s" % (index, CMN.DEF_DATA_SOURCE_INDEX_MAPPING[index])
    print "-t --time\nDescription: The time range of the data source\nDefault: Today\nCaution: Only work when Method is USER_DEFINED"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (start_time,end_time): 2015-01-01,2015-09-04"
    print "-m --method\nDescription: The method of setting the parameters\nDefault: TODAY"
    print "  TODAY: Read the today.conf file and only scrap today's data"
    print "  HISTORY: Read the history.conf file and scrap data in the specific time interval"
    print "  USER_DEFINED: User define the data source (1,2,3) and time interval (None for Today)"
    print "--remove_old\nDescription: Remove the old CSV file in %s" % CMN.DEF_CSV_FILE_PATH
    print "--multi_thread\nDescription: Scrap Web data by using multiple threads"
    print "--check_result\nDescription: Check the CSV files after Scraping Web data"
    print "--clone_result\nDescription: Clone the CSV files if no error occurs\nCaution: Only work when --check_result is set"
    print "--do_debug\nDescription: Debug a specific source type only\nCaution: Ignore other parameters when set"
    print "==================================================="


def do_debug(data_source_index):
    g_mgr.do_debug(data_source_index)
    sys.exit(0)


def show_error_and_exit(errmsg):
    sys.stderr.write(errmsg)
    sys.stderr.write("\n")
    g_logger.error(errmsg)
    sys.exit(1)  


def snapshot_result(run_result_str):
    if not os.path.exists(CMN.DEF_SNAPSHOT_FOLDER):
        os.makedirs(CMN.DEF_SNAPSHOT_FOLDER)
    with open(CMN.RUN_RESULT_FILENAME, 'w') as fp:
        fp.write(run_result_str.encode('utf8'))
    datetime_now = datetime.today()
    snapshot_filename = CMN.SNAPSHOT_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
    subprocess.call(["tar", "cvzf", snapshot_filename, CMN.RUN_RESULT_FILENAME, CMN.DEF_CSV_FILE_PATH, WSL.LOG_FILE_PATH])
    subprocess.call(["mv", snapshot_filename, CMN.DEF_SNAPSHOT_FOLDER])
    subprocess.call(["rm", CMN.RUN_RESULT_FILENAME])


def parse_param():
    source_index_list = None
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
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])

        if re.match("(-h|--help)", sys.argv[index]):
            show_usage()
            sys.exit(0)
        elif re.match("(-s|--source)", sys.argv[index]):
            source = sys.argv[index + 1]
            source_index_str_list = source.split(",")
            source_index_list = []
            for source_index_str in source_index_str_list:
                source_index = int(source_index_str)
                if source_index < 0 or source_index >= CMN.DEF_FINANCE_DATA_INDEX_MAPPING_LEN:
                    errmsg = "Unsupported source: %s" % source
                    show_error_and_exit(errmsg)
                source_index_list.append(source_index)
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
                method_index = CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index(method)
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
            data_source_index = int(sys.argv[index + 1])
            do_debug(data_source_index)
            sys.exit(0)
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset

# Set the default value if it is None
    if method_index is None:
        method_index = CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX

# Remove the old data if necessary
    if remove_old:
        shutil.rmtree(CMN.DEF_CSV_FILE_PATH, ignore_errors=True)

# Create the time range list
    config_list = None
    if method_index != CMN.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX:
        if source_index_list is not None or datetime_range_start is not None:
            sys.stdout.write("Ignore other parameters when the method is %s" % CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE[CMN.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX])
        conf_filename = CMN.DEF_TODAY_CONFIG_FILENAME if method_index == CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX else CMN.DEF_HISTORY_CONFIG_FILENAME
        config_list = CMN.parse_config_file(conf_filename)
        if config_list is None:
            show_error_and_exit("Fail to parse the config file: %s" % conf_filename)
    else:
        config_list = []
        if source_index_list is None:
            source_index_list = range(CMN.DEF_FINANCE_DATA_INDEX_MAPPING_LEN)
        import pdb; pdb.set_trace()
        for source_index in source_index_list:
            config_list.append(
                {
                    "index": source_index,
                    "start": datetime_range_start,
                    "end": datetime_range_end,
                }
            )

# Adjust the end date since some data of the last day are NOT released at the moment while scraping data
    datetime_now = datetime.today()
    datetime_today = datetime(datetime_now.year, datetime_now.month, datetime_now.day)
    datetime_yesterday = datetime_today + timedelta(days = -1)
    datetime_threshold = datetime(datetime_today.year, datetime_today.month, datetime_today.day, CMN.DEF_TODAY_DATA_EXIST_HOUR, CMN.DEF_TODAY_DATA_EXIST_MINUTE)
    # import pdb; pdb.set_trace()
    for config in config_list:
        if config['start'] is None:
            config['start'] = datetime_today if datetime_now >= datetime_threshold else datetime_yesterday
        if config['end'] is None:
            config['end'] = datetime_today if datetime_now >= datetime_threshold else datetime_yesterday
# Check if the end date should be larger than the start date
        if config['end'] < config['start']:
            show_error_and_exit("End Date[%s] should be larger than the Start Date[%s]" % (config['end'], config['start']))
        if config['end'] == config['start']:
            msg = "%s: %04d-%02d-%02d" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], config['start'].year, config['start'].month, config['start'].day)
        else:
            msg = "%s: %04d-%02d-%02d:%04d-%02d-%02d" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[config['index']], config['start'].year, config['start'].month, config['start'].day, config['end'].year, config['end'].month, config['end'].day)
        g_logger.info(msg)
        sys.stdout.write(msg)
        sys.stdout.write("\n")

    return (config_list, multi_thread, check_result, clone_result)


if __name__ == "__main__":
# Parse the parameters
    sys.stdout.write("Try to parse the parameters\n")
    # import pdb; pdb.set_trace()
    (config_list, multi_thread, check_result, clone_result) = parse_param()
# Create the folder for CSV files if not exist
    if not os.path.exists(CMN.DEF_CSV_FILE_PATH):
        os.makedirs(CMN.DEF_CSV_FILE_PATH)
# Reset the file positon of the log file to 0
    if check_result:
        if os.path.exists(WSL.LOG_FILE_PATH):
            with open(WSL.LOG_FILE_PATH, "w") as fp:
                fp.seek(0, 0)

# Try to scrap the web data
    sys.stdout.write("Scrap the data from the website......\n")
    time_start_second = int(time.time())
    g_mgr.do_scrapy(config_list, multi_thread)
    time_end_second = int(time.time())
    time_lapse_msg = u"######### Time Lapse: %d second(s) #########\n" % (time_end_second - time_start_second)
    sys.stdout.write("Scrap the data from the website...... DONE.\n" + time_lapse_msg)

    if check_result:
        error_msg_list = []
        sys.stdout.write("Let's check error......\n")
        (file_not_found_list, file_is_empty_list) = g_mgr.check_scrapy(config_list)
        for file_not_found in file_not_found_list:
            error_msg = u"FileNotFound: %s, %s\n" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[file_not_found['index']], file_not_found['filename'])
            sys.stderr.write(error_msg)
            error_msg_list.append(error_msg)
        for file_is_empty in file_is_empty_list:
            error_msg = u"FileIsEmpty: %s, %s\n" % (CMN.DEF_DATA_SOURCE_INDEX_MAPPING[file_is_empty['index']], file_is_empty['filename'])
            sys.stderr.write(error_msg)
            error_msg_list.append(error_msg)
        if len(error_msg_list) != 0:
            run_result_str = time_lapse_msg
            run_result_str += "".join(error_msg_list)
            snapshot_result(run_result_str)
        else:
            if clone_result:
                datetime_now = datetime.today()
                clone_foldername = CMN.DEF_CSV_FILE_PATH + "_ok" + CMN.TIME_FILENAME_FORMAT % (datetime_now.year, datetime_now.month, datetime_now.day, datetime_now.hour, datetime_now.minute)
                g_logger.debug("Clone the CSV folder to %s", clone_foldername)
                subprocess.call(["cp", "-r", CMN.DEF_CSV_FILE_PATH, clone_foldername])
