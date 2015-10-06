#! /usr/bin/python
# -*- coding: utf8 -*-

import re
import sys
import time
from datetime import datetime
from libs import common as CMN
from libs import web_scrapy_mgr as MGR
g_mgr = MGR.WebSracpyMgr()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage"
    print "-s --source\nDescription: The date source from the website\nDefault: All data sources"
    for index, source in enumerate(CMN.DEF_FINANCE_DATA_INDEX_MAPPING):
        print "  %d: %s" % (index, CMN.DEF_FINANCE_DATA_INDEX_MAPPING[index])
    print "-t --time\nTime: The time range of the data source\nDefault: Today"
    print "  Format 1 (start_time): 2015-01-01"
    print "  Format 2 (start_time end_time): 2015-01-01 2015-09-04"
    print "-d --definition\nMethod: The time range of the data source\nDefault: TODAY"
    print "  TODAY: Read the today.conf file and only scrap today's data"
    print "  HISTORY: Read the history.conf file and scrap data in the specific time interval"
    print "  USER_DEFINED: User define the data source (1;2;3) and time interval (None for Today)"
    print "==================================================="


def show_error_and_exit(errmsg):
    sys.stderr.write(errmsg)
    g_logger.error(errmsg)
    sys.exit(1)  


def parse_param():
    source_index_list = None
    datetime_range_start = None
    datetime_range_end = None
    definition_index = None

    argc = len(sys.argv)
    index = 1
    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])

        if re.search("(-h|--help)", sys.argv[index]):
            show_usage()
            sys.exit(0)
        elif re.search("(-s|--source)", sys.argv[index]):
            source = sys.argv[index + 1]
            source_index_str_list = source.split(";")
            source_index_list = []
            for source_index_str in source_index_str_list:
                source_index = int(source_index_str)
                if source_index < 0 or source_index >= CMN.DEF_FINANCE_DATA_INDEX_MAPPING_LEN:
                    errmsg = "Unsupoorted source: %s" % source
                    show_error_and_exit(errmsg)
                source_index_list.append(source_index)
            g_logger.debug("Param source: %s", source)
        elif re.search("(-t|--time)", sys.argv[index]):
            time = sys.argv[index + 1]
            g_logger.debug("Param time: %s", time)
            mobj = re.search("([\d]{4})-([\d]{1,2})-([\d]{1,2}) ([\d]{4})-([\d]{1,2})-([\d]{1,2})", time)
            if mobj is not None:
                datetime_range_start = datetime(int(mobj.group(1)), int(mobj.group(2)), int(mobj.group(3)))
                datetime_range_end = datetime(int(mobj.group(4)), int(mobj.group(5)), int(mobj.group(6)))
            else:
                mobj = re.search("([\d]{4})-([\d]{1,2})-([\d]{1,2})", time)
                if mobj is not None:
                    datetime_range_start = datetime(int(mobj.group(1)), int(mobj.group(2)), int(mobj.group(3)))
                else:
                    errmsg = "Unsupoorted time: %s" % time
                    show_error_and_exit(errmsg)                
        elif re.search("(-d|--definition)", sys.argv[index]):
            definition = sys.argv[index + 1]
            # import pdb; pdb.set_trace()
            try:
                definition_index = CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index(definition)
            except ValueError as e:
                errmsg = "Unsupoorted definition: %s" % definition
                show_error_and_exit(errmsg)
            g_logger.debug("Param definition: %s", definition)
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += 2

# Set the default value is it is None
    if definition_index is None:
        definition_index = CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX

# Create the time range list
    config_list = None
    if definition_index != CMN.DEF_WEB_SCRAPY_DATA_SOURCE_USER_DEFINED_INDEX:
        if source_index_list is not None or datetime_range_start is not None:
            sys.stdout.write("Ignore other parameters when the defintion is %s" % CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE[CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE_INDEX])
        conf_filename = CMN.DEF_TODAY_CONFIG_FILENAME if definition_index == CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TODAY_INDEX else CMN.DEF_HISTORY_CONFIG_FILENAME
        config_list = CMN.parse_config_file(conf_filename)
        if config_list is None:
            show_error_and_exit("Fail to parse the config file: %s" % conf_filename)
    else:
        config_list = []
        if source_index_list is None:
            source_index_list = range(CMN.DEF_FINANCE_DATA_INDEX_MAPPING_LEN)
        for source_index in source_index_list:
            config_list.append(
                {
                    "index": source_index,
                    "start": datetime_range_start,
                    "end": datetime_range_end,
                }
            )
    return config_list


if __name__ == "__main__":
# Parse the parameters
    sys.stdout.write("Try to parse the parameters\n")
    config_list = parse_param()

# Try to scrap the web data
    sys.stdout.write("Scrap the data from the website......\n")
    time_start_second = int(time.time())
    g_mgr.do_scrapy(config_list)
    time_end_second = int(time.time())
    sys.stdout.write("Scrap the data from the website...... DONE.\n######### Time Lapse: %d second(s) #########\n" % (time_end_second - time_start_second))
