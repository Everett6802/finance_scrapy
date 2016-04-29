#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
# import shutil
# import subprocess
# from datetime import datetime, timedelta
from libs import common as CMN
from libs import web_scrapy_company_code_number_lookup as CompanyCodeNumberLookup
# g_lookup = CompanyCodeNumberLookup.WebScrapyCompanyCodeNumberLookup.Instance()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


def show_usage():
    print "====================== Usage ======================"
    # print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    # print "-s --source\nDescription: The date source from the website\nDefault: All data sources\nCaution: Only work when Method is USER_DEFINED"
    # for index, source in enumerate(CMN.DEF_DATA_SOURCE_INDEX_MAPPING):
    #     print "  %d: %s" % (index, CMN.DEF_DATA_SOURCE_INDEX_MAPPING[index])
    # print "-t --time\nDescription: The time range of the data source\nDefault: Today\nCaution: Only work when Method is USER_DEFINED"
    # print "  Format 1 (start_time): 2015-01-01"
    # print "  Format 2 (start_time,end_time): 2015-01-01,2015-09-04"
    # print "-m --method\nDescr;iption: The method of setting the parameters\nDefault: TODAY"
    # print "  TODAY: Read the today.conf file and only scrap today's data"
    # print "  HISTORY: Read the history.conf file and scrap data in the specific time interval"
    # print "  USER_DEFINED: User define the data source (1,2,3) and time interval (None for Today)"
    # print "--remove_old\nDescription: Remove the old CSV file in %s" % CMN.DEF_CSV_FILE_PATH
    # print "--multi_thread\nDescription: Scrap Web data by using multiple threads"
    # print "--check_result\nDescription: Check the CSV files after Scraping Web data"
    # print "--clone_result\nDescription: Clone the CSV files if no error occurs\nCaution: Only work when --check_result is set"
    # print "--do_debug\nDescription: Debug a specific source type only\nCaution: Ignore other parameters when set"
    # print "--run_daily\nDescription: Run daily web-scrapy\nCaution: Ignore other parameters when set"
    print "==================================================="


def show_error_and_exit(errmsg):
    if show_console:
        sys.stderr.write(errmsg)
        sys.stderr.write("\n")
    g_logger.error(errmsg)
    sys.exit(1)  


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
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


if __name__ == "__main__":
# Parse the parameters
    # import pdb; pdb.set_trace()
    parse_param()
    time_start_second = int(time.time())
    g_lookup = CompanyCodeNumberLookup.WebScrapyCompanyCodeNumberLookup.Instance()
    print "%s\n" % g_lookup.lookup_company_info("2347")
    time_end_second = int(time.time())
    print u"######### Time Lapse: %d second(s) #########\n" % (time_end_second - time_start_second)
