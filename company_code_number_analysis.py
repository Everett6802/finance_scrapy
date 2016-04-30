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
g_lookup = CompanyCodeNumberLookup.WebScrapyCompanyCodeNumberLookup.Instance()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "-g --group\nDescription: Define the group of the company\nCaution: Ignore other parameters when set"
    for index, source in enumerate(CompanyCodeNumberLookup.COMPANY_CODE_NUMBER_GROUP_METHOD_DESCRIPTION_LIST):
        print "  %d: %s" % (index, CompanyCodeNumberLookup.COMPANY_CODE_NUMBER_GROUP_METHOD_DESCRIPTION_LIST[index])
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
        elif re.match("(-g|--group)", sys.argv[index]):
            group_method_number = int(sys.argv[index + 1])
            g_lookup.group_company(group_method_number)
            sys.exit(0)
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset


if __name__ == "__main__":
# Parse the parameters
    # import pdb; pdb.set_trace()
    parse_param()
    time_start_second = int(time.time())
    
    print "%s\n" % g_lookup.lookup_company_info("2347")
    time_end_second = int(time.time())
    print u"######### Time Lapse: %d second(s) #########\n" % (time_end_second - time_start_second)
