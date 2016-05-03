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
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "-g --group\nDescription: Define the group of the company\nCaution: Ignore other parameters when set"
    for index, source in enumerate(CompanyCodeNumberLookup.COMPANY_GROUP_METHOD_DESCRIPTION_LIST):
        print "  %d: %s" % (index, CompanyCodeNumberLookup.COMPANY_GROUP_METHOD_DESCRIPTION_LIST[index])
    print "--renew_table\nDescription: Acquire the latest data from the web"
    print "--lookup_company\nDescription: Lookup company info by company code number"
    print "  Format 1 (company_code_number): 2347"
    print "  Format 2 (company_code_number): 2317, 2362"
    print "==================================================="


def show_error_and_exit(errmsg):
    sys.stderr.write(errmsg)
    sys.stderr.write("\n")
    g_logger.error(errmsg)
    sys.exit(1)  


def parse_param():
    argc = len(sys.argv)
    index = 1
    index_offset = None
    param_dict = {}

    # import pdb; pdb.set_trace()
    while index < argc:
        if not sys.argv[index].startswith('-'):
            show_error_and_exit("Incorrect Parameter format: %s" % sys.argv[index])

        if re.match("(-h|--help)", sys.argv[index]):
            show_usage()
            sys.exit(0)
        elif re.match("(-g|--group)", sys.argv[index]):
            group_method_number = int(sys.argv[index + 1])
            param_dict["group"] = group_method_number
            index_offset = 2
        elif re.match("--renew_table", sys.argv[index]):
            renew_table = True
            param_dict["renew_table"] = renew_table
            index_offset = 1
        elif re.match("--lookup_company", sys.argv[index]):
            lookup_company_list_str = sys.argv[index + 1]
            lookup_company_list = lookup_company_list_str.split(",")
            param_dict["lookup_company"] = lookup_company_list
            index_offset = 2
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset
    return param_dict


if __name__ == "__main__":
    # import pdb; pdb.set_trace()
# Parse the parameters
    param_dict = parse_param()

    g_lookup = CompanyCodeNumberLookup.WebScrapyCompanyCodeNumberLookup.Instance()

    if param_dict.get("group", None) is not None:
        g_lookup.group_company(param_dict["group"])
        sys.exit(0)

    if param_dict.get("renew_table", None) is not None:
        time_start_second = int(time.time())
        g_lookup.renew_table()
        time_end_second = int(time.time())
        print u"######### Time Lapse: %d second(s) #########\n" % (time_end_second - time_start_second)

    if param_dict.get("lookup_company", None) is not None:
        lookup_company_list = param_dict["lookup_company"]
        for lookup_company in lookup_company_list:
            company_info = g_lookup.lookup_company_info(lookup_company)
            company_info_str = u",".join(company_info)
            print company_info_str
