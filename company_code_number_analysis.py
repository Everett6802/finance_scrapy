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
from libs import web_scrapy_company_profile_lookup as CompanyProfileLookup
# g_lookup = CompanyProfileLookup.WebScrapyCompanyProfileLookup.Instance()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "-g --group\nDescription: Define and show the group of the company\nCaution: Ignore other parameters when set"
    print "--group_detail\nDescription: Define and show the group of the company in detail\nCaution: Ignore other parameters when set"
    for index, source in enumerate(CompanyProfileLookup.COMPANY_GROUP_METHOD_DESCRIPTION_LIST):
        print "  %d: %s" % (index, CompanyProfileLookup.COMPANY_GROUP_METHOD_DESCRIPTION_LIST[index])
    print "--renew_table\nDescription: Acquire the latest data from the web"
    print "--lookup_company\nDescription: Lookup company info by company code number"
    print "--lookup_company_group\nDescription: Lookup company group name/number by company code number"
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
            param_dict["group_detail"] = True if re.match("--group_detail", sys.argv[index]) else False
            index_offset = 2
        elif re.match("--renew_table", sys.argv[index]):
            renew_table = True
            param_dict["renew_table"] = renew_table
            index_offset = 1
        elif re.match("--lookup_company_group", sys.argv[index]):
            company_code_number_list_str = sys.argv[index + 1]
            company_code_number_list = company_code_number_list_str.split(",")
            param_dict["lookup_company_group"] = company_code_number_list
            index_offset = 2
        elif re.match("--lookup_company", sys.argv[index]):
            company_code_number_list_str = sys.argv[index + 1]
            company_code_number_list =company_code_number_list_str.split(",")
            param_dict["lookup_company"] = company_code_number_list
            index_offset = 2

        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset
    return param_dict

from datetime import datetime, timedelta
from libs import common_class as CMN_CLS

if __name__ == "__main__":
    finance_time1 = CMN_CLS.FinanceDate(datetime(2016, 9, 3)) - 1
    finance_time2 = CMN_CLS.FinanceDate(datetime(2016, 9, 2))

    # finance_time1 = CMN_CLS.FinanceQuarter(2016, 4)
    # finance_time2 = CMN_CLS.FinanceQuarter(2016, 3)
    print finance_time1.to_string()
    print finance_time2.to_string()
    if finance_time1 == finance_time2:
        print "Eqaul"
    elif finance_time1 > finance_time2:
        print "Greater"
    elif finance_time1 < finance_time2:
        print "Less"


    sys.exit(0);
    # import pdb; pdb.set_trace()
# Parse the parameters
    param_dict = parse_param()
# Initialize the instance
    g_lookup = CompanyProfileLookup.WebScrapyCompanyProfileLookup.Instance()
# Run by argument
    if param_dict.get("group", None) is not None:
        g_lookup.group_company(param_dict["group"], param_dict["group_detail"])
        sys.exit(0)

    if param_dict.get("renew_table", None) is not None:
        g_lookup.renew_table()
        sys.exit(0)

    if param_dict.get("lookup_company", None) is not None:
        company_code_number_list = param_dict["lookup_company"]
        print "Company Info:"
        for company_code_number in company_code_number_list:
            company_info = g_lookup.lookup_company_info(company_code_number)
            company_info_str = u",".join(company_info)
            print company_info_str
    if param_dict.get("lookup_company_group", None) is not None:
        company_code_number_list = param_dict["lookup_company_group"]
        print "Company Group Name:"
        for company_code_number in company_code_number_list:
            print u"%s: %s, %s" % (company_code_number, g_lookup.lookup_company_group_name(company_code_number), g_lookup.lookup_company_group_number(company_code_number))
