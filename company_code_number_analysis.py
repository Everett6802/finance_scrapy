#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import libs.common as CMN
import libs.stock.web_scrapy_company_profile as CompanyProfile
import libs.stock.web_scrapy_company_group_set as CompanyGroupSet
import libs.stock.web_scrapy_url_time_range as URLTimeRange
g_logger = CMN.WSL.get_web_scrapy_logger()


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "--renew_company\nDescription: Acquire the latest company profile from the web"
    print "--renew_company_start_time\nDescription: Acquire the latest company data start time from the web"
    print "--renew\nDescription: Acquire the latest company and then company data start time from the web"
    print "--show_group_division_test_result\nDescription: Show the test result about how to divide companies into groups\nCaution: Ignore other parameters when set"
    print "--show_group_division_test_result_detail\nDescription: Show the test result about how to divide companies into groups in detail\nCaution: Ignore other parameters when set"
    for index, source in enumerate(CompanyProfile.COMPANY_GROUP_METHOD_DESCRIPTION_LIST):
        print "  %d: %s" % (index, CompanyProfile.COMPANY_GROUP_METHOD_DESCRIPTION_LIST[index])
    print "--show_group_statistics\nDescription: Show the statistics of each group\nCaution: Ignore other parameters except --renew when set"
    print "--show_group_statistics_detail\nDescription: Show the statistics of each group in detail\nCaution: Ignore other parameters except --renew when set"
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
        elif re.match("--renew_company_start_time", sys.argv[index]):
            param_dict["renew_company_start_time"] = True
            index_offset = 1
        elif re.match("--renew_table", sys.argv[index]):
            param_dict["renew_table"] = True
            index_offset = 1
        elif re.match("--renew", sys.argv[index]):
            param_dict["renew"] = True
            index_offset = 1
        elif re.match("--show_group_division_test_result", sys.argv[index]):
            group_method_number = int(sys.argv[index + 1])
            param_dict["show_group_division_test_result"] = True
            param_dict["group_method_number"] = group_method_number
            param_dict["show_group_division_test_result_detail"] = True if re.match("--show_group_division_test_result_detail", sys.argv[index]) else False
            index_offset = 2
        elif re.match("--show_group_statistics", sys.argv[index]):
            param_dict["show_group_statistics"] = True
            param_dict["show_group_statistics_detail"] = True if re.match("--show_group_statistics_detail", sys.argv[index]) else False
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


def show_group_statistics(show_detail, company_group_size):
    whole_company_number_in_group_dict = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_number_in_group_dict()
    # import pdb; pdb.set_trace()
    for company_group_index in range(company_group_size):
        statistics_in_group_message = "Group%02d: %s, Len: %d" % (company_group_index, company_profile.get_company_group_description(company_group_index), len(whole_company_number_in_group_dict[company_group_index]))
        if show_detail:
            statistics_in_group_message = statistics_in_group_message + "; " + ",".join(whole_company_number_in_group_dict[company_group_index])
        print statistics_in_group_message
    print "There are totally %d groups" % company_group_size


if __name__ == "__main__":
    # import pdb; pdb.set_trace()
# Parse the parameters
    param_dict = parse_param()
# Initialize the instance
    company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
    url_time_range = URLTimeRange.WebScrapyURLTimeRange.Instance()
# Run by argument
# Renew the company data
    if param_dict.get("renew_company_start_time", None) is not None:
        url_time_range.renew_time_range()
        sys.exit(0)
    elif param_dict.get("renew_company", None) is not None:
        company_profile.renew_table()
        sys.exit(0)
    elif param_dict.get("renew", None) is not None:
        company_profile.renew_table()
        url_time_range.renew_time_range()
        sys.exit(0)

# Show the test result in different division method
    if param_dict.get("show_group_division_test_result", None) is not None:
        company_profile.group_company(param_dict["group_method_number"], param_dict["show_group_division_test_result_detail"])
        sys.exit(0)
# Show the group statistics
    if param_dict.get("show_group_statistics", None) is not None:
        show_group_statistics(param_dict["show_group_statistics_detail"], company_group_size)
        sys.exit(0)

    if param_dict.get("lookup_company", None) is not None:
        company_code_number_list = param_dict["lookup_company"]
        print "Company Profile:"
        for company_code_number in company_code_number_list:
            company_profile = company_profile.lookup_company_profile(company_code_number)
            company_profile_str = u",".join(company_profile)
            print company_profile_str
    if param_dict.get("lookup_company_group", None) is not None:
        company_code_number_list = param_dict["lookup_company_group"]
        print "Company Group Name:"
        for company_code_number in company_code_number_list:
            print u"%s: %s, %s" % (company_code_number, company_profile.lookup_company_group_name(company_code_number), company_profile.lookup_company_group_number(company_code_number))
