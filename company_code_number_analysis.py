#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import time
import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
import libs.base.company_profile as CompanyProfile
import libs.base.company_group_set as CompanyGroupSet
import libs.stock.stock_url_time_range as URLTimeRange
g_logger = CMN.LOG.get_logger()


def show_usage():
    print "====================== Usage ======================"
    print "-h --help\nDescription: The usage\nCaution: Ignore other parameters when set"
    print "--cleanup_old_start_time\nDescription: Cleanup all the old company data start time in the disk"
    print "--renew_start_time\nDescription: Acquire all latest company data start time from the web"
    print "--renew_company\nDescription: Acquire all latest company profiles from the web"
    print "--renew\nDescription: Acquire all latest company profiles and then their data start time from the web"
    print "--renew_company_start_time\nDescription: Acquire some latest company data start time from the web"
    print "--lookup_company\nDescription: Lookup some company info by company code number"
    # print "--lookup_company_group\nDescription: Lookup some company group name/number by company code number"
    print "  Format1: Company code number (ex. 2347)"
    print "  Format2: Company code number range (ex. 2100-2200)"
    print "  Format3: Company group number (ex. [Gg]12)"
    print "  Format4: Company code number/number range/group hybrid (ex. 2347,2100-2200,G12,2362,g2,1500-1510)"
    print "--show_group_division_test_result\nDescription: Show the test result about how to divide companies into groups\nCaution: Ignore other parameters when set"
    print "--show_group_division_test_result_detail\nDescription: Show the test result about how to divide companies into groups in detail\nCaution: Ignore other parameters when set"
    for index, source in enumerate(CompanyProfile.COMPANY_GROUP_METHOD_DESCRIPTION_LIST):
        print "  %d: %s" % (index, CompanyProfile.COMPANY_GROUP_METHOD_DESCRIPTION_LIST[index])
    print "--show_group_statistics\nDescription: Show the statistics of each group\nCaution: Ignore other parameters except --renew when set"
    print "--show_group_statistics_detail\nDescription: Show the statistics of each group in detail\nCaution: Ignore other parameters except --renew when set"
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
        # if re.match("(-h|--help)", sys.argv[index]):
        if sys.argv[index] == "--help" or sys.argv[index] == "-h":
            show_usage()
            sys.exit(0)
        elif sys.argv[index] == "--cleanup_old_start_time":
            param_dict["cleanup_old_start_time"] = True
            index_offset = 1
        elif sys.argv[index] == "--renew_company_start_time":
            param_dict["renew_company_start_time"] = sys.argv[index + 1]
            index_offset = 2
        elif sys.argv[index] == "--renew_start_time":
            param_dict["renew_start_time"] = True
            index_offset = 1
        elif sys.argv[index] == "--renew_company":
            param_dict["renew_company"] = True
            index_offset = 1
        elif sys.argv[index] == "--renew":
            param_dict["renew"] = True
            index_offset = 1
        # elif sys.argv[index] == "--lookup_company_group":
        #     company_code_number_list_str = sys.argv[index + 1]
        #     company_code_number_list = company_code_number_list_str.split(",")
        #     param_dict["lookup_company_group"] = company_code_number_list
        #     index_offset = 2
        elif sys.argv[index] == "--lookup_company":
            # company_code_number_list_str = sys.argv[index + 1]
            # company_code_number_list =company_code_number_list_str.split(",")
            # param_dict["lookup_company"] = company_code_number_list
            param_dict["lookup_company"] = sys.argv[index + 1]
            index_offset = 2
        elif sys.argv[index] == "--show_group_division_test_result":
            group_method_number = int(sys.argv[index + 1])
            param_dict["show_group_division_test_result"] = True
            param_dict["group_method_number"] = group_method_number
            param_dict["show_group_division_test_result_detail"] = True if sys.argv[index] == "--show_group_division_test_result_detail" else False
            index_offset = 2
        elif sys.argv[index] == "--show_group_statistics":
            param_dict["show_group_statistics"] = True
            param_dict["show_group_statistics_detail"] = True if sys.argv[index] == "--show_group_statistics_detail" else False
            index_offset = 1
        else:
            show_error_and_exit("Unknown Parameter: %s" % sys.argv[index])
        index += index_offset
    return param_dict


def show_group_statistics(show_detail, company_group_size):
    whole_company_number_in_group_dict = CompanyGroupSet.CompanyGroupSet.get_whole_company_number_in_group_dict()
    # import pdb; pdb.set_trace()
    for company_group_index in range(company_group_size):
        statistics_in_group_message = "Group%02d: %s, Len: %d" % (company_group_index, company_profile.get_company_group_description(company_group_index), len(whole_company_number_in_group_dict[company_group_index]))
        if show_detail:
            statistics_in_group_message = statistics_in_group_message + "; " + ",".join(whole_company_number_in_group_dict[company_group_index])
        print statistics_in_group_message
    print "There are totally %d groups" % company_group_size


def update_global_variable():
    GV.GLOBAL_VARIABLE_UPDATED = True


def get_company_profile_instance():
    return CompanyProfile.CompanyProfile.Instance()    


def get_url_time_range_instance():
    return URLTimeRange.StockURLTimeRange.Instance()    


if __name__ == "__main__":
    # import pdb; pdb.set_trace()
# Parse the parameters
    param_dict = parse_param()
# Initialize the instance
    # company_profile_obj = CompanyProfile.CompanyProfile.Instance()
    # url_time_range_obj = URLTimeRange.StockURLTimeRange.Instance()
    cleanup_old_start_time = False
# Run by argument
    if param_dict.get("cleanup_old_start_time", None) is not None:
        cleanup_old_start_time = True
    # import pdb; pdb.set_trace()
    update_global_variable()

# Renew the company data
    if param_dict.get("renew_company_start_time", None) is not None:
        company_code_number_list = CompanyGroupSet.CompanyGroupSet.to_company_number_list(param_dict["renew_company_start_time"])
        # print "Renew Company Start time:"
        for company_code_number in company_code_number_list:
            get_url_time_range_instance().renew_time_range(company_code_number, cleanup_old_start_time)
        sys.exit(0)
    elif param_dict.get("renew_start_time", None) is not None:
        get_url_time_range_instance().renew_time_range(None, cleanup_old_start_time)
        sys.exit(0)
    elif param_dict.get("renew_company", None) is not None:
        get_company_profile_instance().renew_table()
        sys.exit(0)
    elif param_dict.get("renew", None) is not None:
        get_company_profile_instance().renew_table()
        get_url_time_range_instance().renew_time_range(None, cleanup_old_start_time)
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
        # import pdb; pdb.set_trace()
        company_code_number_list = CompanyGroupSet.CompanyGroupSet.to_company_number_list(param_dict["lookup_company"])
        print "Company Profile:"
        for company_code_number in company_code_number_list:
            company_profile = get_company_profile_instance().lookup_company_profile(company_code_number)
            company_profile_str = u",".join(company_profile)
            print company_profile_str
    # if param_dict.get("lookup_company_group", None) is not None:
    #     company_code_number_list = param_dict["lookup_company_group"]
    #     print "Company Group Name:"
    #     for company_code_number in company_code_number_list:
    #         print u"%s: %s, %s" % (company_code_number, company_profile.lookup_company_group_name(company_code_number), company_profile.lookup_company_group_number(company_code_number))
