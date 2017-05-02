# -*- coding: utf8 -*-

import os
import sys
import re
from datetime import datetime, timedelta
import libs.common as CMN
import web_scrapy_company_profile as CompanyProfile
g_logger = CMN.WSL.get_web_scrapy_logger()


@CMN.CLS.Singleton
class WebScrapyURLTimeRange(object):

    def __init__(self):
        # self.company_profile_lookup = None
        # self.company_listing_date_dict = None
        self.source_type_index_list = CMN.FUNC.get_source_type_index_range_list()


#     def __create_time_range_folder_if_not_exist(self):
#         time_range_root_folderpath = CMN.FUNC.get_config_filepath(CMN.DEF.DEF_COMPANY_TIME_RANGE_FOLDERNAME)
#         CMN.FUNC.create_folder_if_not_exist(time_range_root_folderpath)
#         folderpath_format = time_range_root_folderpath + "%02d"
#         for index in range(self.__get_company_profile().CompanyGroupSize):
#             folderpath = folderpath_format % index
#             # g_logger.debug("Try to create new folder: %s" % folderpath)
#             CMN.FUNC.create_folder_if_not_exist(folderpath)


#     def __delete_time_range_config(self):
#         time_range_root_folderpath = CMN.FUNC.get_config_filepath(CMN.DEF.DEF_COMPANY_TIME_RANGE_FOLDERNAME)
#         CMN.FUNC.remove_folder_if_exist(time_range_root_folderpath)


#     def __scan_company_time_range_start(self, company_code_number, time_slice_generator_cfg):
# # Generate the time slice
#         timeslice_iterable = self._get_timeslice_iterable(**time_slice_generator_cfg)
# # Generate the time slice list
#         for timeslice in timeslice_iterable:
#             url = self.assemble_web_url(timeslice)
#             g_logger.debug("Check the data exist from URL: %s" % url)
#             try:
# # Grab the data from website and assemble the data to the entry of CSV
#                 csv_data_list = self._parse_web_data(self._get_web_data(url))
#             except Exception as e:
#                 g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))
#             else:
#                 if csv_data_list is None:
#                     if web_data_emtpy_time_start is None:
#                         web_data_emtpy_time_start = web_data_emtpy_time_end = timeslice
#                     else:
#                         if web_data_emtpy_time_end.check_continous_time_duration(timeslice):
#                             web_data_emtpy_time_end = timeslice
#                         else:
# # Keep track of the time range in which the web data is empty
#                             self.emtpy_web_data_list.append(
#                                 CMN.CLS.SourceTypeCompanyTimeDurationTuple(
#                                     self.source_type_index,
#                                     CMN.DEF.DATA_TIME_DURATION_RANGE, 
#                                     web_data_emtpy_time_start, 
#                                     web_data_emtpy_time_end
#                                 )
#                             )
#                             web_data_emtpy_time_start = web_data_emtpy_time_end = None
#                             # raise RuntimeError(url)
#                 else:
#                     csv_data_list_each_year.append(csv_data_list)


#     def __scan_time_range_start(self):
# # Update the time range of time slice
#         time_slice_generator_cfg = {"time_duration_start": web2csv_time_duration_update.NewWebStart, "time_duration_end": web2csv_time_duration_update.NewWebEnd,}
#         whole_company_group_set = CompanyGroupSet.WebScrapyCompanyGroupSet.get_whole_company_number_in_group_dict()
#         for company_group_number, company_code_number_list in whole_company_group_set.items():
#             self.__scan_company_time_range_start(company_group_number, time_slice_generator_cfg)


#     def initialize(self):
#         # self.company_profile_lookup = WebScrapyCompanyProfileLookup.Instance()
#         self.company_listing_date_dict = {}


#     def renew_time_range(self, cleanup_old=False):
#         if cleanup_old:
#             self.__delete_time_range_config()
#         self.__create_time_range_folder_if_not_exist()
#         self.__scan_time_range_start()


    def get_time_range_start(self, source_type_index, company_code_number):
        CMN.FUNC.check_source_type_index_in_range(source_type_index)
        # listing_date = self.company_listing_date_dict.get(source_type_index, None)
        # if listing_date is None:
        #     listing_date_str = self.company_profile_lookup.lookup_company_listing_date(source_type_index)
        #     listing_date = self.company_listing_date_dict[source_type_index] = CMN.CLS.FinanceDate(listing_date_str)
        return CMN.CLS.FinanceDate("2000-01-01")


    def get_time_range_end(self, source_type_index, company_code_number):
        CMN.FUNC.check_source_type_index_in_range(source_type_index)
        # if self.last_url_data_date is None:
        #     self.__get_date_range_end(CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_HOUR, CMN.DEF.DEF_TODAY_STOCK_DATA_EXIST_MINUTE)
        return CMN.CLS.FinanceDate("2099-12-31")
