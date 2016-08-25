# -*- coding: utf8 -*-

import libs.common as CMN
import web_scrapy_company_profile as CompanyProfile
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyCompanyGroupSet(object):

    company_profile = None
    whole_company_number_in_group_dict = None

    def __init__(self):
        self.company_number_in_group_dict = None
        self.altered_company_number_in_group_dict = None
        self.is_add_done = False


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def __get_whole_company_number_in_group_dict(cls):
        # import pdb; pdb.set_trace()
        if cls.whole_company_number_in_group_dict is None:
            cls.whole_company_number_in_group_dict = {}
            company_group_size = cls.__get_company_profile().get_company_group_size();
            for company_group_index in range(company_group_size):
                company_number_list = []
                for entry in cls.__get_company_profile().group_iterator(company_group_index):
                    company_number_list.append(entry[CompanyProfile.COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
                cls.whole_company_number_in_group_dict[company_group_index] = company_number_list
        return cls.whole_company_number_in_group_dict


    @staticmethod
    def get_whole_company_group_set():
        company_group_set = WebScrapyCompanyGroupSet()
        company_group_set.add_done();
        return company_group_set;


    def items(self):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        return self.altered_company_number_in_group_dict.items()


    def add_company_list(self, company_group_number, company_code_number_in_group_list):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")
        if self.company_number_in_group_dict is None:
            self.company_number_in_group_dict = {}
        if company_group_number not in self.company_number_in_group_dict:
            self.company_number_in_group_dict[company_group_number] = []
        else:
            if self.company_number_in_group_dict[company_group_number] is None:
                g_logger.error("The company group[%d] has already been set to NULL" % company_group_number)
                raise ValueError("The company group[%d] has already been set to NULL" % company_group_number)

        for company_code_number in company_code_number_in_group_list:
            if company_code_number in self.company_number_in_group_dict[company_group_number]:
                g_logger.warn("The company code number[%s] has already been added to the group[%d]" % (company_code_number, company_group_number))
                continue
            self.company_number_in_group_dict[company_group_number].append(company_code_number)


    def add_company(self, company_group_number, company_code_number):
        company_code_number_in_group_array = []
        company_code_number_in_group_array.append(company_code_number)
        self.add_company_list(company_group_number, company_code_number_in_group_array)


    def add_company(self, company_code_number):
        company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
        add_company(company_group_number, company_code_number)


    def add_company_group(self, company_group_number):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")

        if self.company_number_in_group_dict is None:
            self.company_number_in_group_dict = {}

        if company_group_number in self.company_number_in_group_dict:
            if self.company_number_in_group_dict[company_group_number] is not None:
                g_logger.warn("Select all company group[%d], ignore the original settings......" % company_group_number);
        self.company_number_in_group_dict[company_group_number] = None


    def add_done(self):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")

        self.__setup_for_traverse()
        self.is_add_done = True


    def get_company_number_in_group_list(self, company_group_index):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        if company_group_index not in self.altered_company_number_in_group_dict:
            g_logger.error("The company group index[%d] is NOT found in data structure" % company_group_index)
            raise ValueError("The company group index[%d] is NOT found in data structure" % company_group_index)
        return self.altered_company_number_in_group_dict[company_group_index]


    def __setup_for_traverse(self):
        if self.company_number_in_group_dict is None:
            self.altered_company_number_in_group_dict = self.__get_whole_company_number_in_group_dict()
        else:
            self.altered_company_number_in_group_dict = {}
            for key, value in self.company_number_in_group_dict.items():
                self.altered_company_number_in_group_dict[key] = value if (value is not None) else (self.__get_whole_company_number_in_group_dict())[key]
