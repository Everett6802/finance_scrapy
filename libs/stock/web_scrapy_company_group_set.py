# -*- coding: utf8 -*-

import collections
import libs.common as CMN
import web_scrapy_company_profile as CompanyProfile
g_logger = CMN.WSL.get_web_scrapy_logger()


class WebScrapyCompanyGroupSet(object):

    company_profile = None
    whole_company_number_in_group_dict = None
    whole_company_number_list = None

    def __init__(self):
        self.company_number_in_group_dict = None
        self.altered_company_number_in_group_dict = None
        self.is_add_done = False
        self.check_company_exist = True
        self.company_amount = None


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.WebScrapyCompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def __init_whole_company_number_in_group_dict(cls):
        assert (cls.whole_company_number_in_group_dict == None), "whole_company_number_in_group_dict is NOT None"

        if cls.whole_company_number_in_group_dict is None:
            cls.whole_company_number_in_group_dict = {}
            company_group_size = cls.__get_company_profile().CompanyGroupSize;
            for company_group_index in range(company_group_size):
                company_number_list = []
                for entry in cls.__get_company_profile().group_iterator(company_group_index):
                    company_number_list.append(entry[CompanyProfile.COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])
                cls.whole_company_number_in_group_dict[company_group_index] = company_number_list
        return cls.whole_company_number_in_group_dict


    @classmethod
    def __init_whole_company_number_list(cls):
        assert (cls.whole_company_number_list == None), "whole_company_number_list is NOT None"

        cls.whole_company_number_list = [];
        for entry in cls.__get_company_profile().iterator():
            cls.whole_company_number_list.append(entry[CompanyProfile.COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER])


    @classmethod
    def get_whole_company_number_in_group_dict(cls):
        # import pdb; pdb.set_trace()
        if cls.whole_company_number_in_group_dict is None:
            cls.__init_whole_company_number_in_group_dict()
        return cls.whole_company_number_in_group_dict


    @classmethod
    def get_whole_company_number_list(cls):
        # import pdb; pdb.set_trace()
        if cls.whole_company_number_list is None:
            cls.__init_whole_company_number_list()
        return cls.whole_company_number_list


    @classmethod
    def get_whole_company_group_set(cls):
        company_group_set = cls()
        company_group_set.add_done();
        return company_group_set


    def items(self):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        return self.altered_company_number_in_group_dict.items()


    def add_company_in_group_list(self, company_group_number, company_code_number_in_group_list):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")
        if self.company_number_in_group_dict is None:
            self.company_number_in_group_dict = {}
        if self.company_number_in_group_dict.get(company_group_number, None) is None:
            self.company_number_in_group_dict[company_group_number] = []
        else:
            if self.company_number_in_group_dict[company_group_number] is None:
                g_logger.error("The company group[%d] has already been set to NULL" % company_group_number)
                raise ValueError("The company group[%d] has already been set to NULL" % company_group_number)

        for company_code_number in company_code_number_in_group_list:
            # import pdb; pdb.set_trace()
            if self.check_company_exist:
                if not self.__get_company_profile().is_company_exist(company_code_number):
                    g_logger.warn("The company of company code number[%s] does NOT exist" % (str(company_code_number), str(company_group_number)))
                    continue
            if company_code_number in self.company_number_in_group_dict[company_group_number]:
                g_logger.warn("The company code number[%s] has already been added to the group[%s]" % (str(company_code_number), str(company_group_number)))
                continue
            self.company_number_in_group_dict[company_group_number].append(company_code_number)


    def add_company_list(self, company_code_number_list):
# Categorize the company code number in the list into correct group
        company_code_number_in_group_dict = {}
        for company_code_number in company_code_number_list:
            company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
            if company_code_number_in_group_dict.get(company_group_number, None) is None:
                company_code_number_in_group_dict[company_group_number] = []
            company_code_number_in_group_dict[company_group_number].append(company_code_number)
# Add data by group
        for company_group_number, company_code_number_in_group_list in company_code_number_in_group_dict.items():
            self.add_company_in_group_list(company_group_number, company_code_number_in_group_list)


    def add_company(self, company_code_number, company_group_number=None):
        if company_group_number is None:
            company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
        company_code_number_in_group_array = []
        company_code_number_in_group_array.append(company_code_number)
        self.add_company_in_group_list(company_group_number, company_code_number_in_group_array)


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
            self.altered_company_number_in_group_dict = self.get_whole_company_number_in_group_dict()
        else:
            self.altered_company_number_in_group_dict = {}
            for key, value in self.company_number_in_group_dict.items():
                self.altered_company_number_in_group_dict[key] = value if (value is not None) else (self.get_whole_company_number_in_group_dict())[key]


    @property
    def CompanyAmount(self):
        # import pdb; pdb.set_trace()
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        if self.company_amount is None:
            self.company_amount = 0;
            for company_group_number, company_number_list in self.altered_company_number_in_group_dict.items():
                for company_number in company_number_list:
                    self.company_amount += 1
        return self.company_amount


    @property
    def CurrentCompanyAmount(self):
        company_amount = 0
        if self.altered_company_number_in_group_dict is not None:
            for company_group_number, company_number_list in self.altered_company_number_in_group_dict.items():
                for company_number in company_number_list:
                    company_amount += 1
        return company_amount


    def get_sub_company_group_set_list(self, sub_company_group_set_amount):
        # import pdb; pdb.set_trace()
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        if sub_company_group_set_amount <= 0:
            raise ValueError("sub_company_group_set_amount should be larger than 0")
        sub_company_group_set_list = []
        sub_company_group_set_amount_list = []
        sub_company_amount = None
        rest_company_amount = None
        if self.CompanyAmount <= sub_company_group_set_amount:
            sub_company_amount = 1
            rest_company_amount = 0
            sub_company_group_set_amount = self.CompanyAmount
            g_logger.debug("The company amount is less than sub company group set amount. Set the sub company group set amount to %d" % self.CompanyAmount)
        else:            
            sub_company_amount = self.CompanyAmount / sub_company_group_set_amount
            rest_company_amount = self.CompanyAmount % sub_company_group_set_amount
        # self.__setup_for_traverse()
        sub_company_group_set = None
        sub_company_group_cnt = 0
        sub_company_amount_in_group_threshold = 0
        sub_company_cnt = 0
        # import pdb; pdb.set_trace()
        for company_group_number, company_code_number_list in self.altered_company_number_in_group_dict.items():
            for company_code_number in company_code_number_list:
                if sub_company_cnt == 0:
                    sub_company_group_set = WebScrapyCompanyGroupSet()
                    sub_company_group_set_list.append(sub_company_group_set)
                    sub_company_amount_in_group_threshold = ((sub_company_amount + 1) if sub_company_group_cnt < rest_company_amount else sub_company_amount)
                    sub_company_group_cnt += 1
                sub_company_group_set.add_company(company_code_number, company_group_number)
                sub_company_cnt += 1
# Add to another group
                if sub_company_cnt == sub_company_amount_in_group_threshold:
                    sub_company_cnt = 0
        for group_index in range(sub_company_group_set_amount):
            sub_company_group_set_list[group_index].add_done()
            sub_company_group_set_amount_list.append(sub_company_group_set_list[group_index].CompanyAmount)
        g_logger.debug("Company Amount list for each sub group: %s" % (",".join([str(i) for i in sub_company_group_set_amount_list])))
        # import pdb; pdb.set_trace()
        return sub_company_group_set_list
