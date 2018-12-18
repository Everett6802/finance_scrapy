# -*- coding: utf8 -*-

import re
import collections
import threading
import company_profile as CompanyProfile
import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
g_logger = CMN.LOG.get_logger()
g_lock = threading.Lock()


class CompanyGroupSet(object):

    company_profile = None
    whole_company_number_in_group_dict = None
    whole_company_number_list = None
    whole_stock_exchange_company_number_in_group_dict = None
    whole_stock_exchange_company_number_list = None
    whole_over_the_counter_company_number_in_group_dict = None
    whole_over_the_counter_company_number_list = None


    @classmethod
    def create_instance_from_string(cls, company_word_list_string):
        """
        The argument type:
        Company code number: 2347
        Company code number range: 2100-2200
        Company group number: [Gg]12
        Company code number/number range hybrid: 2347,2100-2200,2362,g2,1500-1510
        """
        company_group_set = cls()
        company_word_list = company_word_list_string.split(",")
        for company_number in company_word_list:
            mobj = re.match("([\d]{4})-([\d]{4})", company_number)
            if mobj is None:
# Check if data is company code/group number
                mobj = re.match("[Gg]([\d]{1,})", company_number)
                if mobj is None:
# Company code number
                    if not re.match("([\d]{4})", company_number):
                        raise ValueError("Unknown company number format: %s" % company_number)
                    company_group_set.add_company(company_number)
                else:
# Compgny group number
                    company_group_number = int(mobj.group(1))
                    company_group_set.add_company_group(company_group_number)
            else:
# Company code number Range
                start_company_number_int = int(mobj.group(1))
                end_company_number_int = int(mobj.group(2))
                number_list = []
                for number in range(start_company_number_int, end_company_number_int + 1):
                    number_list.append("%04d" % number)
                company_group_set.add_company_word_list(number_list)
        company_group_set.add_done()
        return company_group_set


    @classmethod
    def to_company_number_list(cls, company_word_list_string):
        company_group_set = cls.create_instance_from_string(company_word_list_string)
        company_group_set.altered_company_number_in_group_dict
        whole_company_number_list = []
        for _, company_number_list in company_group_set.altered_company_number_in_group_dict.items():
            whole_company_number_list += company_number_list
        return whole_company_number_list


    @classmethod
    def get_company_in_group_number_list(cls, company_group_number):
        company_word_list_string = "g%02d" % company_group_number
        return cls.to_company_number_list(company_word_list_string)


    @classmethod
    def __get_company_profile(cls):
        if cls.company_profile is None:
            cls.company_profile = CompanyProfile.CompanyProfile.Instance()
        return cls.company_profile


    @classmethod
    def check_company_market_type(cls, company_code_number, market_type):
        return (True if (cls.__get_company_profile().lookup_company_market_type_index(company_code_number) == market_type) else False)


    @classmethod
    def __init_whole_company_number_in_group_dict(cls, market_type=CMN.DEF.MARKET_TYPE_NONE):
        whole_company_number_in_group_dict = None
        if market_type == CMN.DEF.MARKET_TYPE_NONE:
            assert (cls.whole_company_number_in_group_dict == None), "whole_company_number_in_group_dict is NOT None"
            whole_company_number_in_group_dict = cls.whole_company_number_in_group_dict = {}
        elif market_type == CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE:
            assert (cls.whole_stock_exchange_company_number_in_group_dict == None), "whole_company_number_in_group_dict is NOT None"
            whole_company_number_in_group_dict = cls.whole_stock_exchange_company_number_in_group_dict = {}
        elif market_type == CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER:
            assert (cls.whole_over_the_counter_company_number_in_group_dict == None), "whole_company_number_in_group_dict is NOT None"
            whole_company_number_in_group_dict = cls.whole_over_the_counter_company_number_in_group_dict = {}
        else:
            raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Unknown market type: %d" % market_type)
#Traverse the whole company
        company_group_size = cls.__get_company_profile().CompanyGroupSize;
        for company_group_index in range(company_group_size):
            company_number_list = []
            for entry in cls.__get_company_profile().group_iterator(company_group_index):
                company_code_number = entry[CompanyProfile.COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]
                if market_type != CMN.DEF.MARKET_TYPE_NONE:
                    if not cls.check_company_market_type(company_code_number, market_type):
                    # if self.__get_company_profile().lookup_company_market_type_index(company_code_number) != market_type:
                        continue
                company_number_list.append(company_code_number)
            whole_company_number_in_group_dict[company_group_index] = company_number_list


    @classmethod
    def __init_whole_company_number_list(cls, market_type=CMN.DEF.MARKET_TYPE_NONE):
        whole_company_number_list = None
        if market_type == CMN.DEF.MARKET_TYPE_NONE:
            assert (cls.whole_company_number_list == None), "whole_company_number_list is NOT None"
            whole_company_number_list = cls.whole_company_number_list = []
        elif market_type == CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE:
            assert (cls.whole_stock_exchange_company_number_list == None), "whole_company_number_list is NOT None"
            whole_company_number_list = cls.whole_stock_exchange_company_number_list = []
        elif market_type == CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER:
            assert (cls.whole_over_the_counter_company_number_list == None), "whole_company_number_in_group_dict is NOT None"
            whole_company_number_list = cls.whole_over_the_counter_company_number_list = []
        else:
            raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Unknown market type: %d" % market_type)
#Traverse the whole company
        for entry in cls.__get_company_profile().iterator():
            company_code_number = entry[CompanyProfile.COMPANY_PROFILE_ENTRY_FIELD_INDEX_COMPANY_CODE_NUMBER]
            if market_type != CMN.DEF.MARKET_TYPE_NONE:
                if not self.check_company_market_type(company_code_number, market_type):
                # if self.__get_company_profile().lookup_company_market_type_index(company_code_number) != market_type:
                    continue
            whole_company_number_list.append(company_code_number)


    @classmethod
    def get_whole_company_number_in_group_dict(cls, market_type=CMN.DEF.MARKET_TYPE_NONE):
        # import pdb; pdb.set_trace()
        whole_company_number_in_group_dict = None
        if market_type == CMN.DEF.MARKET_TYPE_NONE:
            if cls.whole_company_number_in_group_dict is None:
                with g_lock:
                    if cls.whole_company_number_in_group_dict is None:
                        cls.__init_whole_company_number_in_group_dict(market_type)
            whole_company_number_in_group_dict = cls.whole_company_number_in_group_dict
        elif market_type == CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE:
            if cls.whole_stock_exchange_company_number_in_group_dict is None:
                with g_lock:
                    if cls.whole_stock_exchange_company_number_in_group_dict is None:
                        cls.__init_whole_company_number_in_group_dict(market_type)
            whole_company_number_in_group_dict = cls.whole_stock_exchange_company_number_in_group_dict
        elif market_type == CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER:
            if cls.whole_over_the_counter_company_number_in_group_dict is None:
                with g_lock:
                    if cls.whole_over_the_counter_company_number_in_group_dict is None:
                        cls.__init_whole_company_number_in_group_dict(market_type)
            whole_company_number_in_group_dict = cls.whole_over_the_counter_company_number_in_group_dict
        else:
            raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Unknown market type: %d" % market_type)
        return whole_company_number_in_group_dict


    @classmethod
    def get_whole_company_number_list(cls, market_type=CMN.DEF.MARKET_TYPE_NONE):
        # import pdb; pdb.set_trace()
        whole_company_number_list = None
        if market_type == CMN.DEF.MARKET_TYPE_NONE:
            if cls.whole_company_number_list is None:
                with g_lock:
                    if cls.whole_company_number_list is None:
                        self.__init_whole_company_number_list(market_type)
            whole_company_number_list = cls.whole_company_number_list
        elif market_type == CMN.DEF.MARKET_TYPE_STOCK_EXCHANGE:
            if cls.whole_stock_exchange_company_number_list is None:
                with g_lock:
                    if cls.whole_stock_exchange_company_number_list is None:
                        self.__init_whole_company_number_list(market_type)
            whole_company_number_list = cls.whole_stock_exchange_company_number_list
        elif market_type == CMN.DEF.MARKET_TYPE_OVER_THE_COUNTER:
            if cls.whole_over_the_counter_company_number_list is None:
                with g_lock:
                    if cls.whole_over_the_counter_company_number_list is None:
                        self.__init_whole_company_number_list(market_type)
            whole_company_number_list = cls.whole_over_the_counter_company_number_in_group_dict
        else:
            raise CMN.EXCEPTION.WebScrapyIncorrectValueException("Unknown market type: %d" % market_type)
        return whole_company_number_list


    @classmethod
    def get_whole_company_group_set(cls, market_type=CMN.DEF.MARKET_TYPE_NONE):
        company_group_set = cls()
        company_group_set.set_market_type(market_type)
        company_group_set.add_done();
        return company_group_set


    def __init__(self):
        self.company_number_in_group_dict = None
        self.altered_company_number_in_group_dict = None
        self.is_whole = False
        self.is_add_done = False
        # self.check_company_exist = True
        self.company_amount = None
        self.market_type = CMN.DEF.MARKET_TYPE_NONE
        self.enable_company_not_found_exception = GV.ENABLE_COMPANY_NOT_FOUND_EXCEPTION


    def items(self):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        return self.altered_company_number_in_group_dict.items()


    def set_market_type(self, new_market_type):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")
        self.market_type = new_market_type
        g_logger.debug("Switch the market type: %d" % self.market_type)


    def get_market_type(self):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        return self.market_type


    def disable_market_type(self):
        self.set_market_type(CMN.DEF.MARKET_TYPE_NONE)


    def __check_company_in_group_exist(self, company_code_number, company_group_number):
        exist = True
        if not self.__get_company_profile().is_company_exist(company_code_number, company_group_number):
            msg = "The company [%s] of company group number[%s] does NOT exist" % (str(company_code_number), str(company_group_number))
            if self.enable_company_not_found_exception:
                g_logger.error(msg)
                raise CMN.EXCEPTION.WebScrapyNotFoundException(msg)
            else:
                g_logger.warn(msg)
            exist = False
        return exist


    def __add_company_in_group_list(self, company_group_number, company_code_number_in_group_list, need_check_company=True):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")
        if not self.__check_company_group_number_in_range(company_group_number):
            return
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
# Check if the company exist and company exist in the group
            if need_check_company and not self.__check_company_in_group_exist(company_code_number, company_group_number):
                continue
            if company_code_number in self.company_number_in_group_dict[company_group_number]:
                g_logger.warn("The company code number[%s] has already been added to the group[%s]" % (str(company_code_number), str(company_group_number)))
                continue
            if self.market_type != CMN.DEF.MARKET_TYPE_NONE:
                if not self.check_company_market_type(company_code_number, self.market_type):
                # if self.__get_company_profile().lookup_company_market_type_index(company_code_number) != self.market_type:
                    g_logger.warn("The market type of company code number[%s] is NOT %d" % (str(company_code_number), self.market_type))
                    continue
            self.company_number_in_group_dict[company_group_number].append(company_code_number)


    def __lookup_and_check_company_group_number(self, company_code_number):
        company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number, True)
        if company_group_number is None:
            msg = "The company [%s] does NOT exist" % str(company_code_number)
            if self.enable_company_not_found_exception:
                g_logger.error(msg)
                raise CMN.EXCEPTION.WebScrapyNotFoundException(msg)
            else:
                g_logger.warn(msg)
        return company_group_number


    def __check_company_group_number_in_range(self, company_group_number):
        company_group_number = int(company_group_number)
        if not self.__get_company_profile().is_company_group_number_in_range(company_group_number):
            errmsg = "The company group number[%d] is Out-of-Range [0, %d)" % (company_group_number, self.__get_company_profile().CompanyGroupSize)
            if self.enable_company_not_found_exception:
                g_logger.error(errmsg)
                raise CMN.EXCEPTION.WebScrapyIncorrectValueException(errmsg)
            else:
                g_logger.warn(errmsg)
                return False
        return True


    def add_company_in_group_list(self, company_group_number, company_code_number_in_group_list):
        return self.__add_company_in_group_list(company_group_number, company_code_number_in_group_list)


    def add_company_list(self, company_code_number_list):
# Categorize the company code number in the list into correct group
        company_code_number_in_group_dict = {}
        for company_code_number in company_code_number_list:
            company_group_number = self.__lookup_and_check_company_group_number(company_code_number)
            if company_group_number is None:
                continue
            if company_code_number_in_group_dict.get(company_group_number, None) is None:
                company_code_number_in_group_dict[company_group_number] = []
            company_code_number_in_group_dict[company_group_number].append(company_code_number)
# Add data by group
        for company_group_number, company_code_number_in_group_list in company_code_number_in_group_dict.items():
            self.__add_company_in_group_list(company_group_number, company_code_number_in_group_list, False)


    def add_company(self, company_code_number, company_group_number=None):
        if company_group_number is None:
            company_group_number = self.__lookup_and_check_company_group_number(company_code_number)
            if company_group_number is None:
                return
        company_code_number_in_group_array = []
        company_code_number_in_group_array.append(company_code_number)
        self.__add_company_in_group_list(company_group_number, company_code_number_in_group_array, False)


    def add_company_group(self, company_group_number):
        if self.is_add_done:
            g_logger.error("The add_done flag has already been set to True");
            raise RuntimeError("The add_done flag has already been set to True")
        if not self.__check_company_group_number_in_range(company_group_number):
            return
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
            self.is_whole = True
            self.altered_company_number_in_group_dict = self.get_whole_company_number_in_group_dict(self.market_type)
        else:
            self.altered_company_number_in_group_dict = {}
            for key, value in self.company_number_in_group_dict.items():
                self.altered_company_number_in_group_dict[key] = value if (value is not None) else (self.get_whole_company_number_in_group_dict(self.market_type))[key]


    def get_sub_company_group_set_in_market_type(self, market_type):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        if self.market_type != CMN.DEF.MARKET_TYPE_NONE:
            raise ValueError("The type of the company group set should be MARKET_TYPE_NODE")
        if market_type == CMN.DEF.MARKET_TYPE_NONE:
            raise ValueError("The type of the sub company group set should be NOT MARKET_TYPE_NODE")  
        sub_company_group_set = CompanyGroupSet()
        for company_group_number, company_code_number_list in self.altered_company_number_in_group_dict.items():
            company_code_number_in_group_array = [] 
            for company_code_number in company_code_number_list:
                if not self.check_company_market_type(company_code_number, market_type):
                    continue
                company_code_number_in_group_array.append(company_code_number)
            sub_company_group_set.add_company_in_group_list(company_group_number, company_code_number_in_group_array)
# No need to set market type, since all the companies of a specific market type are selected 
        # sub_company_group_set.set_market_type(market_type)
        sub_company_group_set.add_done()
        return sub_company_group_set


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
            g_logger.debug("The company amount is less equal than sub company group set amount. Set the sub company group set amount to %d" % self.CompanyAmount)
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
                    sub_company_group_set = CompanyGroupSet()
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


    def is_company_in_group(self, company_code_number, company_group_number=None):
        # import pdb; pdb.set_trace()
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        if company_group_number is None:
            try:
                company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
            except ValueError:
                return False
# Check if the company group exist
        company_code_number_list = self.altered_company_number_in_group_dict.get(company_group_number, None)
        if company_code_number_list is None:
            return False
# Check if the company number exist
        try:
            company_code_number_list.index(company_code_number)
        except ValueError:
            return False
        return True


    def is_current_company_in_group(self, company_code_number, company_group_number=None):
        if self.company_number_in_group_dict is None:
            return False
        if company_group_number is None:
            try:
                company_group_number = self.__get_company_profile().lookup_company_group_number(company_code_number)
            except ValueError:
                return False
# Check if the company group exist
        company_code_number_list = self.company_number_in_group_dict.get(company_group_number, None)
        if company_code_number_list is None:
            return False
# Check if the company number exist
        try:
            company_code_number_list.index(company_code_number)
        except ValueError:
            return False
        return True


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


    @property
    def IsWhole(self):
        if not self.is_add_done:
            g_logger.error("The add_done flag is NOT set to True");
            raise RuntimeError("The add_done flag is NOT set to True")
        return self.is_whole


    @property
    def EnableCompanyNotFoundException(self):
        return self.enable_company_not_found_exception


    @EnableCompanyNotFoundException.setter
    def EnableCompanyNotFoundException(self, enable):
        self.enable_company_not_found_exception = True
