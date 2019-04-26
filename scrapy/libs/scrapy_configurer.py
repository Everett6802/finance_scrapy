# -*- coding: utf8 -*-

import re
# from abc import ABCMeta, abstractmethod
import scrapy.common as CMN
g_logger = CMN.LOG.get_logger()


@CMN.CLS.Singleton
class ScrapyConfigurer(object):

    # __metaclass__ = ABCMeta

    def __init__(self):
        # self.finance_mode = None
        self.config_filename = None
        self.config = {}
        self.config_title = set()


    def initialize(self, config_filename=None):
        # import pdb; pdb.set_trace()
        self.config_filename = CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME if (config_filename is None) else config_filename
            
        cur_conf_title = None
        conf_line_list = CMN.FUNC.read_config_file_lines(config_filename)
        for conf_line in conf_line_list:
            if re.match("\[[\w]+\]", conf_line) is not None:
                cur_conf_title = conf_line.strip("[]")
                self.config_title.add(cur_conf_title)
                self.config[cur_conf_title] = {}
            else:
                assert cur_conf_title is not None, "cur_conf_title should NOT be None"
                assert self.config.has_key(cur_conf_title), "The config title[%s] is Unknown" % cur_conf_title
                element_list = conf_line.split("=")
                assert len(element_list) == 2, "The length[%d] of element_list is NOT 2" % len(element_list)
                [key, value] = element_list
                self.config[cur_conf_title][key] = value


    # @staticmethod
    def __get_config_decorator(func):
        def wrapper(self, *args, **kwargs):
            print "start magic"
            func(self, *args, **kwargs)
            print "end magic"
        return wrapper


    @__get_config_decorator
    def get_config(self, title, subtitle=None):
        assert title in self.config_title, "Unknown config title: %s" % title
        if subtitle is None:
            return self.config[title]
        else:
            assert self.config[title].has_key(subtitle), "Unknown config title:subtitle: %s:%s" % (title, subtitle)
            return self.config[title][subtitle]


    # def _get_company(self):
    #     raise NotImplementedError

    @property
    def ConfigFilename(self):
        return self.config_filename


    @property
    def FinanceMode(self):
        # if not hasattr(self, "finance_mode"):
        #     self.finance_mode = int(self.get_config("Common", "finance_mode"))
        # assert self.finance_mode is not None, "Unknown finance mode in Configurer"
        if not hasattr(self, "finance_mode"):
            self.finance_mode = int(self.get_config("Common", "finance_mode"))
        return self.finance_mode


    @property
    def EnableCompanyNotFoundEexception(self):
        if not hasattr(self, "enable_company_not_found_exception"):
            self.enable_company_not_found_exception = int(self.get_config("Common", "enable_company_not_found_exception"))
        return self.enable_company_not_found_exception


    @property
    def Method(self):
        if not hasattr(self, "method"):
            self.method = CMN.FUNC.parse_method_str_to_list(self.get_config("Common", "method"))
            # self.method = [int(method_index_str) for method_index_str in self.get_config("Common", "method").split(",")]
        return self.method


    # @property
    # def TimeType(self):
    #     if not hasattr(self, "time_type"):
    #         self.time_type = int(self.get_config("Common", "time_type"))
    #     return self.time_type


    @property
    def Time(self):
        import pdb; pdb.set_trace()
        if not hasattr(self, "time"):
            # assert self.TimeType == CMN.DEF.DATA_TIME_DURATION_RANGE, "The time_range should be %d, not %d" % (CMN_DEF.DATA_TIME_DURATION_RANGE, self.TimeType)
            time_str = self.get_config("Common", "time")
#             time_range_list = time_range_list_str.split(",")
#             time_range_list_len = len(time_range_list)
#             time_range_start = time_range_end = None
#             if time_range_list_len == 2:
#                 if not time_range_list_str.startswith(","):
# # For time range
#                     time_range_start = CMN.CLS.FinanceTimeBase.from_time_string(time_range_list[0])
#                 time_range_end = CMN.CLS.FinanceTimeBase.from_time_string(time_range_list[1])
#             elif time_range_list_len == 1:
#                 time_range_start = CMN.CLS.FinanceTimeBase.from_time_string(time_range_list[0])
            self.time = CMN.FUNC.parse_time_range_str_to_object(time_str)
        return self.time


    @property
    def Company(self):
        # return self._get_company()
        if not hasattr(self, "company"):
            self.company = self.get_config("Common", "company")
        return self.company


    # @property
    # def CurDatasetSelection(self):
    #     if not hasattr(self, "cur_dataset_selection"):
    #         self.cur_dataset_selection = self.get_config("Dataset", "cur_dataset_selection")
    #     return self.cur_dataset_selection


    # @property
    # def MethodTimeDurationRange(self):
    #     # import pdb; pdb.set_trace()
    #     if not hasattr(self, "method_time_range"):
    #         self.method_time_range = {}
    #         method_description_time_range_dict = self.get_config("MethodTimeDurationRange")
    #         for method_description, time_range_list_str in method_description_time_range_dict.items():
    #             method_index = CMN.FUNC.get_method_index_from_description(method_description.decode(CMN.DEF.URL_ENCODING_UTF8))
    #             self.method_time_range[method_index] = CMN.FUNC.parse_time_range_str_to_object(time_range_list_str)
    #     return self.method_time_range
