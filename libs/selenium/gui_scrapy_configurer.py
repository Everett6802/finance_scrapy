# -*- coding: utf8 -*-

import re
import libs.common as CMN
g_logger = CMN.LOG.get_logger()


@CMN.CLS.Singleton
class GUIScrapyConfigurer(object):

    def __init__(self):
        self.config = {}
        self.config_title = set()


    def initialize(self):
        # import pdb; pdb.set_trace()
        cur_conf_title = None
        conf_line_list = CMN.FUNC.read_config_file_lines(CMN.DEF.FINANCE_SCRAPY_CONF_FILENAME)
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


    def get_config(self, title, subtitle=None):
        assert title in self.config_title, "Unknown config title: %s" % title
        if subtitle is None:
            return self.config[title]
        else:
            assert self.config[title].has_key(subtitle), "Unknown config title:subtitle: %s:%s" % (title, subtitle)
            return self.config[title][subtitle]


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


    @property
    def Company(self):
        # return self._get_company()
        if not hasattr(self, "company"):
            self.company = self.get_config("Common", "company")
        return self.company
