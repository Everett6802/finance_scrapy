# -*- coding: utf8 -*-

import libs.common as CMN
import libs.base as BASE
import web_scrapy_company_group_set as CompanyGroupSet
g_logger = CMN.WSL.get_web_scrapy_logger()


@CMN.CLS.Singleton
class WebScrapyStockConfigurer(BASE.CFR_BASE.WebScrapyConfigurerBase):
    
    finance_mode = CMN.DEF.FINANCE_MODE_STOCK
# CAUTION: The __init__() function can NOT be implemented due to singleton
    # def __init__(self):
    #     super(WebScrapyStockConfigurer, self).__init__()


    def _get_company(self):
        if not hasattr(self, "company"):
            self.company = self.get_config("Common", "company")
        return self.company
