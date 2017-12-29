# -*- coding: utf8 -*-

import libs.common as CMN
import libs.base as BASE
g_logger = CMN.WSL.get_web_scrapy_logger()


@CMN.CLS.Singleton
class WebScrapyMarketConfigurer(BASE.CFR_BASE.WebScrapyConfigurerBase):
    finance_mode = CMN.DEF.FINANCE_MODE_MARKET

# CAUTION: The __init__() function can NOT be implemented due to singleton
    # def __init__(self):
    #     pass
