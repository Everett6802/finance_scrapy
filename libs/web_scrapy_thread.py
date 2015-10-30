# -*- coding: utf8 -*-

import re
import threading
from datetime import datetime, timedelta
import common as CMN
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebScrapyThread(threading.Thread):

    def __init__(self, delegation_obj):
    	super(WebScrapyThread, self).__init__()
        self.delegation_obj = delegation_obj


    def __str__(self):
        return self.delegation_obj.get_description()


    def run(self):
    	# import pdb; pdb.set_trace()
    	g_logger.debug("The thread for[%s] start !!!", self.delegation_obj.get_description())
        self.delegation_obj.scrap_web_to_csv()
        g_logger.debug("The thread for[%s] stop !!!", self.delegation_obj.get_description())
