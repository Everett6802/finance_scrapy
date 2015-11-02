# -*- coding: utf8 -*-

import re
import requests
import threading
from datetime import datetime, timedelta
import common as CMN
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebScrapyThread(threading.Thread):

    def __init__(self, delegation_obj):
    	super(WebScrapyThread, self).__init__()
        self.delegation_obj = delegation_obj
        self.ret = CMN.RET_SUCCESS


    def __str__(self):
        return self.delegation_obj.get_description()


    def get_obj(self):
    	return self.delegation_obj


    def get_result(self):
    	return self.ret


    def run(self):
    	# import pdb; pdb.set_trace()
    	# g_logger.debug("The thread for[%s] start !!!", self.delegation_obj.get_description())
        try:
            self.delegation_obj.scrap_web_to_csv()
        except requests.exceptions.Timeout as e:
            self.ret = CMN.RET_FAILURE_TIMEOUT
        # g_logger.debug("The thread for[%s] stop !!!", self.delegation_obj.get_description())
