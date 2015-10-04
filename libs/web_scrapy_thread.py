# -*- coding: utf8 -*-

import re
import threading
from datetime import datetime, timedelta
import common as CMN


class WebScrapyThread(threading.Thread):

    def __init__(self, delegation_obj):
    	super(WebScrapyThread, self).__init__()
        self.delegation_obj = delegation_obj


    def __str__(self):
        return self.delegation_obj.get_description()


    def run(self):
    	# import pdb; pdb.set_trace()
        self.delegation_obj.scrap_web_to_csv()
