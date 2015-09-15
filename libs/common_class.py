import re
import threading
from datetime import datetime, timedelta
import common as CMN
from libs.web_sracpy_base import WebSracpyBase


class WebSracpyInf(object):
	def scrapy_web_data(self, select_year, select_month, select_day):
		raise NotImplementedError


class WebSracpyThread(threading.Thread):

	def __init__(self, delegation_obj, datetime_range_start = None, datetime_range_end = None):
		self.delegation_obj = delegation_obj
		self.datetime_range_start = datetime_range_start
		self.datetime_range_end = datetime_range_end
# Check Input
        if self.datetime_range_start is None and self.datetime_range_end is None:
        	raise ValueError("The start and end time should NOT be NULL simultaneously")
        if not isinstance(self.delegation_obj, WebSracpyBase):
        	raise ValueError("delegation_obj should be inheritated from the WebSracpyBase class")

	def run(self):
		self.delegation_obj.do_scrapy()