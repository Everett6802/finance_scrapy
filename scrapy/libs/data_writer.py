# -*- coding: utf8 -*-

import os
import sys
import scrapy.common as CMN
g_logger = CMN.LOG.get_logger()


class DataWriter(object):

	def __init__(self, filename=None, folder_path=None, file_attribute="w+", remove_file_while_exit=True):
		self.filepath = None
		if filename is not None:
			self.filepath = os.path.join((os.getcwd() if folder_path is None else folder_path), filename)
# file_attribute only take effect for the normal file
		self.file_attribute = file_attribute
		self.fp = None


	def __enter__(self):
		# import pdb; pdb.set_trace()
		if self.filepath is not None:
			self.fp = file(self.filepath, self.file_attribute)
			# self.fp = open("test", self.file_attribute)
		else:
			self.fp = sys.stdout
		return self


	def __exit__(self, type, msg, traceback):
		if self.filepath is not None and self.fp is not None:
			self.fp.close()
		self.fp = None
		return False


	def write(self, data, newline=True, encode=None):
		assert self.fp is not None, "self.fp should NOT be None"
		if encode is not None:
			data = data.encode(CMN.DEF.URL_ENCODING_UTF8)
		self.fp.write((data + "\n") if newline else data)


	def newline(self):
		self.write("")


	@property
	def IsSTDOUT(self):
		return True if self.filepath is None else False


	@property
	def FilePath(self):
		assert not self.IsSTDOUT, "self.filepath is None"
		return self.filepath
