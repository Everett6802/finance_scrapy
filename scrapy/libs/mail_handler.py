#! /usr/bin/python
# -*- coding: utf8 -*-

import os
import re
import sys
import smtplib
import ssl
import socket
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import scrapy.common as CMN
g_logger = CMN.LOG.get_logger()


# https://realpython.com/python-send-email/#getting-started
# https://docs.python.org/2/library/email-examples.html#email-examples

class MailHandler(object):

	# DEF_MAIL_ADDRESS = "everett6802@hotmail.com"
	# DEF_MAIL_PASSWORD = ""
	DEF_MAIL_SERVER_URL = "smtp.live.com"
	DEF_MAIL_SERVER_PORT = 587
	DEF_SUBJECT = "Test Only"
	HTML_CONTENT_HEAD = "<html><body><p>"
	HTML_CONTENT_TAIL = "</p></body></html>"
	NEW_COMPANY_SPLITTER = "---"
	NEW_TABLE_SPLITTER = "***"
	NOTIFICATION_SPLITTER = ">>>"
	DAILY_INFO_LINE_NUM_RANGE = [1, 2,] # close interval
	OVERVIEW_INFO_LINE_NUM_RANGE = [3, 4,] # close interval
	DAILY_INFO_CHANGE_COLUMN_NUM = 1


	@classmethod
	def __get_rasie_or_drop_font_color(cls, value):
		font_color = None
		if value >= 0.001:
			font_color = "red"
		elif value <= -0.001:
			font_color = "green"
		else:
			font_color = "black"
		return font_color


	@classmethod
	def parse_value_investment_report(cls, mail_handler, filepath):
		line_list = CMN.FUNC.read_file_lines_ex(filepath)
		# import pdb; pdb.set_trace()
		new_company = True
		company_line_cnt = 0
		for line_index, line in enumerate(line_list):
			new_table = False
			kwargs = {}
			if new_company:
				company_line_cnt = 0
				new_company = False
				kwargs["bold"] = True
				kwargs["font color"] = "blue"
			elif company_line_cnt in [cls.DAILY_INFO_LINE_NUM_RANGE[0], cls.OVERVIEW_INFO_LINE_NUM_RANGE[0],]:
				mail_handler.new_table()
			elif company_line_cnt == cls.DAILY_INFO_LINE_NUM_RANGE[1]:
				kwargs["check_rasie_or_drop"] = [cls.DAILY_INFO_CHANGE_COLUMN_NUM,]
			elif line.startswith(cls.NEW_COMPANY_SPLITTER):
				new_company = True
				kwargs["flush_table"] = True
			elif line.startswith(cls.NEW_TABLE_SPLITTER):
				kwargs["bold"] = True
				kwargs["new_table"] = True
				kwargs["flush_table"] = True
				# import pdb; pdb.set_trace()
			elif line.startswith(cls.NOTIFICATION_SPLITTER):
				kwargs["bold"] = True
				if re.search("postive", line, re.I):
					kwargs["font color"] = "red"
				elif re.search("negative", line, re.I):
					kwargs["font color"] = "green"
				kwargs["flush_table"] = True
			company_line_cnt += 1
			mail_handler.add(line, "html", kwargs=kwargs)



	def __init__(self, address, password, server_url=None, server_port=None, subject=None):
		self.mail_address = address # self.DEF_MAIL_ADDRESS if saddress is None else address
		self.mail_password = password # self.DEF_MAIL_PASSWORD if password is None else password
		self.mail_server_url = self.DEF_MAIL_SERVER_URL if server_url is None else server_url
		self.mail_server_port = self.DEF_MAIL_SERVER_PORT if server_port is None else server_port

		self.mail_server = None
		self.subject = self.DEF_SUBJECT if subject is None else subject
		self.text_content = None
		self.html_content = None
		self.html_table_buffer = None
		self.html_table_head_buffer = None

		self.FORMAT_MESSAGE_FUNC_PTR = {
			"text": self.__format_text_message,
			"html": self.__format_html_message,
		}
		self.NEWLINE_STR_DICT = {
			"text": "\n",
			"html": "<br>",
		}
		self.SUPPORT_MAIL_TYPE = self.FORMAT_MESSAGE_FUNC_PTR.keys()



	def __enter__(self):
		# import pdb; pdb.set_trace()
		try:
# Create an unsecured SMTP connection and encrypt it using .starttls().
# Keep in mind that mail server requires that you connect to port 587 when using .starttls()
			socket.setdefaulttimeout(30)
			self.mail_server = smtplib.SMTP(host=self.mail_server_url, port=self.mail_server_port)
			self.mail_server.starttls()
			self.mail_server.login(self.mail_address, self.mail_password)
		except Exception as e:
			# Print any error messages to stdout
			g_logger.error("Error occurs while logging mail server[%s:%d], due to: %s" % (self.mail_server_url, self.mail_server_port, str(e)))
			raise e
			# print(e)
		# finally:
		# 	self.mail_server.quit() 
		return self


	def __exit__(self, type, msg, traceback):
		# import pdb; pdb.set_trace()
		if self.mail_server is not None:
			self.mail_server.quit() 
			self.mail_server = None
		return False


	def __format_text_message(self):
		assert self.text_content is not None, "self.text_content should NOT be None"
		msg = MIMEText(self.text_content)
		msg['Subject'] = self.subject
		msg['From'] = self.mail_address
		msg['To'] = self.mail_address
		return msg


	def __format_html_message(self):
		assert self.text_content is not None, "self.text_content should NOT be None"
		assert self.html_content is not None, "self.html_content should NOT be None"
		msg = MIMEMultipart("alternative")
		msg['Subject'] = self.subject
		msg['From'] = self.mail_address
		msg['To'] = self.mail_address
# Turn these into plain/html MIMEText objects
		part1 = MIMEText(self.text_content, "plain")
		if self.html_table_buffer is not None:
			self.__flush_table_buffer()
		complete_html_content = self.HTML_CONTENT_HEAD + self.html_content + self.HTML_CONTENT_TAIL
		part2 = MIMEText(complete_html_content, "html")
# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
		msg.attach(part1)
		msg.attach(part2)

		return msg


	def send(self, mail_type="text"):
		'''
		argument:
		 *mail_type: the type of mail content
		  text: plain text
		  html: html

		  default: text
		'''
		# import pdb; pdb.set_trace()
		assert self.mail_server is not None, "self.mail_server should NOT be None"
		msg = (self.FORMAT_MESSAGE_FUNC_PTR[mail_type])()
		self.mail_server.sendmail(self.mail_address, self.mail_address, msg.as_string())


	def clear(self):
		self.text_content = None
		self.html_content = None
		self.html_table_buffer = None
		self.html_table_head_buffer = None


	@property
	def TextContent(self):
		return self.text_content

	@TextContent.setter
	def TextContent(self, text_content):
		self.text_content = text_content


	@property
	def HtmlContent(self):
		return self.html_content

	@HtmlContent.setter
	def HtmlContent(self, html_content):
		self.html_content = html_content


	@property
	def Subject(self):
		return self.subject


	@Subject.setter
	def Subject(self, subject):
		self.subject = subject


	def add(self, data, mail_type="text", newline=True, kwargs=None):
		# assert self.fp is not None, "self.fp should NOT be None"
		assert mail_type in self.SUPPORT_MAIL_TYPE, "Unknown mail type: %s" %  mail_type
		if mail_type in ["text", "html",]:
			content = ((data + self.NEWLINE_STR_DICT["text"]) if newline else data)
			self.TextContent = (self.TextContent + content) if self.TextContent is not None else content
		if mail_type == "html":
# Check if it's required to flush the table buffer
			if kwargs.get("check_rasie_or_drop", None):
				# import pdb; pdb.set_trace()
				data_list = filter(lambda x : len(x) > 0, data.split(" "))
				check_rasie_or_drop_column_list = kwargs["check_rasie_or_drop"]
				for check_rasie_or_drop_column in check_rasie_or_drop_column_list:
					font_color = self.__get_rasie_or_drop_font_color(float(data_list[check_rasie_or_drop_column]))
					data_with_font_color = "<font color=\"%s\">" % font_color + data_list[check_rasie_or_drop_column] + "</font>"
					# data.replace(data_list[check_rasie_or_drop_column], data_with_font_color)
					data_list[check_rasie_or_drop_column] = data_with_font_color
					data = data_list
			if kwargs.get("flush_table", False):
				if self.html_table_buffer is not None:
					self.__flush_table_buffer()
			if self.html_table_buffer is not None:
				self.html_table_buffer.append(data)
			else:
# Move the table entry into buffer
				content = ((data + self.NEWLINE_STR_DICT["html"]) if newline else data)
				if kwargs is not None:
					if kwargs.get("bold", False):
						content = "<strong>" + content + "</strong>"
					if kwargs.get("underline", False):
						content = "<u>" + content + "</u>"
					if kwargs.get("font color", None) is not None:
						content = ("<font color=\"%s\">" % kwargs["font color"]) + content + "</font>"
				self.HtmlContent = (self.HtmlContent + content) if self.HtmlContent is not None else content
			if kwargs.get("new_table", False):
				assert self.html_table_buffer is None, "self.html_table_buffer should be None"
				self.html_table_buffer = []


	def __flush_table_buffer(self):
		assert self.html_content is not None, "self.html_content should NOT be None"
		assert self.html_table_buffer is not None, "self.html_table_buffer should NOT be None"
		# import pdb; pdb.set_trace()
		html_table = ""
		for table_entry_index, table_entry in enumerate(self.html_table_buffer):
			table_element_list = None
			# column_tag = None
			if table_entry_index == 0:
				assert self.html_table_head_buffer is None, "self.html_table_head_buffer should be None"
				self.html_table_head_buffer = table_entry.split("/")
			else:			
				# import pdb; pdb.set_trace()		
				table_element_list = None
				if type(table_entry) is str:
					table_element_list = filter(lambda x : len(x) > 0, table_entry.split(" "))
				elif type(table_entry) is list:
					table_element_list = table_entry
				else:
					raise ValueError("Incorrect table entry type: %s, %s" % (table_entry, type(table_entry)))
				table_element_list[0] = table_element_list[0].rstrip(":")
				if self.html_table_head_buffer is not None:
					html_table_head_buffer_len = len(self.html_table_head_buffer)
					table_element_list_len = len(table_element_list)
					if table_element_list_len - html_table_head_buffer_len == 1:
# Table contains time column
						self.html_table_head_buffer.insert(0, "")
					assert len(self.html_table_head_buffer) == table_element_list_len, "Table column lengths are NOT identical: %d, %d" % (len(self.html_table_head_buffer), table_element_list_len)
					html_table += ("<tr>" + "".join(["<th>" + table_head_element + "</th>" for table_head_element in self.html_table_head_buffer]) + "</tr>")					
					self.html_table_head_buffer = None
				html_table += ("<tr>" + "".join(["<td>" + table_element + "</td>" for table_element in table_element_list]) + "</tr>")
		self.html_content += ("<table border='1'>" + html_table + "</table>")
		self.html_table_buffer = None


	def new_table(self):
		if self.html_table_buffer is not None:
			self.__flush_table_buffer()
		self.html_table_buffer = []
	# def newline(self):
	# 	self.write("")
