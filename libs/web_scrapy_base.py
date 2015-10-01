import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyBase(object):

    def __init__(self, url_format, csv_filename_format, encoding, select_flag, description, datetime_range_start=None, datetime_range_end=None):
        self.url_format = url_format
        self.csv_filename_format = csv_filename_format
        self.encoding = encoding
        self.select_flag = select_flag
        self.datetime_range_list = []
        self.description = description

        self.csv_filename = self.csv_filename_format % self. __generate_time_string_filename(datetime_range_start)
        self.csv_filepath = "%s/%s" % (CMN.DEF_CSV_FILE_PATH, self.csv_filename)
        g_logger.debug("Write data to: %s" % self.csv_filepath)

    	self.__generate_time_range_list(datetime_range_start, datetime_range_end)
        g_logger.debug("There are totally %d day(s) to be downloaded" % len(self.datetime_range_list))

        if len(self.datetime_range_list) == 1:
            datetime_oneday = self.datetime_range_list[0]
            self.description = "%s[%04d%02d%02d]" % (
                description, 
                datetime_oneday.year, 
                datetime_oneday.month, 
                datetime_oneday.day
            )
        else:
            datetime_startday = self.datetime_range_list[0]
            datetime_endday = self.datetime_range_list[-1]
            self.description = "%s[%04d%02d%02d-%04d%02d%02d]" % (
                description, 
                datetime_startday.year, 
                datetime_startday.month, 
                datetime_startday.day,
                datetime_endday.year, 
                datetime_endday.month, 
                datetime_endday.day
            )


    def get_description(self):
        return self.description


    def __generate_time_string_filename(self, datetime_cfg=None):
    	if datetime_cfg is None:
    		datetime_cfg = datetime.today()
    	return "%04d%02d" % (datetime_cfg.year, datetime_cfg.month)


    def __get_web_data(self, url):
        res = requests.get(url)
        res.encoding = self.encoding
        # print res.text
        soup = BeautifulSoup(res.text)
        web_data = soup.select(self.select_flag)

        return web_data


    def __generate_time_range_list(self, datetime_range_start=None, datetime_range_end=None):
        datetime_tmp = datetime.today()
        datetime_today = datetime(datetime_tmp.year, datetime_tmp.month, datetime_tmp.day)
    	datetime_start = None
    	datetime_end = None
        if datetime_range_start is None:
        	if datetime_range_end is not None:
        		raise ValueError("datetime_range_start is None but datetime_range_end is NOT None")
        	else:
        		datetime_start = datetime_end = datetime_today
        		g_logger.debug("Only grab the data today[%s-%s-%s]" % (datetime_today.year, datetime_today.month, datetime_today.day))
        else:
        	datetime_start = datetime_range_start
        	if datetime_range_end is not None:
        		datetime_end = datetime_range_end
        	else:
        		datetime_end = datetime_today
        	g_logger.debug("Grab the data from date[%s-%s-%s] to date[%s-%s-%s]" % (datetime_start.year, datetime_start.month, datetime_start.day, datetime_end.year, datetime_end.month, datetime_end.day))

        day_offset = 1
        datetime_offset = datetime_start
        while True: 
            self.datetime_range_list.append(datetime_offset)
            if datetime_offset == datetime_end:
            	break
            datetime_offset = datetime_offset + timedelta(days = day_offset)


    def assemble_web_url(self, datetime_cfg):
        raise NotImplementedError


    def parse_web_data(self, web_data):
        raise NotImplementedError


    def scrap_web_to_csv(self):
        csv_data_list = []
        web_data = None
        # import pdb; pdb.set_trace()
        with open(self.csv_filepath, 'w') as fp:
            fp_writer = csv.writer(fp, delimiter=',')
            filtered_web_data_date = None
            filtered_web_data = None
            for datetime_cfg in self.datetime_range_list:
                url = self.assemble_web_url(datetime_cfg)
                g_logger.debug("Get the data from URL: %s" % url)
                try:
# Grab the data from website and assemble the data to the entry of CSV
                    web_data = self.parse_web_data(self.__get_web_data(url))
                    if web_data is None:
                        raise RuntimeError(url)
                    csv_data = ["%04d-%02d-%02d" % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)] + web_data
                    g_logger.debug("Write the data[%s] to %s" % (csv_data, self.csv_filename))
                    csv_data_list.append(csv_data)
                except Exception as e:
                    g_logger.warn("Fail to scrap URL[%s], due to: %s" % (url, str(e)))
            g_logger.debug("Write %d data to %s" % (len(csv_data_list), self.csv_filepath))
            if len(csv_data_list) > 0:
                fp_writer.writerows(csv_data_list)
