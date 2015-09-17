import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyBase(Object):

    def __init__(self, csv_filename):
    	self.csv_filename = csv_filename
        self.url_format = None
        self.encoding = None
        self.select_flag = None
        self.datetime_range_list = []


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
        		g_logger.debug("Only grab the data today[%s-%s-%s]" (datetime_today.year, datetime_today.month, datetime_today.day))
        else:
        	# datetime_start = datetime(datetime_range_start['year'], datetime_range_start['month'], datetime_range_start['day'])
        	datetime_start = datetime_range_start
        	if datetime_range_end is not None:
        		# datetime_end = datetime(datetime_range_end['year'], datetime_range_end['month'], datetime_range_end['day'])
        		datetime_end = datetime_range_end
        	else:
        		datetime_end = datetime_today
        	g_logger.debug("Grab the data from date[%s-%s-%s] to date[%s-%s-%s]" (datetime_start.year, datetime_start.month, datetime_start.day, datetime_end.year, datetime_end.month, datetime_end.day))

        day_offset = 1
        while True:
            datetime_offset = datetime_start + timedelta(days = day_offset)
            self.datetime_range_list.append(datetime_offset)
            if datetime_offset == datetime_end:
            	break


    def assemble_web_url(self, time_cfg):
    	raise NotImplementedError


    def parse_web_data(self, web_data):
    	raise NotImplementedError


    def do_scrapy(self, datetime_range_start=None, datetime_range_end=None):
    	self.__generate_time_range_list(datetime_range_start, datetime_range_end)
        g_logger.debug("There are totally %d days to be downloaded" % len(self.time_range_list))

        csv_data_list = []
        web_data = None
        with open(self.csv_filename, 'w') as fp:
            fp_writer = csv.writer(fp, delimiter=',')
            filtered_web_data_date = None
            filtered_web_data = None
    	    for datetime_cfg in self.datetime_range_list:
                url = self.assemble_web_url(datetime_cfg)
                g_logger.debug("Get the data from URL: %s" url)
                try:
# Grab the data from website and assemble the data to the entry of CSV
                    csv_data = ["%04d-%02d-%02d" % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)] + self.parse_web_data(self.__get_web_data(url))
                    g_logger.debug("Get the data[%s] to %s" % (csv_data, self.csv_filename))
                    csv_data_list.append(csv_data)
                except Exception as e:
                    g_logger.warn("Fail to scrapy URL[%s], due to: %s" % (url, str(e))
            g_logger.debug("Write %d data to %s" % (len(csv_data_list), self.csv_filename))
            fp_writer.writerows(csv_data_list)
