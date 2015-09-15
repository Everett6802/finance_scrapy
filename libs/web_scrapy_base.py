import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyBase(Object):

    def __init__(self, start_time_cfg = None):
        self.url_format = None
        self.encoding = None
        self.select_flag = None
        self.time_range_list = []


    def get_web_data(self, url):
        res = requests.get(url)
        res.encoding = self.encoding
        # print res.text
        soup = BeautifulSoup(res.text)
        web_data = soup.select(self.select_flag)

        return web_data


    def generate_time_range_list(self, datetime_range_start=None, datetime_range_end=None):
    	datetime_tmp = datetime.today()
    	datetime_today = datetime(datetime_tmp.year, datetime_tmp.month, datetime_tmp.day)
    	datetime_start = None
    	datetime_end = None
        if start_time_cfg is None:
        	if end_time_cfg is not None:
        		raise ValueError("start_time_cfg is None but end_time_cfg is NOT None")
        	else:
        		datetime_start = datetime_end = datetime_today
        		g_logger.debug("Only grab the data today[%s-%s-%s]" (datetime_today.year, datetime_today.month, datetime_today.day))
        else:
        	# datetime_start = datetime(start_time_cfg['year'], start_time_cfg['month'], start_time_cfg['day'])
        	datetime_start = datetime_range_start
        	if end_time_cfg is not None:
        		# datetime_end = datetime(end_time_cfg['year'], end_time_cfg['month'], end_time_cfg['day'])
        		datetime_end = datetime_range_end
        	else:
        		datetime_end = datetime_today
        	g_logger.debug("Grab the data from date[%s-%s-%s] to date[%s-%s-%s]" (datetime_start.year, datetime_start.month, datetime_start.day, datetime_end.year, datetime_end.month, datetime_end.day))

        day_offset = 1
        while True:
            date_offset = datetime_start + timedelta(days = day_offset)
            self.time_range_list.append(date_offset)
            if date_offset == datetime_end:
            	break


    def assemble_web_url(self, time_cfg):
    	raise NotImplementedError


    def parse_web_data(self):
    	raise NotImplementedError


    def do_scrapy(self, datetime_range_start=None, datetime_range_end=None):
    	self.generate_time_range_list(datetime_range_start, datetime_range_end)
        g_logger.debug("There are totally %d days to be downloaded" % len(self.time_range_list))

    	web_data = None
    	for time_cfg in self.time_range_list:
            url = self.assemble_web_url(time_cfg)
            g_logger.debug("Get the data from URL: %s" url)
            try:
                web_data = self.get_web_data(url)
            except Exception as e:
                g_logger.warn("Fail to scrapy URL[%s], due to: %s" % (url, str(e))

            filtered_web_data_date = time_cfg
            filtered_web_data = self.parse_web_data()
