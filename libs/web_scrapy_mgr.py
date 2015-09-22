from datetime import datetime
import web_scrapy_future_top10_dealers_and_legal_persons
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyMgr(object):

    def __init__(self):
        self.max_concurrent_thread_amount = 4
        self.thread_pool_list = []
        self.web_scrapy_func_ptr = [
            self.__scrap_future_top10_dealers_and_legal_persons,
        ]


    def __scrap_future_top10_dealers_and_legal_persons(self):
        web_srapy_obj = web_scrapy_future_top10_dealers_and_legal_persons.WebSracpyFutureTop10DealersAndLegalPersons()
        web_srapy_obj.scrap_web_to_csv()


    def do_scrapy(self, config_list):
        for config in config_list:
            try:
               (self.web_scrapy_func_ptr[config['index']])()
            except Exception as e:
                g_logger.error("There are totally %d day(s) to be downloaded" % len(self.datetime_range_list))
