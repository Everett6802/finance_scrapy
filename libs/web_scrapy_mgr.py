from datetime import datetime
import web_scrapy_future_top10_dealers_and_legal_persons


class WebSracpyMgr(object):

    def __init__(self):
        self.max_concurrent_thread_amount = 4
        self.thread_pool_list = []


    def get_future_top10_dealers_and_legal_persons(self):
        web_srapy_obj = web_scrapy_future_top10_dealers_and_legal_persons.WebSracpyFutureTop10DealersAndLegalPersons()
        web_srapy_obj.do_scrapy()
