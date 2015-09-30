import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyFutureTop3LegalPersonsNetInterest(web_scrapy_base.WebSracpyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        url_format = "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy=%d&choose_mm=%d&choose_dd=%d&datestart=%d%2F%d%2F%d&choose_item=TX+++++"
        csv_filename_format = "web_scrapy_future_top10_dealers_and_legal_persons_%s.csv"
        
        super(WebSracpyFutureTop3LegalPersonsNetInterest, self).__init__(url_format, csv_filename_format, 'utf-8', '.table_f tr', datetime_range_start, datetime_range_end)
        

    def assemble_web_url(self, datetime_cfg):
        url = self.url_format % (datetime_cfg.year, datetime_cfg.month, datetime_cfg.day, datetime_cfg.year, datetime_cfg.month, datetime_cfg.day)
        return url


    def parse_web_data(self, web_data):
        data_list = []
        for tr in g_data[4:6]:
            td = tr.select('td')
            for i in range(1, 9):
                data_list.append(re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text))
        return data_list

    #print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text, td[6].text, td[7].text, td[8].text