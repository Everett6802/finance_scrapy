import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyFutureTop3LegalPersonsOpenInterest(web_scrapy_base.WebSracpyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        data_source_index = 1
        url_format = "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++"
        csv_filename_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%s.csv"
        
        super(WebSracpyFutureTop3LegalPersonsOpenInterest, self).__init__(
            url_format, 
            csv_filename_format, 
            'utf-8', 
            '.table_f tr', 
            CMN.DEF_DATA_SOURCE_INDEX_MAPPING[data_source_index],
            datetime_range_start, 
            datetime_range_end
        )
        

    def assemble_web_url(self, datetime_cfg):
        url = self.url_format.format(*(datetime_cfg.year, datetime_cfg.month, datetime_cfg.day))
        return url


    def parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        data_list = []
        # import pdb; pdb.set_trace()
        for tr in web_data[4:6]:
            td = tr.select('td')
            for i in range(1, 9):
                element = str(re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)).strip(' ')
                data_list.append(element)
        return data_list


    def debug_only(self):
        res = requests.get("http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=2015&DATA_DATE_M=9&DATA_DATE_D=11&syear=2015&smonth=9&sday=10&datestart=2015%2F9%2F11")
        res.encoding = 'utf-8'
        #print res.text
        soup = BeautifulSoup(res.text)
        #g_data = soup.find_all("table", {"class": "table_f"}).select('tr')
        g_data = soup.select('.table_f tr')
        #print "len: %d" % len(g_data)
        for tr in g_data[3:6]:
            th = tr.select('th')
            td = tr.select('td')
            print th[0].text, td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
        print "\n"
        g_data = soup.select('.table_c tr')
        #print "len: %d" % len(g_data)
        for tr in g_data[3:6]:
            th = tr.select('th')
            td = tr.select('td')
            print th[0].text, td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
