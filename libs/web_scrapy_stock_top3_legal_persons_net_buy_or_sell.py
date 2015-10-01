import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


class WebSracpyStockTop3LegalPersonsNetBuyOrSell(web_scrapy_base.WebSracpyBase):

    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        data_source_index = 2
        url_format = "http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=1979&mmtemp=9&ddtemp=4&chooseitemtemp=TX+++++&goday=&choose_yy={0}&choose_mm={1}&choose_dd={2}&datestart={0}%2F{1}%2F{2}&choose_item=TX+++++"
        csv_filename_format = CMN.DEF_WEB_SCRAPY_MODULE_NAME_MAPPING[data_source_index] + "_%s.csv"
        
        super(WebSracpyStockTop3LegalPersonsNetBuyOrSell, self).__init__(
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
        res = requests.get("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=20150907&m_date=20150901")
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        # print soup
        for tr in soup.select('.board_trad tr')[2:6]:
            td = tr.select('td')
            print td[0].text, td[1].text, td[2].text, td[3].text 
