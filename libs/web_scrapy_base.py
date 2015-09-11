import requests
from bs4 import BeautifulSoup

class WebSracpyBase(Object):

    def __init__(self):
        self.url_format = None;

    def scrapy_web_data(self, encoding, select_flag):
        res = requests.get("http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=2015&mmtemp=9&ddtemp=10&chooseitemtemp=ALL&goday=&choose_yy=2015&choose_mm=9&choose_dd=10&datestart=2015%2F9%2F10&choose_item=ALL")
        res.encoding = 'utf-8'
        #print res.text
        soup = BeautifulSoup(res.text)
        web_data = soup.select('.table_f tr')

        return web_data
