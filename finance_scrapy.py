#! /usr/bin/python

# from datetime import datetime, timedelta
from libs import common as CMN
from libs import web_scrapy_mgr as MGR
g_mgr = MGR.WebSracpyMgr()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()

import csv

if __name__ == "__main__":
    g_mgr.get_future_top10_dealers_and_legal_persons()
    # time_range_list = CMN.get_time_range_list(2014, 3)
    # web_scrapy_logging = WebScrapyLogging()
    # logging.basicConfig(level=logging.INFO)
    # g_logger.info("Fuck You, too ! Damn")
    # logging.warning('Hello world!')
    # logging.info('Hello world again!')
    # logging.error('Fuck')

    # ggm_obj = GrabVolume(2015,'volume.csv')
    # ggm_obj.do_grab()

    # with open('test.csv', 'w') as fp:
    #     a = csv.writer(fp, delimiter=',')
    #     data = [['Me', 'You'], ['293', '219'], ['54', '13']]
    #     a.writerows(data)

# import requests
# from bs4 import BeautifulSoup
# res = requests.get("http://www.twse.com.tw/ch/trading/fund/BFI82U/BFI82U.php?report1=day&input_date=104%2F09%2F08&mSubmit=%ACd%B8%DF&yr=2015&w_date=20150907&m_date=20150901")
# res.encoding = 'big5'
# soup = BeautifulSoup(res.text)
# # print soup
# for tr in soup.select('.board_trad tr')[2:6]:
#     td = tr.select('td')
#     print td[0].text, td[1].text, td[2].text, td[3].text 

# import requests
# from bs4 import BeautifulSoup
# res = requests.get("http://www.taifex.com.tw/chinese/3/7_12_1.asp?goday=&DATA_DATE_Y=2015&DATA_DATE_M=9&DATA_DATE_D=11&syear=2015&smonth=9&sday=10&datestart=2015%2F9%2F11")
# res.encoding = 'utf-8'
# #print res.text
# soup = BeautifulSoup(res.text)
# #g_data = soup.find_all("table", {"class": "table_f"}).select('tr')
# g_data = soup.select('.table_f tr')
# #print "len: %d" % len(g_data)
# for tr in g_data[3:6]:
#     th = tr.select('th')
#     td = tr.select('td')
#     print th[0].text, td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text
# print "\n"
# g_data = soup.select('.table_c tr')
# #print "len: %d" % len(g_data)
# for tr in g_data[3:6]:
#     th = tr.select('th')
#     td = tr.select('td')
#     print th[0].text, td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text

# import re
# import requests
# from bs4 import BeautifulSoup
# res = requests.get("http://www.taifex.com.tw/chinese/3/7_8.asp?pFlag=&yytemp=2015&mmtemp=9&ddtemp=10&chooseitemtemp=ALL&goday=&choose_yy=2015&choose_mm=9&choose_dd=10&datestart=2015%2F9%2F10&choose_item=TX+++++")
# res.encoding = 'utf-8'
# #print res.text
# soup = BeautifulSoup(res.text)
# g_data = soup.select('.table_f tr')
# for tr in g_data[4:6]:
#     td = tr.select('td')
#     for i in range(9):
#         print re.sub('(\(.+\)|[\%\r\t\n])', "", td[i].text)

