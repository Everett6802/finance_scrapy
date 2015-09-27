#! /usr/bin/python

import re
import sys
from libs import common as CMN
from libs import web_scrapy_mgr as MGR
g_mgr = MGR.WebSracpyMgr()
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


def show_usage():
    print "================= Usage =================\n"
    print "-H\n--help\nDescription: The usage\n"
    print "-s\n--source\nDescription: The date source from the website\nDefault: All data sources"
    for index, source in enumerate(CMN.DEF_FINANCE_DATA_INDEX_MAPPING):
        print "  %d: %s\n" % (index, CMN.DEF_FINANCE_DATA_INDEX_MAPPING[index])
    print "-t\n--time\nTime: The time range of the data source\nDefault: Today"
    print "  Format 1 (start_time): 2015-01-01\n"
    print "  Format 2 (start_time end_time): 2015-01-01 2015-09-04\n"
    print "-d\n--definition\nMethod: The time range of the data source\nDefault: TODAY"
    print "  TODAY: Read the today.conf file and only scrap today's data\n"
    print "  HISTORY: Read the history.conf file and scrap data in the specific time interval\n"
    print "  USER_DEFINED: User define the data source and time interval"


def show_error_and_exit(errmsg):
    sys.stderr.write(errmsg)
    g_logger.error(errmsg)
    sys.exit(1)  


def parse_param():
    source_index = None
    timerange_start = None
    timerange_end = None
    definition_index = None

    argc = len(sys.argv)
    index = 1
    while index < argc:
        if not sys.argv[index].startswith('-'):
            raise RuntimeError("Incorrect Parameter format: %s" % sys.argv[index])

        if re.search("(-h|--help)", sys.argv[index]):
            show_usage()
            system.exit(0)
        elif re.search("(-s|--source)", sys.argv[index]):
            source = sys.argv[index + 1]
            try:
                source_index = CMN.DEF_FINANCE_DATA_INDEX_MAPPING.index(source)
            except ValueError:
                errmsg = "Unsupoorted source: %s", source
                show_error_and_exit(errmsg)
            g_logger.debug("Param source: %s", source)
        elif re.search("(-t|--time)", sys.argv[index]):
            time = sys.argv[index + 1]
            g_logger.debug("Param time: %s", time)
            if mobj = re.search("([\d]{4})-([\d]{1,2})-([\d]{1,2})", time):

        elif re.search("(-d|--definition)", sys.argv[index]):
            definition = sys.argv[index + 1]
            try:
                definition_index = CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE.index(definition)
            except ValueError:
                errmsg = "Unsupoorted definition: %s", definition
                show_error_and_exit(errmsg)
            g_logger.debug("Param definition: %s", definition)
        else:
            raise RuntimeError("Unknown Parameter: %s" % sys.argv[index])
        index += 2

    config_list = None
    if definition_index !=  CMD.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE_INDEX:
        if source_index is not None or timerange_start is not None or definition_index is not None:
            sys.stdout.write("Ignore other parameters when the defintion is %s" % CMD.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE[CMN.DEF_WEB_SCRAPY_DATA_SOURCE_TYPE_INDEX])
        config_list = CMN.parse_config_file()
        if config_list is None:
            raise RuntimeError("Fail to parse the config file: %s" % conf_filename)
    else:
        system.out.printf



if __name__ == "__main__":

    # argc = len(sys.argv)
    # if argc < 2:
    #     sys.stderr.write('Usage: %s..........\n' % sys.argv[0])
    #     sys.exit(1)    
    # conf_filename = sys.argv[1]

    config_list = CMN.parse_config(conf_filename)
    if config_list is None:
        raise RuntimeError("Fail to parse the config file: %s" % conf_filename)

    g_mgr.do_scrapy(config_list)

    # g_mgr.scrap_future_top10_dealers_and_legal_persons()
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

