# -*- coding: utf8 -*-

import re
import requests
import csv
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import common as CMN
import common_class as CMN_CLS
import web_scrapy_base
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


# 外資及陸資投資持股統計
class WebScrapyCompanyForeignInvestorsShareholder(web_scrapy_base.WebScrapyBase):

    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        url = self.URL_FORMAT.format(
            *(
                timeslice.year - 1911, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day
            )
        )
        return url


    def __init__(self, datetime_range_start=None, datetime_range_end=None):
        super(WebScrapyCompanyForeignInvestorsShareholder, self).__init__(
            "http://www.twse.com.tw/ch/trading/fund/MI_QFIIS/MI_QFIIS.php?input_date={0}%2F{1}%2F{2}&select2=all&login_btn=%ACd%B8%DF&orderby=SortByStockCode", 
            __file__, 
            # 'big5', 
            # 'table tbody tr', 
            CMN_CLS.ParseURLDataByBS4('big5', 'table tbody tr'),
            datetime_range_start, 
            datetime_range_end
        )


    def _scrape_web_data(self, timeslice, company_code_number):
        url = self.assemble_web_url(timeslice, company_code_number)
        web_data = self.try_get_web_data(url)
        return web_data


    def _parse_web_data(self, web_data):
        data_list = []
        for tr in web_data:
            td = tr.select('td')
            element_list = []
            company_number = "%s" % str(td[0].text).strip(' ')
            if not re.match("^[\d][\d]{2}[\d]$", company_number):
                continue
            element_list.append(company_number)
            for i in range(3, 6): 
                element_list.append(str(CMN.transform_share_number_string_to_board_lot(td[i].text)))
            for i in range(6, 10): 
                element_list.append(td[i].text)
            data_list.append(element_list)

        return data_list
# 證券代號
# 證券名稱
# 國際證券編碼
# 發行股數
# 外資及陸資尚可投資股數
# 全體外資及陸資持有股數
# 外資及陸資尚可投資比率
# 全體外資及陸資持股比率
# 外資及陸資共用法令投資上限比率
# 陸資法令投資上限比率


    def do_debug(self, silent_mode=False):
        # import pdb; pdb.set_trace()
        # res = requests.get("http://www.twse.com.tw/ch/trading/fund/MI_QFIIS/MI_QFIIS.php?input_date=105%2F04%2F12&select2=02&login_btn=%ACd%B8%DF&orderby=SortByStockCode")
        res = CMN.FUNC.request_from_url_and_check_return("http://www.twse.com.tw/ch/trading/fund/MI_QFIIS/MI_QFIIS.php?input_date=105%2F04%2F12&select2=02&login_btn=%ACd%B8%DF&orderby=SortByStockCode")
        # res = requests.get("http://www.twse.com.tw/ch/trading/fund/MI_QFIIS/MI_QFIIS.php?input_date=105%2F04%2F11&select2=01&login_btn=%ACd%B8%DF&orderby=SortByStockCode")
        # # #print res.text
        res.encoding = 'big5'
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tbody tr')
        # import pdb; pdb.set_trace()
        for tr in g_data:
            td = tr.select('td')
            if not silent_mode: print td[0].text, td[1].text, td[2].text, td[3].text, td[4].text, td[5].text, td[6].text, td[7].text, td[8].text, td[9].text
# ==== result: ====
# 1101 台泥 TW0001101004 3,692,175,869 2,729,877,785 962,298,084 73.93 26.06 100.00 100.00
# 1102 亞泥 TW0001102002 3,361,447,198 2,769,219,754 592,227,444 82.38 17.61 100.00 100.00
# 1103 嘉泥 TW0001103000 774,780,548 753,031,825 21,748,723 97.19 2.80 100.00 100.00
# 1104 環泥 TW0001104008 628,289,140 511,446,018 116,843,122 81.40 18.59 100.00 100.00
# 1108 幸福 TW0001108009 404,738,049 394,494,037 10,244,012 97.46 2.53 100.00 100.00
# 1109 信大 TW0001109007 378,900,684 374,484,417 4,416,267 98.83 1.16 100.00 100.00
# 1110 東泥 TW0001110005 572,000,797 556,950,341 15,050,456 97.36 2.63 100.00 100.00
