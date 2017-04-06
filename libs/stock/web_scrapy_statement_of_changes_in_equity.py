# -*- coding: utf8 -*-

import re
import requests
# import threading
import collections
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()
# g_lock =  threading.Lock()


# 股東權益變動表
class WebScrapyStatementOfChangesInEquity(WebScrapyStockBase.WebScrapyStockStatementBase):

    TABLE_FIELD_NOT_INTEREST_TITLE_LIST = [
    ]
    TABLE_FIELD_INTEREST_TITLE_LIST = None
    TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_ENTRY_START_INDEX = 1
    TABLE_FIELD_INTEREST_ENTRY_LEN = 15
    TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = None
    TABLE_FIELD_START_INDEX = 5
    TABLE_FIELD_END_INDEX = None # 13


    @classmethod
    def init_class_variables(cls):
        if cls.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            cls._init_statement_field_class_variables(CMN.DEF.DEF_STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME)
            # cls.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT[u"　母公司暨子公司所持有之母公司庫藏股股數（單位：股）".encode(CMN.DEF.URL_ENCODING_UTF8)] = [1, 3, 5]


    @classmethod
    def show_statement_field_dimension(cls, auto_gen_sql_element=True):
        cls.init_class_variables()
        cls._show_statement_field_dimension_internal(CMN.DEF.DEF_STATEMENT_OF_CHANGES_IN_EQUITY_INTEREST_FIELD_METADATA_FILENAME, auto_gen_sql_element)


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyStatementOfChangesInEquity, self).__init__(__file__, **kwargs)


    def _parse_web_statement_field_data(self, web_data):
        return super(WebScrapyStatementOfChangesInEquity, self)._parse_web_statement_field_data_internal(web_data, self.TABLE_FIELD_START_INDEX, self.TABLE_FIELD_END_INDEX)


    def _parse_web_data(self, web_data):
        return super(WebScrapyStatementOfChangesInEquity, self)._parse_web_data_internal(web_data, self.TABLE_FIELD_START_INDEX, self.TABLE_FIELD_END_INDEX)


    def do_debug(self, silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb06?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=3189&year=104&season=02")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        # print g_data
# Title
        title_td = g_data[4].select('td')
        title_td_len = len(title_td)
        title_line = ""
        for title_i in range(0, title_td_len):
            mobj = re.match(r"^[\s]{0,}([\w]+)", title_td[title_i].text, re.U)
            if mobj is None:
                raise ValueError(u"Title Field[%d] Incorrect format: %s" % (title_i, title_td[title_i].text))
            title_line += u"%s " % mobj.group(1)
        print u"Title: %s" % title_line
        # Content
        for index, tr in enumerate(g_data[5:13]):
        #      print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            td_len = len(td)
            mobj = re.match(r"^[\s]{0,}([\w]+)", td[0].text, re.U)
            if mobj is None:
                raise ValueError(u"Line[%d] Incorrect format: %s" % (index, td[0].text))
            line = u"%s " % mobj.group(1)
            for i in range(1, td_len):
                line += u"%s " % str(td[i].text).strip(" ")
            print u"%s" % line
# 民國104年前3季
# 單位：新台幣仟元
# ==== result: ====
# Title: 普通股股本 股本合計 資本公積 法定盈餘公積 特別盈餘公積 未分配盈餘 保留盈餘合計 國外營運機構財務報表換算之兌換差額 備供出售金融資產未實現 其他權益項目合計 庫藏股票 歸屬於母公司業主之權益總計 非控制權益 權益總額 
# 期初餘額 4,460,000 4,460,000 5,939,819 2,687,890 0 14,030,597 16,718,487 255,009 24,694 279,703  27,398,009 2,654,765 30,052,774 
# 提列法定盈餘公積 0 0 0 361,733 0 -361,733 0 0 0 0  0 0 0 
# 普通股現金股利 0 0 0 0 0 -1,784,000 -1,784,000 0 0 0  -1,784,000 0 -1,784,000 
# 本期淨利 0 0 0 0 0 1,217,957 1,217,957 0 0 0  1,217,957 -86,023 1,131,934 
# 本期其他綜合損益 0 0 0 0 0 0 0 -68,771 -24,694 -93,465  -93,465 -49,363 -142,828 
# 本期綜合損益總額 0 0 0 0 0 1,217,957 1,217,957 -68,771 -24,694 -93,465  1,124,492 -135,386 989,106 
# 權益增加 0 0 0 361,733 0 -927,776 -566,043 -68,771 -24,694 -93,465  -659,508 -135,386 -794,894 
# 期末餘額 4,460,000 4,460,000 5,939,819 3,049,623 0 13,102,821 16,152,444 186,238 0 186,238  26,738,501 2,519,379 29,257,880 
