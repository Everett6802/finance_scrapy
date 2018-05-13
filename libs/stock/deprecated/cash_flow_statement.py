# -*- coding: utf8 -*-

import re
import requests
# import threading
import collections
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import scrapy_statement_base as ScrapyStatementBase
g_logger = CMN.LOG.get_logger()
# g_lock =  threading.Lock()


# 現金流量表
class CashFlowStatement(ScrapyStatementBase.ScrapyStatementBase):

    # TABLE_FIELD_NOT_INTEREST_TITLE_LIST = [
    #     u"營業活動之現金流量－間接法".encode(CMN.DEF.URL_ENCODING_UTF8), 
    #     u"投資活動之現金流量".encode(CMN.DEF.URL_ENCODING_UTF8), 
    #     u"籌資活動之現金流量".encode(CMN.DEF.URL_ENCODING_UTF8), 
    # ]
    TABLE_FIELD_INTEREST_TITLE_LIST = None
    # TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_ENTRY_START_INDEX = 1
    TABLE_FIELD_INTEREST_ENTRY_LEN = 1
    TABLE_FIELD_INTEREST_ALIAS_TITLE_DICT = None
    TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT = None
    TABLE_FIELD_START_INDEX = 6
    TABLE_FIELD_END_INDEX = None


    @classmethod
    def init_class_customized_variables(cls):
        # import pdb; pdb.set_trace()
# CAUTION: This function MUST be called by the LEAF derived class
        if cls.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            cls._init_statement_field_class_variables(CMN.DEF.CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME)


    @classmethod
    def show_statement_field_dimension(cls, auto_gen_sql_element=True):
        # cls.get_parent_class().init_class_variables() # Caution: This class function should NOT be called by the parent class
        if cls.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            cls._init_statement_field_class_variables(CMN.DEF.CASH_FLOW_STATEMENT_FIELD_NAME_CONF_FILENAME)
        cls._show_statement_field_dimension_internal(CMN.DEF.CASH_FLOW_STATEMENT_INTEREST_FIELD_METADATA_FILENAME, auto_gen_sql_element)


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(CashFlowStatement, self).__init__(**kwargs)


    def _scrape_web_data(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        return self._scrape_web_data_internal(timeslice, company_code_number)


    def _parse_web_statement_field_data(self, web_data):
        return self._parse_web_statement_field_data_internal(web_data, self.TABLE_FIELD_START_INDEX, self.TABLE_FIELD_END_INDEX)


    def _parse_web_data(self, web_data):
        return self._parse_web_data_internal(web_data, self.TABLE_FIELD_START_INDEX, self.TABLE_FIELD_END_INDEX)


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb05?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=3189&year=104&season=02")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        # print g_data
        for index, tr in enumerate(g_data[6:]):
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
# 民國104年第3季
# 單位：新台幣仟元
# 會計項目    104年01月01日至104年09月30日   103年01月01日至103年09月30日
#     金額  金額
# ==== result: ====
# 營業活動之現金流量   
# 繼續營業單位稅前淨利 1,325,694 2,341,860 
# 本期稅前淨利 1,325,694 2,341,860 
# 折舊費用 1,560,671 1,477,991 
# 攤銷費用 15,434 12,036 
# 呆帳費用提列 -11,817 -2,812 
# 透過損益按公允價值衡量金融資產及負債之淨損失 -14,099 -13,241 
# 利息費用 25,628 30,204 
# 利息收入 -44,386 -48,175 
# 處分及報廢不動產 108,450 89 
# 處分投資損失 -31,068 -25,637 
# 非金融資產減損損失 14,165 0 
# 收益費損項目合計 1,622,978 1,430,455 
# 持有供交易之金融資產 17,317 25 
# 應收票據 1,054 61,630 
# 應收帳款 -36,700 -779,587 
# 應收帳款 80,412 56,911 
# 其他應收款 -54,177 -47,294 
# 其他應收款 -333 703 
# 存貨 17,812 -144,248 
# 預付款項 -73,775 -53,758 
# 其他流動資產 3,820 16,497 
# 其他營業資產 10,298 5,223 
# 與營業活動相關之資產之淨變動合計 -34,272 -883,898 
# 應付票據增加 35,028 3,900 
# 應付票據 0 0 
# 應付帳款增加 -274,535 271,955 
# 應付帳款 0 -163 
# 其他應付款增加 -30,702 336,084 
# 負債準備增加 -87 0 
# 預收款項增加 -10,364 26,282 
# 其他流動負債增加 -20,128 -40,608 
# 淨確定福利負債增加 -2,128 -1,994 
# 與營業活動相關之負債之淨變動合計 -302,916 595,456 
# 與營業活動相關之資產及負債之淨變動合計 -337,188 -288,442 
# 調整項目合計 1,285,790 1,142,013 
# 營運產生之現金流入 2,611,484 3,483,873 
# 收取之利息 48,525 46,760 
# 支付之利息 -25,180 -28,955 
# 退還 -448,793 -498,191 
# 營業活動之淨現金流入 2,186,036 3,003,487 
# 投資活動之現金流量   
# 處分備供出售金融資產 46,520 51,619 
# 取得無活絡市場之債務工具投資 0 0 
# 處分無活絡市場之債務工具投資 38,729 7,523 
# 取得以成本衡量之金融資產 0 -50,000 
# 取得不動產 -2,406,764 -1,453,536 
# 處分不動產 1,674 1,387 
# 存出保證金增加 -1,003 0 
# 存出保證金減少 0 1,740 
# 取得無形資產 -17,452 -18,951 
# 投資活動之淨現金流入 -2,338,296 -1,460,218 
# 籌資活動之現金流量   
# 短期借款增加 352,453 465,794 
# 舉借長期借款 311,283 70,000 
# 償還長期借款 -694,869 -660,622 
# 存入保證金增加 7,878 0 
# 存入保證金減少 0 -14,267 
# 籌資活動之淨現金流入 -23,255 -139,095 
# 匯率變動對現金及約當現金之影響 9,331 -194 
# 本期現金及約當現金增加 -166,184 1,403,980 
# 期初現金及約當現金餘額 11,541,615 9,787,827 
# 期末現金及約當現金餘額 11,375,431 11,191,807 
# 資產負債表帳列之現金及約當現金 11,375,431 11,191,807 
