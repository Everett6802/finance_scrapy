# -*- coding: utf8 -*-

import re
import requests
# import threading
import collections
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
# import web_scrapy_company_group_set as CompanyGroupSet
import scrapy_statement_base as ScrapyStatementBase
g_logger = CMN.LOG.get_logger()
# g_lock =  threading.Lock()


# 股東權益變動表
class StatementOfChangesInEquity(ScrapyStatementBase.ScrapyStatementBase):

    # TABLE_FIELD_NOT_INTEREST_TITLE_LIST = [
    #     u"　追溯適用及追溯重編之影響數".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　追溯調整共同控制下組織重組之前手權益".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　期初重編後餘額".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　法定盈餘公積彌補虧損".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　採用權益法認列之關聯企業及合資之變動數".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　提列法定盈餘公積".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　特別股股票股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　法定盈餘公積彌補虧損".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　特別盈餘公積彌補虧損".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　特別盈餘公積迴轉".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　提列特別盈餘公積".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　普通股現金股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　特別股現金股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　資本公積配發股票股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　因合併而產生者".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　資本公積配發現金股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　因發行可轉換公司債（特別股）認列權益組成項目－認股權而產生者".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　普通股股票股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　因合併而產生者".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　資本公積彌補虧損".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　資本公積配發股票股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　資本公積配發現金股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　其他資本公積變動數".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　因受領贈與產生者".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　因發行可轉換公司債（特別股）認列權益組成項目－認股權而產生者".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　　採用權益法認列之關聯企業及合資之變動數".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　本期淨利（淨損）".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　本期其他綜合損益".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　本期綜合損益總額".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　對子公司所有權權益變動".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　處分採用權益法之投資 /子公司".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　實際取得或處分子公司股權價格與帳面價值差額".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　子公司處分母公司股票視同庫藏股交易".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　非控制權益增減".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　其他".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　可轉換公司債轉換".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　庫藏股買回".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　實際取得或處分子公司股權價格與帳面價值差額".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　股份基礎給付".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　非控制權益增減".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　其他".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"　權益增加（減少）總額".encode(CMN.DEF.URL_ENCODING_UTF8),
    # ]
    TABLE_FIELD_INTEREST_TITLE_LIST = None
    # TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_TITLE_LIST_LEN = None
# Column
    # TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST = [
    #     u"普通股股本".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"債券換股權利證書".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"預收股本".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"待分配股票股利".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     # u"股本合計".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     # u"資本公積".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"法定盈餘公積".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"特別盈餘公積".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"未分配盈餘（或待彌補虧損）".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     # u"保留盈餘合計".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"國外營運機構財務報表換算之兌換差額".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     # u"透過其他綜合損益按公允價值衡量之權益工具投資利益（損失）".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     # u"備供出售金融資產未實現（損）益".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"現金流量避險中屬有效避險部分之避險工具利益（損失）".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"國外營運機構淨投資避險中屬有效避險部分之避險工具利益（損失）".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"指定按公允價值衡量之金融負債信用風險變動影響數".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"確定福利計畫再衡量數".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"重估增值".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"與待出售（非流動）資產直接相關之權益".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"其他".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"其他權益項目合計".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"庫藏股票".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"歸屬於母公司業主之權益總計".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     u"非控制權益".encode(CMN.DEF.URL_ENCODING_UTF8),
    #     # u"權益總額".encode(CMN.DEF.URL_ENCODING_UTF8),
    # ]
    URL_SPECIAL_FORMAT = None
    TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST = None
    # TABLE_COLUMN_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_COLUMN_FIELD_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_ENTRY_START_INDEX = 1
    TABLE_FIELD_INTEREST_ENTRY_LEN = 15
    TABLE_FIELD_INTEREST_ALIAS_TITLE_DICT = None
    TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT = None
    TABLE_FIELD_START_INDEX = 3
    TABLE_FIELD_END_INDEX = None # 13
    TABLE_COLUMN_FIELD_TITLE_INDEX = TABLE_FIELD_START_INDEX - 1
    TABLE_COLUMN_FIELD_EXIST = True
    # NEED_URL_SPECIAL_FORMAT_LIST = ["5871", "2841", "2801", "2809", "2812", "2816", "2820", "2823", "2832", "2834", "2836", "2838",]
    # import pdb; pdb.set_trace()
    NEED_URL_SPECIAL_FORMAT_LIST = ["5871", "2841", "g27",]
    for elem in NEED_URL_SPECIAL_FORMAT_LIST:
        if re.match("[Gg]\d{2}", elem):
            NEED_URL_SPECIAL_FORMAT_LIST += BASE.CGS.CompanyGroupSet.get_company_in_group_number_list(int(elem[1:]))
    # import pdb; pdb.set_trace()
    NEED_SPECIAL_WEB_TABLE_DATA_LIST = ["9929",]
    WEB_TABLE_DATA_LEN = 3
    WEB_TABLE_DATA_INDEX = 1
    SPECIAL_WEB_TABLE_DATA_LEN = 4
    SPECIAL_WEB_TABLE_DATA_INDEX = 2


    @classmethod
    def assemble_web_url(cls, timeslice, company_code_number, *args):
        # import pdb; pdb.set_trace()
# CAUTION: This function MUST be called by the LEAF derived class
        if company_code_number in cls.NEED_URL_SPECIAL_FORMAT_LIST:
            url = cls.URL_SPECIAL_FORMAT.format(
                *(
                    company_code_number,
                    timeslice.year - 1911, 
                    "%02d" % timeslice.quarter,
                )
            )
        else:
            url = cls.URL_FORMAT.format(
                *(
                    company_code_number,
                    timeslice.year - 1911, 
                    "%02d" % timeslice.quarter,
                )
            )
        return url


    @classmethod
    def init_class_customized_variables(cls):
        # import pdb; pdb.set_trace()
# CAUTION: This function MUST be called by the LEAF derived class
        if cls.URL_SPECIAL_FORMAT is None:
            cls.URL_SPECIAL_FORMAT = cls.CLASS_CONSTANT_CFG["url_special_format"]
        if cls.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            cls._init_statement_field_class_variables(CMN.DEF.STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME)


    @classmethod
    def show_statement_field_dimension(cls, auto_gen_sql_element=True):
        # cls.get_parent_class().init_class_variables() # Caution: This class function should NOT be called by the parent class
        if cls.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            cls._init_statement_field_class_variables(CMN.DEF.STATEMENT_OF_CHANGES_IN_EQUITY_FIELD_NAME_CONF_FILENAME)
            # cls.TABLE_FIELD_INTEREST_ENTRY_DEFAULTDICT[u"　母公司暨子公司所持有之母公司庫藏股股數（單位：股）".encode(CMN.DEF.URL_ENCODING_UTF8)] = [1, 3, 5]
        cls._show_statement_column_field_dimension_internal(CMN.DEF.STATEMENT_OF_CHANGES_IN_EQUITY_INTEREST_FIELD_METADATA_FILENAME, auto_gen_sql_element)


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(StatementOfChangesInEquity, self).__init__(**kwargs)


    @classmethod
    def _customized_select_web_data(cls, url_data, url_parsing_cfg):
        # import pdb; pdb.set_trace()
        url_data.encoding = url_parsing_cfg["url_encoding"]
        soup = BeautifulSoup(url_data.text)
        table_list = soup.select(url_parsing_cfg["url_data_selector"])
        table_list_len = len(table_list)
        if table_list_len == cls.WEB_TABLE_DATA_LEN:
            return table_list[cls.WEB_TABLE_DATA_INDEX].select('tr')
        elif table_list_len == cls.SPECIAL_WEB_TABLE_DATA_LEN:
            return table_list[cls.SPECIAL_WEB_TABLE_DATA_INDEX].select('tr')
        elif re.search(r"查無資料", soup.text.encode(CMN.DEF.URL_ENCODING_UTF8), re.U):
            return None
        raise CMN.EXCEPTION.WebScrapyServerBusyException("The len of the table_list should be 3, not %d. Server is probably busy, need retry" % len(table_list))
            # raise ValueError("The len of the table_list should be 3, not %d" % len(table_list))


    def _scrape_web_data(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        return self._scrape_web_data_internal(timeslice, company_code_number)


    def _parse_web_statement_field_data(self, web_data):
        return self._parse_web_statement_field_data_internal(web_data, self.TABLE_FIELD_START_INDEX, self.TABLE_FIELD_END_INDEX)


    def _parse_web_statement_column_field_data(self, web_data):
        return self._parse_web_statement_column_field_data_internal(web_data, self.TABLE_COLUMN_FIELD_TITLE_INDEX)


    def _parse_web_data(self, web_data):
        # import pdb; pdb.set_trace()
        return self._parse_web_data_internal(web_data, self.TABLE_COLUMN_FIELD_TITLE_INDEX, self.TABLE_FIELD_END_INDEX)


    @staticmethod
    def do_debug(silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb06?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=4947&year=105&season=01")
        # res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb06?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=9929&year=102&season=01")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        # g_data = soup.select('table tr')
        table_list = soup.select('table')
        g_data = table_list[1].select('tr')
        # print g_data
# Title
        # import pdb;pdb.set_trace()
        title_td = g_data[2].select('td')
        title_td_len = len(title_td)
        title_line = ""
        for title_i in range(0, title_td_len):
            mobj = re.match(r"^[\s]{0,}([\w]+)", title_td[title_i].text, re.U)
            if mobj is None:
                raise ValueError(u"Title Field[%d] Incorrect format: %s" % (title_i, title_td[title_i].text))
            title_line += u"%s " % mobj.group(1)
            # title_line += u"%s " % title_td[title_i].text
        print u"Title: %s" % title_line
# Content
        # import pdb;pdb.set_trace()
        for index, tr in enumerate(g_data[3:]):
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
