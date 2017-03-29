# -*- coding: utf8 -*-

import re
import requests
import threading
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()
g_lock =  threading.Lock()


# 股東權益變動表
class WebScrapyStatementOfChangesInEquity(WebScrapyStockBase.WebScrapyStockStatementBase):

    TABLE_FIELD_NOT_INTEREST_TITLE_LIST = [
    ]
    TABLE_FIELD_INTEREST_TITLE_LIST = None
    TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_DEFAULT_ENTRY_LEN = 2
    TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = None


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyStatementOfChangesInEquity, self).__init__(__file__, **kwargs)
# Initialize the table meta-data
        if self.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            with g_lock:
                if self.TABLE_FIELD_INTEREST_TITLE_LIST is None:
                    conf_filename = CMN.DEF.DEF_BALANCE_SHEET_FIELD_NAME_CONF_FILENAME
                    if not CMN.FUNC.check_config_file_exist(conf_filename):
                        raise ValueError("The %s file does NOT exist" % conf_filename)
                    table_field_title_list = CMN.FUNC.read_config_file_lines_ex(conf_filename, "rb")
                    self.TABLE_FIELD_INTEREST_TITLE_LIST = [title for title in table_field_title_list if title not in self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST]
                    self.TABLE_FIELD_INTEREST_TITLE_INDEX_DICT = {title: title_index for title_index, title in enumerate(self.TABLE_FIELD_INTEREST_TITLE_LIST)}
                    self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = len(self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST)
                    self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN = len(self.TABLE_FIELD_INTEREST_TITLE_LIST)
                    self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = collections.defaultdict(lambda: self.TABLE_FIELD_INTEREST_DEFAULT_ENTRY_LEN)


    def assemble_web_url(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        super(WebScrapyStatementOfChangesInEquity, self).assemble_web_url(timeslice, company_code_number)
        url = self.url_format.format(
            *(
                company_code_number,
                timeslice.year - 1911, 
                "%02d" % timeslice.quarter,
            )
        )
        return url


    def _parse_web_statement_field_data(self, web_data):
        if len(web_data) == 0:
            return None
        data_list = []
# Filter the data which is NOT interested in
        for tr in web_data[7:]:
        #     print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            # data_list.append(td[0].text.encode(CMN.DEF.URL_ENCODING_UTF8))
            data_list.append(td[0].text)
        return data_list


    def _parse_web_data(self, web_data):
        if len(web_data) == 0:
            return None
        # import pdb; pdb.set_trace()
        data_list = []
        data_list.append(self.date_cur_string)
# Scrape the data of each stock interval
        for tr in web_data[9:24]:
            td = tr.select('td')
            data_list.append(str(CMN.FUNC.remove_comma_in_string(td[2].text)))
            data_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(td[3].text)))
            data_list.append(td[4].text)
# Ignore the data which is NOT interesting... Scrape the data of sum
        sum_found = False
        # import pdb; pdb.set_trace()
        for tr in web_data[24:]:
            td = tr.select('td')
            if not re.match(self.TABLE_SUM_FLAG, td[1].text, re.U):
                continue
            data_list.append(str(CMN.FUNC.remove_comma_in_string(td[2].text)))
            data_list.append(str(CMN.FUNC.transform_share_number_string_to_board_lot(td[3].text)))
            data_list.append(td[4].text)
            sum_found = True
            break
        if not sum_found:
            raise ValueError("Fail to find the sum flag in the table");
        return data_list


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
