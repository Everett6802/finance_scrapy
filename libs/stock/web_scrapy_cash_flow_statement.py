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


# 現金流量表
class WebScrapyCashFlowStatement(WebScrapyStockBase.WebScrapyStockStatementBase):

    TABLE_FIELD_NOT_INTEREST_TITLE_LIST = [
    ]
    TABLE_FIELD_INTEREST_TITLE_LIST = None
    TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_DEFAULT_ENTRY_LEN = 2
    TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = None


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyCashFlowStatement, self).__init__(__file__, **kwargs)
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
        super(WebScrapyCashFlowStatement, self).assemble_web_url(timeslice, company_code_number)
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
