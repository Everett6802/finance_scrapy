# -*- coding: utf8 -*-

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 損益表
class WebScrapyIncomeStatement(WebScrapyStockBase.WebScrapyStockBase):

    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyIncomeStatement, self).__init__(__file__, **kwargs)


    def assemble_web_url(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        super(WebScrapyIncomeStatement, self).assemble_web_url(timeslice, company_code_number)
        url = self.url_format.format(
            *(
                company_code_number,
                timeslice.year - 1911, 
                "%02d" % timeslice.quarter,
            )
        )
        return url


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
# 持股1-999人數
# 持股1-999股數
# 持股1-999佔集保庫存數比例
# 持股1,000-5,000人數
# 持股1,000-5,000股數
# 持股1,000-5,000佔集保庫存數比例
# 持股5,001-10,000人數
# 持股5,001-10,000股數
# 持股5,001-10,000佔集保庫存數比例
# 持股10,001-15,000人數
# 持股10,001-15,000股數
# 持股10,001-15,000佔集保庫存數比例
# 持股15,001-20,000人數
# 持股15,001-20,000股數
# 持股15,001-20,000佔集保庫存數比例
# 持股20,001-30,000人數
# 持股20,001-30,000股數
# 持股20,001-30,000佔集保庫存數比例
# 持股30,001-40,000人數
# 持股30,001-40,000股數
# 持股30,001-40,000佔集保庫存數比例
# 持股40,001-50,000人數
# 持股40,001-50,000股數
# 持股40,001-50,000佔集保庫存數比例
# 持股50,001-100,000人數
# 持股50,001-100,000股數
# 持股150,001-100,000佔集保庫存數比例
# 持股100,001-200,000人數
# 持股100,001-200,000股數
# 持股100,001-200,000佔集保庫存數比例
# 持股200,001-400,000人數
# 持股200,001-400,000股數
# 持股200,001-400,000佔集保庫存數比例
# 持股400,001-600,000人數
# 持股400,001-600,000股數
# 持股400,001-600,000佔集保庫存數比例
# 持股600,001-800,000人數
# 持股600,001-800,000股數
# 持股600,001-800,000佔集保庫存數比例
# 持股800,001-1,000,000人數
# 持股800,001-1,000,000股數
# 持股800,001-1,000,000佔集保庫存數比例
# 持股1,000,001以上人數
# 持股1,000,001以上股數
# 持股1,000,001以上佔集保庫存數比例
# 合計人數
# 合計股數
# 合計佔集保庫存數比例


    def do_debug(self, silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb04?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=3189&year=104&season=02")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        # print g_data
        for index, tr in enumerate(g_data[7:]):
        #     print "%d: %s" % (index, tr.text)
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
# 會計項目    104年第3季 103年第3季 104年01月01日至104年09月30日   103年01月01日至103年09月30日
#     金額  %   金額  %   金額  %   金額  %
# ==== result: ====
# 營業收入合計 5,432,665 100.00 6,784,197 100.00 10,778,886 100.00 12,580,996 100.00 
# 營業成本合計 4,185,950 77.05 4,614,823 68.02 8,224,440 76.30 8,929,211 70.97 
# 營業毛利 1,246,715 22.95 2,169,374 31.98 2,554,446 23.70 3,651,785 29.03 
# 營業毛利 1,246,715 22.95 2,169,374 31.98 2,554,446 23.70 3,651,785 29.03 
# 營業費用         
# 推銷費用 82,533 1.52 168,509 2.48 183,424 1.70 228,460 1.82 
# 管理費用 222,887 4.10 265,976 3.92 478,838 4.44 499,764 3.97 
# 研究發展費用 336,744 6.20 343,823 5.07 693,203 6.43 655,222 5.21 
# 營業費用合計 642,164 11.82 778,308 11.47 1,355,465 12.58 1,383,446 11.00 
# 營業利益 604,551 11.13 1,391,066 20.50 1,198,981 11.12 2,268,339 18.03 
# 營業外收入及支出         
# 其他收入 168,338 3.10 36,389 0.54 224,500 2.08 75,231 0.60 
# 其他利益及損失淨額 -104,743 -1.93 35,840 0.53 -72,158 -0.67 28,494 0.23 
# 財務成本淨額 13,400 0.25 15,224 0.22 25,629 0.24 30,204 0.24 
# 營業外收入及支出合計 50,195 0.92 57,005 0.84 126,713 1.18 73,521 0.58 
# 稅前淨利 654,746 12.05 1,448,071 21.34 1,325,694 12.30 2,341,860 18.61 
# 所得稅費用 101,505 1.87 203,259 3.00 193,760 1.80 326,606 2.60 
# 繼續營業單位本期淨利 553,241 10.18 1,244,812 18.35 1,131,934 10.50 2,015,254 16.02 
# 本期淨利 553,241 10.18 1,244,812 18.35 1,131,934 10.50 2,015,254 16.02 
# 後續可能重分類至損益之項目         
# 國外營運機構財務報表換算之兌換差額 -51,361 -0.95 -114,597 -1.69 -132,220 -1.23 -39,137 -0.31 
# 備供出售金融資產未實現評價損益 0 0.00 -9,712 -0.14 -24,694 -0.23 734 0.01 
# 與可能重分類之項目相關之所得稅 -5,478 -0.10 -12,258 -0.18 -14,086 -0.13 -4,147 -0.03 
# 其他綜合損益 -45,883 -0.84 -112,051 -1.65 -142,828 -1.33 -34,256 -0.27 
# 本期綜合損益總額 507,358 9.34 1,132,761 16.70 989,106 9.18 1,980,998 15.75 
# 母公司業主 600,768 11.06 1,231,780 18.16 1,217,957 11.30 2,024,327 16.09 
# 非控制權益 -47,527 -0.87 13,032 0.19 -86,023 -0.80 -9,073 -0.07 
# 母公司業主 574,022 10.57 1,162,221 17.13 1,124,492 10.43 2,004,813 15.94 
# 非控制權益 -66,664 -1.23 -29,460 -0.43 -135,386 -1.26 -23,815 -0.19 
# 基本每股盈餘         
# 基本每股盈餘 1.35  2.76  2.73  4.54  
# 稀釋每股盈餘         
# 稀釋每股盈餘 1.32  2.72  2.68  4.48
