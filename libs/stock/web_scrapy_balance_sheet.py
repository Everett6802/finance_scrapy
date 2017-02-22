# -*- coding: utf8 -*-

import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()


# 資產負債表
class WebScrapyBalanceSheet(WebScrapyStockBase.WebScrapyStockBase):

    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyBalanceSheet, self).__init__(__file__, **kwargs)


    def assemble_web_url(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        url = self.url_format.format(
            *(
                timeslice.year, 
                "%02d" % timeslice.month,
                "%02d" % timeslice.day,
                company_code_number
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
        res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb03?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=3189&year=104&season=02")
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
# 會計項目    104年09月30日  103年12月31日  103年09月30日
#     金額  %   金額  %   金額  %
# ==== result: ====
# 流動資產       
# 現金及約當現金 11,375,431 27.57 11,541,615 28.11 11,191,807 27.77 
# 透過損益按公允價值衡量之金融資產 5,132,439 12.44 5,135,434 12.51 5,149,290 12.78 
# 備供出售金融資產 0 0.00 40,369 0.10 31,521 0.08 
# 無活絡市場之債務工具投資 425,098 1.03 463,827 1.13 499,856 1.24 
# 應收票據淨額 5,198 0.01 6,252 0.02 7,753 0.02 
# 應收帳款淨額 3,088,930 7.49 3,040,343 7.41 3,796,090 9.42 
# 應收帳款 355,994 0.86 436,406 1.06 503,558 1.25 
# 其他應收款淨額 502,227 1.22 452,265 1.10 543,409 1.35 
# 其他應收款 1,640 0.00 1,307 0.00 1,226 0.00 
# 存貨 2,145,157 5.20 2,162,969 5.27 2,149,745 5.33 
# 預付款項 172,276 0.42 98,501 0.24 166,427 0.41 
# 其他流動資產 88,160 0.21 91,980 0.22 49,313 0.12 
# 流動資產合計 23,292,550 56.45 23,471,268 57.17 24,089,995 59.77 
# 非流動資產
# 以成本衡量之金融資產 50,000 0.12 50,000 0.12 50,000 0.12 
# 無活絡市場之債務工具投資 0 0.00 0 0.00 49 0.00 
# 不動產 15,802,797 38.30 15,429,778 37.59 15,160,199 37.62 
# 無形資產 21,884 0.05 19,982 0.05 21,056 0.05 
# 遞延所得稅資產 53 0.00 276 0.00 0 0.00 
# 其他非流動資產 2,097,904 5.08 2,080,370 5.07 980,956 2.43 
# 非流動資產合計 17,972,638 43.55 17,580,406 42.83 16,212,260 40.23 
# 資產總計 41,265,188 100.00 41,051,674 100.00 40,302,255 100.00 
# 流動負債       
# 短期借款 2,159,349 5.23 1,806,896 4.40 2,047,248 5.08 
# 應付票據 76,039 0.18 41,011 0.10 43,969 0.11 
# 應付票據 0 0.00 0 0.00 0 0.00 
# 應付帳款 1,712,214 4.15 1,986,749 4.84 2,157,507 5.35 
# 應付帳款 0 0.00 0 0.00 0 0.00 
# 其他應付款 5,401,971 13.09 3,828,752 9.33 4,674,550 11.60 
# 本期所得稅負債 642,360 1.56 896,540 2.18 900,549 2.23 
# 負債準備 215 0.00 302 0.00 0 0.00 
# 其他流動負債 1,021,266 2.47 1,542,931 3.76 1,452,795 3.60 
# 流動負債合計 11,013,414 26.69 10,103,181 24.61 11,276,618 27.98 
# 非流動負債       
# 長期借款 838,309 2.03 730,722 1.78 910,399 2.26 
# 遞延所得稅負債 39,215 0.10 54,377 0.13 18,380 0.05 
# 其他非流動負債 116,370 0.28 110,620 0.27 147,720 0.37 
# 非流動負債合計 993,894 2.41 895,719 2.18 1,076,499 2.67 
# 負債總計 12,007,308 29.10 10,998,900 26.79 12,353,117 30.65 
# 歸屬於母公司業主之權益       
# 股本       
# 普通股股本 4,460,000 10.81 4,460,000 10.86 4,460,000 11.07 
# 股本合計 4,460,000 10.81 4,460,000 10.86 4,460,000 11.07 
# 資本公積       
# 資本公積合計 5,939,819 14.39 5,939,819 14.47 5,863,612 14.55 
# 保留盈餘       
# 法定盈餘公積 3,049,623 7.39 2,687,890 6.55 2,687,890 6.67 
# 特別盈餘公積 0 0.00 0 0.00 0 0.00 
# 未分配盈餘 13,102,821 31.75 14,030,597 34.18 12,421,887 30.82 
# 保留盈餘合計 16,152,444 39.14 16,718,487 40.73 15,109,777 37.49 
# 其他權益       
# 其他權益合計 186,238 0.45 279,703 0.68 89,365 0.22 
# 歸屬於母公司業主之權益合計 26,738,501 64.80 27,398,009 66.74 25,522,754 63.33 
# 非控制權益 2,519,379 6.11 2,654,765 6.47 2,426,384 6.02 
# 權益總計 29,257,880 70.90 30,052,774 73.21 27,949,138 69.35 
# 負債及權益總計 41,265,188 100.00 41,051,674 100.00 40,302,255 100.00 
# 待註銷股本股數 0  0  0  
# 預收股款 0  0  0  
# 母公司暨子公司所持有之母公司庫藏股股數 0  0  0  
