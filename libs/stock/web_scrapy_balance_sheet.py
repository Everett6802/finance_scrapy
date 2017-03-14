# -*- coding: utf8 -*-

import re
import requests
import threading
import collections
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import libs.common as CMN
import libs.base as BASE
import web_scrapy_stock_base as WebScrapyStockBase
g_logger = CMN.WSL.get_web_scrapy_logger()
g_lock =  threading.Lock()


# 資產負債表
class WebScrapyBalanceSheet(WebScrapyStockBase.WebScrapyStockBase):

    TABLE_FIELD_TITLE_LIST = [
        u"　流動資產".encode('utf8'), #0
        u"　　　現金及約當現金".encode('utf8'), #1
        u"　　　透過損益按公允價值衡量之金融資產－流動".encode('utf8'), #2
        u"　　　備供出售金融資產－流動淨額".encode('utf8'), #3
        u"　　　無活絡市場之債務工具投資－流動淨額".encode('utf8'), #3
        u"　　　應收票據淨額".encode('utf8'), #4
        u"　　　應收帳款淨額".encode('utf8'), #5
        u"　　　應收帳款－關係人淨額".encode('utf8'), #6
        u"　　　其他應收款淨額".encode('utf8'), #7
        u"　　　其他應收款－關係人淨額".encode('utf8'), #8
        u"　　　存貨".encode('utf8'), #9
        u"　　　預付款項".encode('utf8'), #10
        u"　　　待出售非流動資產（淨額）".encode('utf8'), #11
        u"　　　其他流動資產".encode('utf8'), #12
        u"　　流動資產合計".encode('utf8'), #13
        u"　非流動資產".encode('utf8'), #14
        u"　　　備供出售金融資產－非流動淨額".encode('utf8'), #15
        u"　　　以成本衡量之金融資產－非流動淨額".encode('utf8'), #15
        u"　　　無活絡市場之債務工具投資－非流動淨額".encode('utf8'), #15
        u"　　　採用權益法之投資淨額".encode('utf8'), #16
        u"　　　不動產、廠房及設備".encode('utf8'), #17
        u"　　　投資性不動產淨額".encode('utf8'), #17
        u"　　　無形資產".encode('utf8'), #18
        u"　　    遞延所得稅資產".encode('utf8'), #19
        u"　　　其他非流動資產".encode('utf8'), #20
        u"　　非流動資產合計".encode('utf8'), #21
        u"　資產總計".encode('utf8'), #22
        u"　流動負債".encode('utf8'), #23
        u"　　　短期借款".encode('utf8'), #24
        u"　　　應付短期票券".encode('utf8'), #24
        u"　　　透過損益按公允價值衡量之金融負債－流動".encode('utf8'), #25
        u"　　　應付票據".encode('utf8'), #26
        u"　　　應付票據－關係人".encode('utf8'), #26
        u"　　　應付帳款".encode('utf8'), #27
        u"　　　應付帳款－關係人".encode('utf8'), #28
        u"　　　其他應付款".encode('utf8'), #29
        u"　　    本期所得稅負債".encode('utf8'), #30
        u"　　　其他流動負債".encode('utf8'), #31
        u"　　流動負債合計".encode('utf8'), #32
        u"　非流動負債".encode('utf8'), #33
        u"　　　應付公司債".encode('utf8'), #34
        u"　　　長期借款".encode('utf8'), #35
        u"　　　遞延所得稅負債".encode('utf8'), #36
        u"　　　其他非流動負債".encode('utf8'), #37
        u"　　非流動負債合計".encode('utf8'), #38
        u"　負債總計".encode('utf8'), #39
        u"　歸屬於母公司業主之權益".encode('utf8'), #40
        u"　　股本".encode('utf8'), #41
        u"　　　　普通股股本".encode('utf8'), #42
        u"　　　　預收股本".encode('utf8'), #43
        u"　　　股本合計".encode('utf8'), #44
        u"　　資本公積".encode('utf8'), #45
        u"　　　　資本公積－發行溢價".encode('utf8'), #46
        u"　　　　資本公積－庫藏股票交易".encode('utf8'), #47
        u"　　　　資本公積－實際取得或處分子公司股權價格與帳面價值差額".encode('utf8'), #48
        u"　　　    資本公積－採用權益法認列關聯企業及合資股權淨值之變動數".encode('utf8'), #49
        u"　　　　資本公積－合併溢額".encode('utf8'), #50
        u"　　　　資本公積－員工認股權".encode('utf8'), #51
        u"　　　　資本公積－其他".encode('utf8'), #52
        u"　　　資本公積合計".encode('utf8'), #53
        u"　　保留盈餘".encode('utf8'), #54
        u"　　　　法定盈餘公積".encode('utf8'), #55
        u"　　　　特別盈餘公積".encode('utf8'), #56
        u"　　　　未分配盈餘（或待彌補虧損）".encode('utf8'), #57
        u"　　　保留盈餘合計".encode('utf8'), #58
        u"　　其他權益".encode('utf8'), #59
        u"　　　　國外營運機構財務報表換算之兌換差額".encode('utf8'), #60
        u"　　　　備供出售金融資產未實現損益".encode('utf8'), #61
        u"　　　　其他權益－其他".encode('utf8'), #62
        u"　　　其他權益合計".encode('utf8'), #63
        u"　　　庫藏股票".encode('utf8'), #64
        u"　　歸屬於母公司業主之權益合計".encode('utf8'), #65
        u"　　非控制權益".encode('utf8'), #66
        u"　權益總計".encode('utf8'), #67
        u"　負債及權益總計".encode('utf8'), #68
        u"　待註銷股本股數（單位：股）".encode('utf8'), #69
        u"　預收股款（權益項下）之約當發行股數（單位：股）".encode('utf8'), #70
        u"　母公司暨子公司所持有之母公司庫藏股股數（單位：股）".encode('utf8'), #71
    ]
    TABLE_FIELD_NOT_INTEREST_TITLE_LIST = [
        u"　流動資產".encode('utf8'), #0
        u"　非流動資產".encode('utf8'), #14
        u"　流動負債".encode('utf8'), #23
        u"　非流動負債".encode('utf8'), #33
        u"　歸屬於母公司業主之權益".encode('utf8'), #40
        u"　　股本".encode('utf8'), #41
        u"　　資本公積".encode('utf8'), #45
        u"　　保留盈餘".encode('utf8'), #54
        u"　　其他權益".encode('utf8'), #59
        u"　待註銷股本股數（單位：股）".encode('utf8'), #69
        u"　預收股款（權益項下）之約當發行股數（單位：股）".encode('utf8'), #70
    ]
    TABLE_FIELD_INTEREST_TITLE_LIST = None
    TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_TITLE_LIST_LEN = None
    TABLE_FIELD_INTEREST_DEFAULT_ENTRY_LEN = 2
    TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = None


    def __init__(self, **kwargs):
        # import pdb; pdb.set_trace()
        super(WebScrapyBalanceSheet, self).__init__(__file__, **kwargs)
# Initialize the table meta-data
        if self.TABLE_FIELD_INTEREST_TITLE_LIST is None:
            with g_lock:
                if self.TABLE_FIELD_INTEREST_TITLE_LIST is None:
                    self.TABLE_FIELD_INTEREST_TITLE_LIST = [title for title in self.TABLE_FIELD_TITLE_LIST if title not in self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST]
                    self.TABLE_FIELD_INTEREST_TITLE_INDEX_DICT = {title: title_index for title_index, title in enumerate(self.TABLE_FIELD_INTEREST_TITLE_LIST)}
                    self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN = len(self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST)
                    self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN = len(self.TABLE_FIELD_INTEREST_TITLE_LIST)
                    self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT = collections.defaultdict(lambda: self.TABLE_FIELD_INTEREST_DEFAULT_ENTRY_LEN)
                    self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT[u"　母公司暨子公司所持有之母公司庫藏股股數（單位：股）".encode('utf8')] = 3


    def _modify_time_for_timeslice_generator(self, finance_time_start, finance_time_end):
        assert finance_time_start.get_time_unit_type() == CMN.DEF.DATA_TIME_UNIT_DAY, "The input start time unit type should be %d, not %d" % (CMN.DEF.DATA_TIME_UNIT_DAY, finance_time_start.get_time_unit_type())
        assert finance_time_end.get_time_unit_type() == CMN.DEF.DATA_TIME_UNIT_DAY, "The input end time unit type should be %d, not %d" % (CMN.DEF.DATA_TIME_UNIT_DAY, finance_time_end.get_time_unit_type())
        finance_quarter_start = CMN.CLS.FinanceQuarter.get_start_finance_quarter_from_date(finance_time_start)
        finance_quarter_end = CMN.CLS.FinanceQuarter.get_end_finance_quarter_from_date(finance_time_end)
        return (finance_quarter_start, finance_quarter_end)


    def assemble_web_url(self, timeslice, company_code_number):
        # import pdb; pdb.set_trace()
        super(WebScrapyBalanceSheet, self).assemble_web_url(timeslice, company_code_number)
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
        table_field_list = [None] * self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN
        interest_index = 0
        not_interest_index = 0
# Filter the data which is NOT interested in
        for index, tr in enumerate(web_data[7:]):
        #     print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            data_found = False
            data_can_ignore = False
            data_index = None
            if interest_index < self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN:
                try:
                    title = td[0].text.encode('utf8')
                    # g_logger.error(u"Search for the index of the title[%s] ......" % td[0].text)
                    data_index = cur_interest_index = (self.TABLE_FIELD_INTEREST_TITLE_LIST[interest_index:]).index(title)
                    interest_index = cur_interest_index + 1
                    data_found = True
                except ValueError:
                    pass
            if not data_found:
                if not_interest_index < self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST_LEN:
                    try:
                        cur_not_interest_index = (self.TABLE_FIELD_NOT_INTEREST_TITLE_LIST[not_interest_index:]).index(title)
                        not_interest_index = cur_not_interest_index + 1
                        data_can_ignore = True
                    except ValueError:
                        pass                
# Check if the entry is NOT in the title list of interest
            if (not data_found) and (not data_can_ignore):
                # import pdb; pdb.set_trace()
                raise CMN.EXCEPTION.WebScrapyNotFoundException(u"The title[%s] in company[%s] does NOT exist in the title list of interest" % (td[0].text, self.cur_company_code_number))
            if data_can_ignore:
                continue
# Parse the content of this entry, and the interested field into data structure
            entry_list_len = self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT[td[0].text]
            # entry_string = "%s" % str(td[0].text).strip(" ")
            table_field_list[data_index] = []
            for i in range(1, entry_list_len):
                # entry_string += (",%s" % str(td[i].text).strip(" "))
                table_field_list[data_index].append(str(td[i].text).strip(" "))
# Transforms the table into the 1-Dimension list
        padding_entry = "0" 
        for index in range(self.TABLE_FIELD_INTEREST_TITLE_LIST_LEN):
            if table_field_list[index] is None:
# Padding
                entry_list_len = self.TABLE_FIELD_INTEREST_ENTRY_LEN_DEFAULTDICT[self.TABLE_FIELD_INTEREST_TITLE_LIST[index]]
                data_list.extend([padding_entry] * entry_list_len)
            else:
                data_list.extend(table_field_list[index])
        return data_list
# 現金及約當現金 
# 透過損益按公允價值衡量之金融資產 
# 備供出售金融資產 
# 無活絡市場之債務工具投資
# 應收票據淨額 
# 應收帳款淨額 
# 應收帳款 
# 其他應收款淨額 
# 其他應收款 
# 存貨 
# 預付款項 
# 待出售非流動資產 
# 其他流動資產 
# 流動資產合計 
# 備供出售金融資產－非流動淨額
# 以成本衡量之金融資產－非流動淨額
# 無活絡市場之債務工具投資－非流動淨額
# 採用權益法之投資淨額 
# 不動產、廠房及設備
# 投資性不動產淨額
# 無形資產 
# 遞延所得稅資產 
# 其他非流動資產 
# 非流動資產合計 
# 資產總計 
# 短期借款 
# 應付短期票券
# 透過損益按公允價值衡量之金融負債－流動 
# 應付票據 
# 應付票據－關係人
# 應付帳款 
# 應付帳款－關係人 
# 其他應付款 
# 本期所得稅負債 
# 其他流動負債 
# 流動負債合計 
# 應付公司債 
# 長期借款 
# 遞延所得稅負債 
# 其他非流動負債 
# 非流動負債合計 
# 負債總計 
# 普通股股本 
# 預收股本 
# 股本合計 
# 資本公積－發行溢價
# 資本公積－庫藏股票交易 
# 資本公積－實際取得或處分子公司股權價格與帳面價值差額 
# 資本公積－採用權益法認列關聯企業及合資股權淨值之變動數 
# 資本公積－合併溢額 
# 資本公積－員工認股權 
# 資本公積－其他 
# 資本公積合計 
# 法定盈餘公積 
# 特別盈餘公積 
# 未分配盈餘 
# 保留盈餘合計 
# 國外營運機構財務報表換算之兌換差額 
# 備供出售金融資產未實現損益 
# 其他權益 
# 其他權益合計 
# 庫藏股票 
# 歸屬於母公司業主之權益合計 
# 非控制權益 
# 權益總計 
# 負債及權益總計 
# 母公司暨子公司所持有之母公司庫藏股股數 

    def do_debug(self, silent_mode=False):
        # import pdb; pdb.set_trace()
        res = CMN.FUNC.request_from_url_and_check_return("http://mops.twse.com.tw/mops/web/ajax_t164sb03?encodeURIComponent=1&step=1&firstin=1&off=1&keyword4=&code1=&TYPEK2=&checkbtn=&queryName=co_id&inpuType=co_id&TYPEK=all&isnew=false&co_id=2448&year=104&season=02")
        res.encoding = 'utf-8'
        # print res.text
        soup = BeautifulSoup(res.text)
        g_data = soup.select('table tr')
        # print g_data
        for index, tr in enumerate(g_data[7:]):
        #     print "%d: %s" % (index, tr.text)
            td = tr.select('td')
            td_len = len(td)
        #     mobj = re.match(r"^[\s]{0,}([\w]+)", td[0].text, re.U)
        #     if mobj is None:
        #         raise ValueError(u"Line[%d] Incorrect format: %s" % (index, td[0].text))
        #     line = u"%s " % mobj.group(1)
            line = u"%s " % td[0].text
            for i in range(1, td_len):
                line += u"%s " % str(td[i].text).strip(" ")
            print u"%s" % line
# 民國104年第3季
# 單位：新台幣仟元
# 會計項目    104年09月30日  103年12月31日  103年09月30日
#     金額  %   金額  %   金額  %
# ==== result: ====
# 流動資產       
# 　　　現金及約當現金 10,328,155 11.42 10,514,997 11.45 7,116,700 10.13 
# 　　　透過損益按公允價值衡量之金融資產－流動 1,441,303 1.59 1,195,677 1.30 1,594,621 2.27 
# 　　　備供出售金融資產－流動淨額 14,950 0.02 15,000 0.02 0 0.00 
# 　　　應收票據淨額 3,130,968 3.46 4,345,989 4.73 3,643,903 5.18 
# 　　　應收帳款淨額 7,535,884 8.33 7,421,177 8.08 7,959,974 11.33 
# 　　　應收帳款－關係人淨額 2,630,484 2.91 3,861,927 4.21 2,819,219 4.01 
# 　　　其他應收款淨額 630,376 0.70 772,989 0.84 284,486 0.40 
# 　　　其他應收款－關係人淨額 43,417 0.05 50,676 0.06 44,810 0.06 
# 　　　存貨 7,564,033 8.36 6,337,694 6.90 4,896,002 6.97 
# 　　　預付款項 1,970,401 2.18 1,635,299 1.78 1,176,267 1.67 
# 　　　待出售非流動資產（淨額） 300,315 0.33 188,281 0.21 4,410 0.01 
# 　　　其他流動資產 2,756,643 3.05 1,770,923 1.93 111,383 0.16 
# 　　流動資產合計 38,346,929 42.41 38,110,629 41.50 29,651,775 42.19 
# 　非流動資產       
# 　　　備供出售金融資產－非流動淨額 1,701,197 1.88 2,022,151 2.20 1,812,848 2.58 
# 　　　採用權益法之投資淨額 3,637,929 4.02 3,821,381 4.16 3,626,817 5.16 
# 　　　不動產、廠房及設備 36,288,072 40.13 37,499,279 40.83 28,513,928 40.57 
# 　　　無形資產 6,914,123 7.65 6,582,803 7.17 4,352,114 6.19 
# 　　    遞延所得稅資產 2,363,321 2.61 2,251,396 2.45 1,469,368 2.09 
# 　　　其他非流動資產 1,178,115 1.30 1,546,236 1.68 858,606 1.22 
# 　　非流動資產合計 52,082,757 57.59 53,723,246 58.50 40,633,681 57.81 
# 　資產總計 90,429,686 100.00 91,833,875 100.00 70,285,456 100.00 
# 　流動負債       
# 　　　短期借款 6,173,629 6.83 5,965,135 6.50 2,435,855 3.47 
# 　　　透過損益按公允價值衡量之金融負債－流動 277,363 0.31 1,270,635 1.38 2,368,294 3.37 
# 　　　應付票據 42,343 0.05 1,523 0.00 11,316 0.02 
# 　　　應付帳款 3,193,900 3.53 3,047,683 3.32 2,870,640 4.08 
# 　　　應付帳款－關係人 470,427 0.52 341,490 0.37 382,168 0.54 
# 　　　其他應付款 5,050,605 5.59 4,090,039 4.45 3,940,604 5.61 
# 　　    本期所得稅負債 45,922 0.05 51,191 0.06 18,044 0.03 
# 　　　其他流動負債 3,638,281 4.02 4,886,617 5.32 1,000,966 1.42 
# 　　流動負債合計 18,892,470 20.89 19,654,313 21.40 13,027,887 18.54 
# 　非流動負債       
# 　　　應付公司債 7,243,949 8.01 7,769,087 8.46 7,242,302 10.30 
# 　　　長期借款 1,819,520 2.01 910,321 0.99 1,125,861 1.60 
# 　　　遞延所得稅負債 1,062,683 1.18 1,013,621 1.10 157,255 0.22 
# 　　　其他非流動負債 1,372,521 1.52 1,484,435 1.62 1,217,697 1.73 
# 　　非流動負債合計 11,498,673 12.72 11,177,464 12.17 9,743,115 13.86 
# 　負債總計 30,391,143 33.61 30,831,777 33.57 22,771,002 32.40 
# 　歸屬於母公司業主之權益       
# 　　股本       
# 　　　　普通股股本 11,015,131 12.18 11,031,787 12.01 9,363,389 13.32 
# 　　　　預收股本 0 0.00 0 0.00 0 0.00 
# 　　　股本合計 11,015,131 12.18 11,031,787 12.01 9,363,389 13.32 
# 　　資本公積       
# 　　　　資本公積－發行溢價 37,710,828 41.70 41,575,478 45.27 28,907,419 41.13 
# 　　　　資本公積－庫藏股票交易 17,165 0.02 27,091 0.03 27,091 0.04 
# 　　　　資本公積－實際取得或處分子公司股權價格與帳面價值差額 850,962 0.94 923,016 1.01 923,016 1.31 
# 　　　    資本公積－採用權益法認列關聯企業及合資股權淨值之變動數 43,677 0.05 43,711 0.05 38,501 0.05 
# 　　　　資本公積－合併溢額 3,828,476 4.23 0 0.00 3,828,476 5.45 
# 　　　　資本公積－員工認股權 10,966 0.01 10,966 0.01 10,966 0.02 
# 　　　　資本公積－其他 768,320 0.85 762,570 0.83 759,970 1.08 
# 　　　資本公積合計 43,230,394 47.81 43,342,832 47.20 34,495,439 49.08 
# 　　保留盈餘       
# 　　　　法定盈餘公積 1,547,864 1.71 1,366,831 1.49 1,366,831 1.94 
# 　　　　特別盈餘公積 0 0.00 100,596 0.11 100,596 0.14 
# 　　　　未分配盈餘（或待彌補虧損） 2,423,871 2.68 2,737,708 2.98 819,717 1.17 
# 　　　保留盈餘合計 3,971,735 4.39 4,205,135 4.58 2,287,144 3.25 
# 　　其他權益       
# 　　　　國外營運機構財務報表換算之兌換差額 403,574 0.45 639,823 0.70 171,154 0.24 
# 　　　　備供出售金融資產未實現損益 -445,762 -0.49 -149,071 -0.16 1,443 0.00 
# 　　　　其他權益－其他 -310,411 -0.34 -346,915 -0.38 -275,136 -0.39 
# 　　　其他權益合計 -352,599 -0.39 143,837 0.16 -102,539 -0.15 
# 　　　庫藏股票 -399,536 -0.44 -461,200 -0.50 -197,622 -0.28 
# 　　歸屬於母公司業主之權益合計 57,465,125 63.55 58,262,391 63.44 45,845,811 65.23 
# 　　非控制權益 2,573,418 2.85 2,739,707 2.98 1,668,643 2.37 
# 　權益總計 60,038,543 66.39 61,002,098 66.43 47,514,454 67.60 
# 　負債及權益總計 90,429,686 100.00 91,833,875 100.00 70,285,456 100.00 
# 　待註銷股本股數（單位：股） 0  0  0  
# 　預收股款（權益項下）之約當發行股數（單位：股） 0  0  0  
# 　母公司暨子公司所持有之母公司庫藏股股數（單位：股） 6,717,732  7,887,835  3,747,208   
