import re
import logging
from datetime import datetime, timedelta
from libs import web_scrapy_logging as WSL
g_logger = WSL.get_web_scrapy_logger()


DEF_CSV_FILE_PATH = "/var/tmp"

# def get_web_data(self, url, encoding, select_flag):
#     web_data = None
#     try:
#         res = requests.get(url)
#         res.encoding = encoding
#         #print res.text
#         soup = BeautifulSoup(res.text)
#         web_data = soup.select(select_flag)
#     except Exception as e:
#         print "Fail to scrapy URL[%s], due to: %s" % (url, str(e))

#     return web_data


# def get_time_range_list(start_year, start_month):
# # Parse the current time
#     today = datetime.today()
#     end_year = today.year
#     end_month = today.month
# # Generate the time range list
#     time_range_list = []
#     cur_year = start_year
#     cur_month = start_month
#     while True:
#         time_range_list.append({"year": cur_year, "month": cur_month})
#         cur_month += 1
#         if cur_month > 12:
#             cur_year += 1
#             cur_month = 1
#         if cur_year == end_year and cur_month == end_month:
#             break

# 	return time_range_list
