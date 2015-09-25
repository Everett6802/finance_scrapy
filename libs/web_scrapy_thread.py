import re
import threading
from datetime import datetime, timedelta
import common as CMN


class WebSrapyThread(threading.Thread):

    def __init__(self, delegation_obj):
        self.delegation_obj = delegation_obj


    def __str__(self):
        return self.delegation_obj.get_descrption()


    def run(self):
        self.delegation_obj.scrap_web_to_csv()
