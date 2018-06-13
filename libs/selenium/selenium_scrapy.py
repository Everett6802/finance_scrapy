#! /usr/bin/python
# -*- coding: utf8 -*-

import cmoney_scrapy as CMS
import statement_dog_scrapy as SDS


def scrape_company(company_number, table_data_count=1):
	with CMS.CMoneyWebScrapy() as cmoney:
		cmoney.CompanyNumber = company_number
		kwargs = {}
		kwargs["table_data_count"] = table_data_count
		for scrapy_method in CMS.CMoneyWebScrapy.get_scrapy_method_list():
			cmoney.scrape(scrapy_method, **kwargs)