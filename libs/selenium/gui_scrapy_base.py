#! /usr/bin/python
# -*- coding: utf8 -*-

from abc import ABCMeta, abstractmethod

import libs.common as CMN
import common_definition as CMN_DEF
import common_function as CMN_FUNC
# import libs.common as CMN
g_logger = CMN.LOG.get_logger()


class GUIWebScrapyBase(object):

    @classmethod
    def _write_scrapy_data_to_csv(cls, csv_data_list, scrapy_method_index, finance_parent_folderpath, company_number=None, company_group_number=None, dry_run_only=False):
        def get_old_csv_time_duration_if_exist(scrapy_method_index, csv_time_duration_dict):
            if csv_time_duration_dict is not None:
                if csv_time_duration_dict.get(scrapy_method_index, None) is not None:
                    return csv_time_duration_dict[scrapy_method_index]
            return None

        assert csv_data_list is not None, "csv_data_list should NOT be None"

        # import pdb; pdb.set_trace()
        csv_time_duration_folderpath = CMN.FUNC.get_finance_data_folderpath(finance_parent_folderpath, company_group_number, company_number)
        csv_time_duration_dict = CMN_FUNC.read_csv_time_duration_config_file(CMN_DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_time_duration_folderpath)

        url_time_unit = CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]["url_time_unit"]
# Caution: Need transfrom the time string from unicode to string
        time_duration_start = cls._transform_time_str2obj(url_time_unit, str(csv_data_list[0][0]))
        time_duration_end = cls._transform_time_str2obj(url_time_unit, str(csv_data_list[-1][0]))

        csv_old_time_duration_tuple = get_old_csv_time_duration_if_exist(scrapy_method_index, csv_time_duration_dict)

        new_csv_extension_time_duration, web2csv_time_duration_update_tuple = CMN.CLS.CSVTimeRangeUpdate.get_csv_time_duration_update(
            time_duration_start, 
            time_duration_end,
            csv_old_time_duration_tuple
        )
        # import pdb; pdb.set_trace()
        if  web2csv_time_duration_update_tuple is None:
            msg = None
            if company_number is not None:
                msg = u"The data[%s:%s] is Update-to-Date" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index], company_number)
            else:
                msg = u"The data[%s] is Update-to-Date" % CMN_DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index]
            g_logger.debug(msg)
            return
# Find the file path for writing data into csv
        csv_filepath = CMN_FUNC.get_finance_data_csv_filepath(scrapy_method_index, finance_parent_folderpath, company_group_number, company_number)

# Scrape the web data from each time duration
        for web2csv_time_duration_update in web2csv_time_duration_update_tuple: 

            scrapy_msg = None
            if company_number is not None:
                scrapy_msg = u"[%s:%s] %s:%s => %s" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index], company_number, web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
            else:
                scrapy_msg = "[%s] %s:%s => %s" % (CMN_DEF.SCRAPY_METHOD_DESCRIPTION[scrapy_method_index], CMN.DEF.TIME_DURATION_TYPE_DESCRIPTION[self.xcfg["time_duration_type"]], web2csv_time_duration_update.NewWebStart, web2csv_time_duration_update.NewWebEnd, csv_filepath)
            g_logger.info(scrapy_msg)
# Check if only dry-run
            if dry_run_only:
                print scrapy_msg
                continue

# If it's required to add the new web data in front of the old CSV data, a file is created to backup the old CSV data
            web2csv_time_duration_update.backup_old_csv_if_necessary(csv_filepath)
            # sub_csv_data_list = cls._filter_scrapy_data(csv_data_list, web2csv_time_duration_update)
            sub_csv_data_list = []
            if web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_BEFORE:
                for csv_data in csv_data_list:
                    time_duration = cls._transform_time_str2obj(url_time_unit, str(csv_data[0]))
                    if time_duration > web2csv_time_duration_update.NewWebEnd:
                        break
                    sub_csv_data_list.append(csv_data)
            elif web2csv_time_duration_update.AppendDirection == CMN.CLS.CSVTimeRangeUpdate.CSV_APPEND_AFTER:
                # for csv_data in reversed(csv_data_list):
                for csv_data in csv_data_list:
                    time_duration = cls._transform_time_str2obj(url_time_unit, str(csv_data[0]))
                    if time_duration < web2csv_time_duration_update.NewWebStart:
                        continue
                    sub_csv_data_list.append(csv_data)
            else:
                raise ValueError("Unsupport AppendDirection: %d" % web2csv_time_duration_update.AppendDirection)

            g_logger.debug("Write %d data to %s" % (len(sub_csv_data_list), csv_filepath))
            CMN.FUNC.write_csv_file_data(sub_csv_data_list, csv_filepath)
# Append the old CSV data after the new web data if necessary
            web2csv_time_duration_update.append_old_csv_if_necessary(csv_filepath)

        if dry_run_only:
            return
        # import pdb; pdb.set_trace()
# Update the time duration
        if csv_time_duration_dict is None:
            csv_time_duration_dict = {}
        csv_time_duration_dict[scrapy_method_index] = new_csv_extension_time_duration
        CMN_FUNC.write_csv_time_duration_config_file(CMN_DEF.CSV_DATA_TIME_DURATION_FILENAME, csv_time_duration_folderpath, csv_time_duration_dict)


    @classmethod
    def check_scrapy_field_description_exist(cls, scrapy_method_index, finance_parent_folderpath):
        conf_filepath = "%s/%s/%s%s" % (finance_parent_folderpath, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME, CMN_DEF.SCRAPY_CLASS_METHOD[scrapy_method_index], CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX)
        return CMN.FUNC.check_file_exist(conf_filepath)


    @classmethod
    def _write_scrapy_field_data_to_config(cls, csv_data_field_list, scrapy_method_index, finance_parent_folderpath):
        conf_folderpath = "%s/%s" % (finance_parent_folderpath, CMN.DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME)
        conf_filename = ("%s" % CMN_DEF.SCRAPY_CLASS_METHOD[scrapy_method_index]) + CMN.DEF.CSV_COLUMN_DESCRIPTION_CONF_FILENAME_POSTFIX
        CMN.FUNC.unicode_write_config_file_lines(csv_data_field_list, conf_filename, conf_folderpath)


    def scrape_web_to_csv(self, *args, **kwargs):
        # scrapy_method = CMN_DEF.SCRAPY_CLASS_CONSTANT_CFG[scrapy_method_index]["scrapy_class_method"]
        csv_data_list, _ = self.scrape_web(*args, **kwargs)
        # import pdb; pdb.set_trace()
        self._write_scrapy_data_to_csv(csv_data_list, self.scrapy_method_index, self.xcfg['finance_root_folderpath'], self.company_number, self.company_group_number, dry_run_only=self.xcfg['dry_run_only'])


    @abstractmethod
    def scrape_web(self, *args, **kwargs):
        raise NotImplementedError


    @abstractmethod
    def update_csv_field(self):
        raise NotImplementedError


    @abstractmethod
    def _transform_time_str2obj(cls, time_unit, time_str):
        raise NotImplementedError


    @property
    def ScrapyMethod(self):
        raise NotImplementedError

    @ScrapyMethod.setter
    def ScrapyMethod(self, value):
        raise NotImplementedError


    @property
    def ScrapyMethodIndex(self):
        raise NotImplementedError

    @ScrapyMethodIndex.setter
    def ScrapyMethodIndex(self, value):
        raise NotImplementedError


    @property
    def CompanyNumber(self):
        raise NotImplementedError

    @CompanyNumber.setter
    def CompanyNumber(self, value):
        raise NotImplementedError


    @property
    def CompanyGroupNumber(self):
        raise NotImplementedError

    @CompanyGroupNumber.setter
    def CompanyGroupNumber(self, value):
        raise NotImplementedError
