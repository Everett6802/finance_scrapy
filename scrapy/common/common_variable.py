# -*- coding: utf8 -*-

import os
import common_definition as CMN_DEF


class GlobalVar(object):
    _GLOBAL_VARIABLE_UPDATED = False
    _FINANCE_MODE = CMN_DEF.FINANCE_MODE_UNKNOWN
    _IS_FINANCE_MARKET_MODE = False
    _IS_FINANCE_STOCK_MODE = True
    _ENABLE_COMPANY_NOT_FOUND_EXCEPTION = False 
    _PROJECT_FOLDERPATH = None #CMN_FUNC.get_project_folderpath()
    _PROJECT_SCRAPY_FOLDERPATH = None
    _PROJECT_CONF_FOLDERPATH = None
    # _CUR_DATASET_SELECTION = None
    _PROJECT_PARENT_FOLDERPATH = None
    _FINANCE_DATASET_FOLDERPATH = None
    _FINANCE_DATASET_CONF_FOLDERPATH = None
    _FINANCE_DATASET_DATA_FOLDERPATH = None
    _CAN_USE_SELEIUM = None


    class __metaclass__(type):
        # __SCRAPY_FOLDERNAME_IN_PROJECT = CMN_DEF.SCRAPY_FOLDERNAME
        # __CONF_FOLDERNAME_IN_PROJECT = CMN_DEF.CONF_FOLDERNAME
        # __DATASET_FOLDERNAME_IN_PROJECT = CMN_DEF.DATASET_FOLDERNAME

        @property
        def GLOBAL_VARIABLE_UPDATED(cls):
            raise RuntimeError("Can NOT be invoked")


        @GLOBAL_VARIABLE_UPDATED.setter
        def GLOBAL_VARIABLE_UPDATED(cls, global_variable_updated):
            if cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables have already been UPDATED !!!")
            cls._GLOBAL_VARIABLE_UPDATED = global_variable_updated


        @property
        def ENABLE_COMPANY_NOT_FOUND_EXCEPTION(cls):
            if not cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables are NOT updated !!!")
            return cls._ENABLE_COMPANY_NOT_FOUND_EXCEPTION


        @ENABLE_COMPANY_NOT_FOUND_EXCEPTION.setter
        def ENABLE_COMPANY_NOT_FOUND_EXCEPTION(cls, enable_company_not_found_exception):
            if cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables have already been UPDATED !!!")
            cls._ENABLE_COMPANY_NOT_FOUND_EXCEPTION = enable_company_not_found_exception


        @property
        def PROJECT_FOLDERPATH(cls):
            # if not cls._GLOBAL_VARIABLE_UPDATED:
            #     raise RuntimeError("Global variables are NOT updated !!!")
            if cls._PROJECT_FOLDERPATH is None:
                cls._PROJECT_FOLDERPATH = cls.__get_project_folderpath()
            return cls._PROJECT_FOLDERPATH


        @property
        def PROJECT_SCRAPY_FOLDERPATH(cls):
            if cls._PROJECT_SCRAPY_FOLDERPATH is None:
                cls._PROJECT_SCRAPY_FOLDERPATH = "%s/%s" % (cls.PROJECT_FOLDERPATH, CMN_DEF.SCRAPY_FOLDERNAME)
            return cls._PROJECT_SCRAPY_FOLDERPATH


        @property
        def PROJECT_CONF_FOLDERPATH(cls):
            if cls._PROJECT_CONF_FOLDERPATH is None:
                cls._PROJECT_CONF_FOLDERPATH = "%s/%s" % (cls.PROJECT_FOLDERPATH, CMN_DEF.CONF_FOLDERNAME)
            return cls._PROJECT_CONF_FOLDERPATH


        @property
        def PROJECT_PARENT_FOLDERPATH(cls):
            if cls._PROJECT_PARENT_FOLDERPATH is None:
                [project_parent_folder, project_folder] = cls.PROJECT_FOLDERPATH.rsplit('/', 1)
                assert (project_folder == CMN_DEF.FINANCE_SCRAPY_PROJECT_NAME), "The project folder name[%s] is NOT as expected: %s" % (project_folder, CMN_DEF.FINANCE_SCRAPY_PROJECT_NAME)
                cls._PROJECT_PARENT_FOLDERPATH = project_parent_folder
            return cls._PROJECT_PARENT_FOLDERPATH


        @property
        def FINANCE_DATASET_FOLDERPATH(cls):
            # import pdb; pdb.set_trace()
            if cls._FINANCE_DATASET_FOLDERPATH is None:
                cls._FINANCE_DATASET_FOLDERPATH = "%s/%s" % (cls.PROJECT_PARENT_FOLDERPATH, CMN_DEF.FINANCE_DATASET_PROJECT_NAME)
            return cls._FINANCE_DATASET_FOLDERPATH


        @property
        def FINANCE_DATASET_CONF_FOLDERPATH(cls):
            if cls._FINANCE_DATASET_CONF_FOLDERPATH is None:
                cls._FINANCE_DATASET_CONF_FOLDERPATH = "%s/%s" % (cls.FINANCE_DATASET_FOLDERPATH, CMN_DEF.CONF_FOLDERNAME)
            return cls._FINANCE_DATASET_CONF_FOLDERPATH


        @property
        def FINANCE_DATASET_DATA_FOLDERPATH(cls):
            # import pdb; pdb.set_trace()
            if cls._FINANCE_DATASET_DATA_FOLDERPATH is None:
                # cls._FINANCE_DATASET_DATA_FOLDERPATH = "%s/%s/%s" % (cls.FINANCE_DATASET_FOLDERPATH, CMN_DEF.DATA_FOLDERNAME, CMN_DEF.CSV_FIELD_DESCRIPTION_FOLDERNAME)
                cls._FINANCE_DATASET_DATA_FOLDERPATH = "%s/%s" % (cls.FINANCE_DATASET_FOLDERPATH, CMN_DEF.DATA_FOLDERNAME)
            return cls._FINANCE_DATASET_DATA_FOLDERPATH


        @property
        def CAN_USE_SELEIUM(cls):
            if cls._CAN_USE_SELEIUM is None:
                try:
                    from selenium import webdriver
                except ImportError:
                    cls._CAN_USE_SELEIUM = False
                else:
                    cls._CAN_USE_SELEIUM = True
            return cls._CAN_USE_SELEIUM


        # @property
        # def CUR_DATASET_SELECTION(cls):
        #     if cls.__CUR_DATASET_SELECTION is None:
        #         dataset_filepath = "%s/%s" % (cls.FINANCE_DATASET_FOLDERPATH, CMN_DEF.DATASET_CONF_FILENAME)


        @classmethod
        def __get_project_folderpath(cls):
            RELATIVE_COMMON_FOLDERPATH = ""
            current_path = os.path.dirname(os.path.realpath(__file__))
            # print current_path
            [project_folder, scrapy_folder, common_folder] = current_path.rsplit('/', 2)
            assert (scrapy_folder == "scrapy"), "The scrapy folder name[%s] is NOT as expected: scrapy" %  scrapy_folder
            assert (common_folder == "common"), "The common folder name[%s] is NOT as expected: common" % common_folder
            return project_folder
