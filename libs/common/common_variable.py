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
    _PROJECT_LIB_FOLDERPATH = None
    _PROJECT_CONF_FOLDERPATH = None
    _PROJECT_DATASET_FOLDERPATH = None
    _CUR_DATASET_SELECTION = None


    class __metaclass__(type):
        __LIB_FOLDERNAME_IN_PROJECT = CMN_DEF.LIB_FOLDERNAME
        __CONF_FOLDERNAME_IN_PROJECT = CMN_DEF.CONF_FOLDERNAME
        __DATASET_FOLDERNAME_IN_PROJECT = CMN_DEF.DATASET_FOLDERNAME

        @property
        def GLOBAL_VARIABLE_UPDATED(cls):
            raise RuntimeError("Can NOT be invoked")


        @GLOBAL_VARIABLE_UPDATED.setter
        def GLOBAL_VARIABLE_UPDATED(cls, global_variable_updated):
            if cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables have already been UPDATED !!!")
            cls._GLOBAL_VARIABLE_UPDATED = global_variable_updated


        @property
        def FINANCE_MODE(cls):
            if not cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables are NOT updated !!!")
            return cls._FINANCE_MODE


        @FINANCE_MODE.setter
        def FINANCE_MODE(cls, finance_mode):
            if cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables have already been UPDATED !!!")
            cls._FINANCE_MODE = finance_mode


        @property
        def IS_FINANCE_MARKET_MODE(cls):
            if not cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables are NOT updated !!!")
            return cls._IS_FINANCE_MARKET_MODE


        @IS_FINANCE_MARKET_MODE.setter
        def IS_FINANCE_MARKET_MODE(cls, is_finance_market_mode):
            if cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables have already been UPDATED !!!")
            cls._IS_FINANCE_MARKET_MODE = is_finance_market_mode


        @property
        def IS_FINANCE_STOCK_MODE(cls):
            if not cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables are NOT updated !!!")
            return cls._IS_FINANCE_STOCK_MODE


        @IS_FINANCE_STOCK_MODE.setter
        def IS_FINANCE_STOCK_MODE(cls, is_finance_stock_mode):
            if cls._GLOBAL_VARIABLE_UPDATED:
                raise RuntimeError("Global variables have already been UPDATED !!!")
            cls._IS_FINANCE_STOCK_MODE = is_finance_stock_mode


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
        def PROJECT_LIB_FOLDERPATH(cls):
            if cls._PROJECT_LIB_FOLDERPATH is None:
                cls._PROJECT_LIB_FOLDERPATH = "%s/%s" % (cls.PROJECT_FOLDERPATH, cls.__LIB_FOLDERNAME_IN_PROJECT)
            return cls._PROJECT_LIB_FOLDERPATH


        @property
        def PROJECT_CONF_FOLDERPATH(cls):
            if cls._PROJECT_CONF_FOLDERPATH is None:
                cls._PROJECT_CONF_FOLDERPATH = "%s/%s" % (cls.PROJECT_FOLDERPATH, cls.__CONF_FOLDERNAME_IN_PROJECT)
            return cls._PROJECT_CONF_FOLDERPATH


        @property
        def PROJECT_DATASET_FOLDERPATH(cls):
            if cls._PROJECT_DATASET_FOLDERPATH is None:
                cls._PROJECT_DATASET_FOLDERPATH = "%s/%s" % (cls.PROJECT_FOLDERPATH, cls.__DATASET_FOLDERNAME_IN_PROJECT)
            return cls._PROJECT_DATASET_FOLDERPATH


        @property
        def CUR_DATASET_SELECTION(cls):
            if cls.__CUR_DATASET_SELECTION is None:
                dataset_filepath = "%s/%s" % (cls.PROJECT_DATASET_FOLDERPATH, CMN_DEF.DATASET_CONF_FILENAME)



        @classmethod
        def __get_project_folderpath(cls):
            RELATIVE_COMMON_FOLDERPATH = ""
            current_path = os.path.dirname(os.path.realpath(__file__))
            # print current_path
            [project_folder, lib_folder, common_folder] = current_path.rsplit('/', 2)
            assert (lib_folder == "libs"), "The lib folder name[%s] is NOT as expected: libs" %  lib_folder
            assert (common_folder == "common"), "The common folder name[%s] is NOT as expected: common" % common_folder
            return project_folder
