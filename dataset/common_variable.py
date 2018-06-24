# -*- coding: utf8 -*-

import os
import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
import common_definition as DS_CMN_DEF


class DatasetVarMeta(type):

    _param_update = False
    # __MY_VALUE = None
    _GLOBAL_VARIABLE_UPDATED = False
    # _PARAM_DICT = None
    # _FINANCE_DATASET_FOLDER_PATH = None
    _CAN_VISUALIZE = False


    def __init__(cls, name, bases, dct):
        # cls.__update_param()
        super(DatasetVarMeta, cls).__init__(name, bases, dct)


#     @classmethod
#     def __update_param(cls):
#         config_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, DS_CMN_DEF.DATASET_FOLDERNAME, DS_CMN_DEF.DATASET_CONF_FILENAME)
#         line_list = CMN.FUNC.read_file_lines_ex(config_filepath)
#         cls._PARAM_DICT = {}
#         for key, value in map(lambda line : line.split("="), line_list):
#             cls._PARAM_DICT[key] = value
# # validate the parameter
#         assert cls._PARAM_DICT.get("cur_dataset_selection", None) is not None, "The cur_dataset_selection field is NOT set"
#         cls._param_update = True


    @property
    def GLOBAL_VARIABLE_UPDATED(cls):
        # raise RuntimeError("Can NOT be invoked")
        return cls._GLOBAL_VARIABLE_UPDATED

    @GLOBAL_VARIABLE_UPDATED.setter
    def GLOBAL_VARIABLE_UPDATED(cls, global_variable_updated):
        if cls._GLOBAL_VARIABLE_UPDATED:
            raise RuntimeError("Global variables have already been UPDATED !!!")
        cls._GLOBAL_VARIABLE_UPDATED = global_variable_updated


#     @property
#     def CUR_DATASET_SELECTION(cls):
#         if not cls._param_update:
#             raise ValueError("Param not update !!!")
#         return cls._PARAM_DICT["cur_dataset_selection"]


#     # @property
#     # def FINANCE_DATASET_FOLDER_PATH(cls):
#     #     if cls._FINANCE_DATASET_FOLDER_PATH is None:
#     #         cls._FINANCE_DATASET_FOLDER_PATH = "%s/%s" % (GV.PROJECT_DATASET_FOLDERPATH, cls.CUR_DATASET_SELECTION)
#     #     return cls._FINANCE_DATASET_FOLDER_PATH


    @property
    def CAN_VISUALIZE(cls):
        if not cls._GLOBAL_VARIABLE_UPDATED:
            raise RuntimeError("Global variables are NOT updated !!!")
        return cls._CAN_VISUALIZE   

    @CAN_VISUALIZE.setter
    def CAN_VISUALIZE(cls, can_visualize):
        if cls._GLOBAL_VARIABLE_UPDATED:
            raise RuntimeError("Global variables have already been UPDATED !!!")
        cls._CAN_VISUALIZE = can_visualize


class DatasetVar(object):

    __metaclass__ = DatasetVarMeta
