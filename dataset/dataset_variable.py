# -*- coding: utf8 -*-

import os
import libs.common as CMN
from libs.common.common_variable import GlobalVar as GV
import dataset_definition as DS_DEF


class DatasetVarMeta(type):

    _param_update = False
    # __MY_VALUE = None
    _PARAM_DICT = None


    def __init__(cls, name, bases, dct):
        cls.__update_param()
        super(DatasetVarMeta, cls).__init__(name, bases, dct)


    @classmethod
    def __update_param(cls):
        config_filepath = "%s/%s/%s" % (GV.PROJECT_FOLDERPATH, DS_DEF.DATASET_FOLDERNAME, DS_DEF.DATASET_CONF_FILENAME)
        line_list = CMN.FUNC.read_file_lines_ex(config_filepath)
        cls._PARAM_DICT = {}
        for key, value in map(lambda line : line.split("="), line_list):
            cls._PARAM_DICT[key] = value
# validate the parameter
        assert cls._PARAM_DICT.get("cur_dataset_selection", None) is not None, "The cur_dataset_selection field is NOT set"
        cls._param_update = True


    @property
    def CUR_DATASET_SELECTION(self):
        if not self._param_update:
            raise ValueError("Param not update !!!")
        return self._PARAM_DICT["cur_dataset_selection"]


class DatasetVar(object):

    __metaclass__ = DatasetVarMeta
