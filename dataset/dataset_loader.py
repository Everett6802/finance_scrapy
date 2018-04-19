# -*- coding: utf8 -*-

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

import libs.common as CMN
import dataset_definition as DS_DEF
import dataset_variable as DS_VAR


def load():
	print DS_VAR.DatasetVar.CUR_DATASET_SELECTION
