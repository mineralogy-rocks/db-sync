import os

import pandas as pd
import numpy as np


path = 'data/georoc/'
format = 'csv'

def _get_files(path, format='csv'):

    if not format.startswith('.'):
        format = '.' + format

    _files = []
    for file in os.listdir(path):
        if file.endswith(format):
            _files.append(file)

    return _files


data = pd.read_csv('data/georoc/Carbonates 2024.csv', delimiter=',', encoding='latin1', engine='pyarrow')

def _get_data(filename):
    return pd.read_csv(filename, encoding='latin1', delimiter=',', engine="pyarrow")


_filenames = _get_files(path, format)
data = pd.DataFrame()
for file in _filenames:
    data = pd.concat([data, _get_data(path + file)], axis=0)
