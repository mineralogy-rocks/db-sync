import os

import polars as pl
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

_cols = [
    'CITATION', 'SAMPLE NAME', 'TECTONIC SETTING', 'LOCATION', 'LOCATION COMMENT', 'LATITUDE (MIN.)', 'LATITUDE (MAX.)',
    'LONGITUDE (MIN.)', 'LONGITUDE (MAX.)', 'ELEVATION (MIN.)', 'ELEVATION (MAX.)', 'ROCK NAME',
    'ROCK TEXTURE', 'DRILLING DEPTH (MIN.)', 'DRILLING DEPTH (MAX.)', 'ALTERATION', 'MINERAL', 'SPOT', 'CRYSTAL',
    'RIM/CORE (MINERAL GRAINS)', 'GRAIN SIZE', 'PRIMARY/SECONDARY', 'SIO2(WT%)', 'TIO2(WT%)', 'ZRO2(WT%)',
    'HFO2(WT%)', 'THO2(WT%)', 'UO2(WT%)', 'AL2O3(WT%)', 'CR2O3(WT%)', 'LA2O3(WT%)', 'CE2O3(WT%)',
    'ND2O3(WT%)', 'SM2O3(WT%)', 'EU2O3(WT%)', 'EUO(WT%)', 'GD2O3(WT%)', 'TB2O3(WT%)', 'DY2O3(WT%)',
    'HO2O3(WT%)', 'ER2O3(WT%)', 'TM2O3(WT%)', 'YB2O3(WT%)', 'LU2O3(WT%)', 'Y2O3(WT%)', 'V2O3(WT%)',
    'V2O5(WT%)', 'NB2O5(WT%)', 'TA2O5(WT%)', 'PR2O3(WT%)', 'FE2O3T(WT%)', 'FE2O3(WT%)', 'FEOT(WT%)',
    'FEO(WT%)', 'FECO3(WT%)', 'CAO(WT%)', 'MGO(WT%)', 'MGCO3(WT%)', 'MNO(WT%)', 'MNCO3(WT%)', 'BAO(WT%)',
    'SRO(WT%)', 'SRCO3(WT%)', 'PBO2(WT%)', 'NIO(WT%)', 'ZNO(WT%)', 'ZNCO3(WT%)', 'COO(WT%)', 'CUO(WT%)',
    'K2O(WT%)', 'NA2O(WT%)', 'P2O5(WT%)', 'H2O(WT%)', 'H2OP(WT%)', 'H2OM(WT%)', 'CO2(WT%)', 'CACO3(WT%)',
    'BACO3(WT%)', 'K2CO3(WT%)', 'NA2CO3(WT%)', 'C(WT%)', 'F(WT%)', 'CL(WT%)', 'SO2(WT%)', 'SO3(WT%)',
    'LOI(WT%)', 'O(WT%)', 'AL(WT%)', 'TI(WT%)', 'FE(WT%)', 'MG(WT%)', 'MN(WT%)', 'CA(WT%)', 'BA(WT%)',
    'SR(WT%)', 'K(WT%)', 'NA(WT%)', 'P(WT%)', 'CO(WT%)', 'CU(WT%)', 'NI(WT%)', 'AS(WT%)', 'ZN(WT%)',
    'AG(WT%)', 'LI(PPM)', 'BE(PPM)', 'B(PPM)', 'N(PPB)', 'F(PPM)', 'NA(PPM)', 'MG(PPM)', 'AL(PPM)', 'SI(PPM)', 'P(PPM)',
    'S(PPM)', 'CL(PPM)', 'K(PPM)', 'CA(PPM)', 'SC(PPM)', 'TI(PPM)', 'V(PPM)', 'CR(PPM)', 'MN(PPM)', 'FE(PPM)',
    'CO(PPM)', 'NI(PPM)', 'CU(PPM)', 'ZN(PPM)', 'GA(PPM)', 'GE(PPM)', 'AS(PPM)', 'SE(PPM)', 'BR(PPM)',
    'RB(PPM)', 'SR(PPM)', 'Y(PPM)', 'ZR(PPM)', 'NB(PPM)', 'MO(PPM)', 'MO95(PPM)', 'MO97(PPM)', 'MO98(PPM)',
    'MO100(PPM)', 'RU(PPB)', 'RH(PPM)', 'PD(PPB)', 'AG(PPM)', 'CD(PPM)', 'IN(PPM)', 'SN(PPM)', 'SB(PPM)',
    'TE(PPM)', 'CS(PPM)', 'BA(PPM)', 'LA(PPM)', 'CE(PPM)', 'PR(PPM)', 'ND(PPM)', 'SM(PPM)', 'EU(PPM)', 'GD(PPM)', 'TB(PPM)',
    'DY(PPM)', 'HO(PPM)', 'ER(PPM)', 'TM(PPM)', 'YB(PPM)', 'LU(PPM)', 'HF(PPM)', 'TA(PPM)', 'W(PPM)', 'RE(PPM)', 'OS(PPB)',
    'IR(PPB)', 'PT(PPM)', 'PT(PPB)', 'AU(PPM)', 'TL(PPM)', 'PB(PPM)', 'BI(PPM)', 'TH(PPM)', 'U(PPM)', 'CO2(PPM)',
]
data = pl.read_csv('data/georoc/2024-12-SGFTFN_CARBONATES.csv', encoding='utf8-lossy', ignore_errors=True, columns=_cols)

# add new column ID
_minerals = data.select(
    pl.col('MINERAL').value_counts()
)

def _get_data(filename):
    return pl.read_csv(filename, encoding='utf8-lossy', separator=',', ignore_errors=True, columns=_cols)


_filenames = _get_files(path, format)
data = pl.DataFrame()
for file in _filenames:
    data = data.vstack(_get_data(path + file))
