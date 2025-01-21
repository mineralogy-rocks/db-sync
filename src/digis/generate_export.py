import os
import sys
import time

import polars as pl
from src.constants import TECTONIC_SETTING_CHOICES, ALTERATION_CHOICES, PRIMARY_SECONDARY_CHOICES, GEOROC_REPLACEMENTS


PATH = 'data/georoc/'
FORMAT = 'csv'

_meta_cols = [
    'CITATION', 'SAMPLE NAME', 'TECTONIC SETTING', 'LOCATION', 'LOCATION COMMENT', 'LATITUDE (MIN.)', 'LATITUDE (MAX.)',
    'LONGITUDE (MIN.)', 'LONGITUDE (MAX.)', 'ELEVATION (MIN.)', 'ELEVATION (MAX.)', 'ROCK NAME',
    'ROCK TEXTURE', 'DRILLING DEPTH (MIN.)', 'DRILLING DEPTH (MAX.)', 'ALTERATION', 'MINERAL', 'SPOT', 'CRYSTAL',
    'RIM/CORE (MINERAL GRAINS)', 'GRAIN SIZE', 'PRIMARY/SECONDARY',
]
_chem_cols = [
    'SIO2(WT%)', 'TIO2(WT%)', 'ZRO2(WT%)',
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
    'TE(PPM)', 'CS(PPM)', 'BA(PPM)', 'LA(PPM)', 'CE(PPM)', 'PR(PPM)', 'ND(PPM)', 'SM(PPM)', 'EU(PPM)', 'GD(PPM)',
    'TB(PPM)', 'DY(PPM)', 'HO(PPM)', 'ER(PPM)', 'TM(PPM)', 'YB(PPM)', 'LU(PPM)', 'HF(PPM)', 'TA(PPM)', 'W(PPM)', 'RE(PPM)',
    'OS(PPB)', 'IR(PPB)', 'PT(PPM)', 'PT(PPB)', 'AU(PPM)', 'TL(PPM)', 'PB(PPM)', 'BI(PPM)', 'TH(PPM)', 'U(PPM)', 'CO2(PPM)',
]
_cols = _meta_cols + _chem_cols


def _get_files(path, format='csv'):

    if not format.startswith('.'):
        format = '.' + format

    _files = []
    for file in os.listdir(path):
        if file.endswith(format):
            _files.append(file)

    return _files


def _idealize(data):
    """
    Idealize the data by assigning IDs to rows and cleaning up the data
    :param data: polars.DataFrame
    :return: polars.DataFrame
    """
    _data = data.with_columns(
        (
            pl.when(pl.col('SAMPLE NAME').is_not_null())
                .then(pl.col('CITATION').str.extract(r'\[(\d+)\]') + '-' + pl.col('SAMPLE NAME'))
                .otherwise(pl.col('CITATION').str.extract(r'\[(\d+)\]'))
                .alias('ID')
        ),
        SPOT=pl.col('SPOT').str.replace(r'\s+$', ''),
    )
    _data = _data.with_columns(
        rank=pl.col('ID').rank('ordinal').over('ID')
    )
    _data = _data.with_columns(
        pl.col('ID') + '-' + pl.col('rank').cast(pl.Utf8)
    ).drop('rank')
    return _data


def _get_data(filename):
    # filename = 'data/georoc/Clinopyroxenes Dec 2024.csv'
    _data = pl.scan_csv(filename, ignore_errors=True, encoding='utf8-lossy', null_values=['', ' ']).with_row_index()
    _colnames = _data.collect_schema().names()

    _data = _data.select(['index'] + [_col for _col in _cols if _col in _colnames])
    _missing_cols = [_col for _col in _cols if _col not in _colnames[1:]]

    _data = _data.with_columns(
        [pl.col(_col).cast(pl.Float32, strict=False).alias(_col) for _col in _chem_cols if _col not in _missing_cols],
    )
    _data = _data.with_columns(
        pl.col('SPOT').cast(pl.Utf8, strict=False).alias('SPOT'),
    )
    _data = _data.with_columns(
        [
            pl.when(pl.col(_col).is_not_null())
            .then(pl.col(_col).cast(pl.Utf8).str.replace(r'\s+$', ''))
            .otherwise(pl.col(_col))
            .alias(_col)
            for _col in _meta_cols if _col not in _missing_cols
        ]
    )
    _data = _data.filter(
        pl.any_horizontal([~pl.col(_col).is_null() for _col in _chem_cols if _col not in _missing_cols]) |
        pl.all_horizontal([pl.col(_col).is_null() for _col in _cols if _col not in _missing_cols])
    )
    _data = _data.collect()

    _broken_lines = _data.filter(
        (pl.col('CRYSTAL').str.contains(r'^\d+\.\d+$') | pl.col('RIM/CORE (MINERAL GRAINS)').str.contains(
            r'^\d+\.\d+$'))
    ).select(pl.col('index'))
    _data = _data.filter(~pl.col('index').is_in(_broken_lines))

    _data = _data.with_columns(
        [pl.lit(None).alias(_col) for _col in _missing_cols],
    )

    # Offset: detect row id after which the citations follow
    _offset = _data.filter(pl.col('CITATION').is_null()).select(pl.col('index'))
    if len(_offset):
        # TODO extract citations
        _citations = _data.filter(
            pl.col('index') > _offset
        )
        _data = _data.filter(
            pl.col('index') < _offset
        )

    return _data.select(pl.col(_cols))



def _clean_data(data):
    _titlecase_cols = ['ALTERATION', 'PRIMARY/SECONDARY', 'TECTONIC SETTING']
    _uppercase_cols = ['MINERAL', 'ROCK NAME']

    # Step 1 - Run replacements
    for _col, _vals in GEOROC_REPLACEMENTS:
        for _old, _new in _vals:
            _old = _old.replace('(', r'\(').replace(')', r'\)')
            data = data.with_columns(
                pl.col(_col).str.replace(r'(?i)^' + _old + '$', _new)
            )

    # Step 2 - Convert to titlecase and uppercase
    data = data.with_columns(
        pl.col(_col).str.to_titlecase() for _col in _titlecase_cols
    )
    data = data.with_columns(
        pl.col(_col).str.to_uppercase() for _col in _uppercase_cols
    )

    return data



def _check_validity(data):
    _invalid_ids = data.filter(
        pl.col('ID').is_null()
    )
    if len(_invalid_ids):
        raise ValueError('Some rows have no IDs assigned: ', len(_invalid_ids))

    _duplicate_ids = data.filter(
        pl.col('ID').is_duplicated()
    )
    if len(_duplicate_ids):
        raise ValueError('Duplicate IDs detected: ', len(_duplicate_ids))

    _new_tectonic_choices = data.filter(
        pl.col('TECTONIC SETTING').is_not_null()
    ).select(
        pl.col('TECTONIC SETTING').unique().sort()
    ).filter(
        ~pl.col('TECTONIC SETTING').is_in([_ for i, _ in TECTONIC_SETTING_CHOICES])
    )
    if len(_new_tectonic_choices):
        raise ValueError('New tectonic settings detected: ', len(_new_tectonic_choices))


    _new_alteration_choices = data.filter(
        pl.col('ALTERATION').is_not_null()
    ).select(
        pl.col('ALTERATION').unique().sort()
    ).filter(
        ~pl.col('ALTERATION').is_in([_ for i, _ in ALTERATION_CHOICES])
    )

    if len(_new_alteration_choices):
        raise ValueError('New alteration types detected: ', len(_new_alteration_choices))

    _new_primary_secondary_choices = data.filter(
        pl.col('PRIMARY/SECONDARY').is_not_null()
    ).select(
        pl.col('PRIMARY/SECONDARY').unique().sort()
    ).filter(
        ~pl.col('PRIMARY/SECONDARY').is_in([_ for i, _ in PRIMARY_SECONDARY_CHOICES])
    )

    if len(_new_primary_secondary_choices):
        raise ValueError('New primary/secondary types detected: ', len(_new_primary_secondary_choices))


def _convert_colnames(data):
    _map = {
        'ID': 'external_key',
        'MINERAL': 'mineral',
        'MINERAL_NOTE': 'mineral_note',
        'SAMPLE NAME': 'sample_name',
        'GRAIN SIZE': 'grain_size',
        'ROCK NAME': 'rock_name',
        'ROCK TEXTURE': 'rock_texture',
        'ALTERATION': 'alteration',
        'PRIMARY/SECONDARY': 'is_primary',
        'TECTONIC SETTING': 'tectonic_setting',
        'CITATION': 'citation',
        'LATITUDE (MIN.)': 'latitude_min',
        'LATITUDE (MAX.)': 'latitude_max',
        'LONGITUDE (MIN.)': 'longitude_min',
        'LONGITUDE (MAX.)': 'longitude_max',
        'ELEVATION (MIN.)': 'elevation_min',
        'ELEVATION (MAX.)': 'elevation_max',
        'LOCATION': 'location',
        'LOCATION COMMENT': 'location_note',
    }
    return data.select(pl.all().name.map(lambda x: _map[x] if x in _map else x))


def _replace_enums(df):
    """
    Replace string values with their corresponding enum integers using pattern matching.
    Optimized for large datasets by reducing the number of string operations.

    Args:
        df (pl.DataFrame): Input DataFrame containing columns to be processed

    Returns:
        pl.DataFrame: DataFrame with enum values replaced
    """
    enum_mappings = {
        'TECTONIC SETTING': TECTONIC_SETTING_CHOICES,
        'ALTERATION': ALTERATION_CHOICES,
        'PRIMARY/SECONDARY': PRIMARY_SECONDARY_CHOICES
    }

    for column, choices in enum_mappings.items():
        # Create when-then chain for all enum values
        conditions = [
            pl.when(pl.col(column).str.contains(key))
            .then(pl.lit(index))
            for index, key in choices
        ]

        # Replace with None if there's no enum match
        expr = pl.coalesce(conditions + [pl.lit(None)])
        df = df.with_columns(expr.alias(column))

    return df


def _convert_types(df):
    types_mapping = {
        'is_primary': pl.Boolean,
    }
    df = df.with_columns(
        [pl.col(col).cast(types_mapping.get(col, pl.Utf8)) for col in df.columns]
    )
    # TODO: replace nans and nulls with python None
    return df


def main():
    try:
        from dotenv import load_dotenv
        load_dotenv(".envs/.local/.mr")
    except:
        raise ImportError('Please create a .envs/.local/.mr file with the necessary environment variables.')

    _filenames = _get_files(PATH, FORMAT)
    data = pl.DataFrame(schema=_cols)
    for file in _filenames:
        print(f'Processing {file}')
        try:
            data = pl.concat([data, _get_data(PATH + file)], how='vertical_relaxed')
        except Exception as e:
            print(f'Error processing {file}: {e}')
            pass

    data = _idealize(data)
    data = data.select(pl.col(['ID'] + _cols))
    data = _clean_data(data)

    # If data is not valid, we need to investigate and possible add new choices to the db
    try:
        _check_validity(data)
    except ValueError as e:
        sys.exit(1)

    uri = f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
    mineral_log = pl.read_database_uri('SELECT UPPER(name) AS name FROM mineral_log;', uri=uri)

    MINERAL_CHOICES = (data.filter(
        pl.col('MINERAL').is_not_null(),
    )
    .select(
        pl.col('MINERAL').unique().sort()
    ))
    _missing_minerals = MINERAL_CHOICES.join(mineral_log, left_on='MINERAL', right_on='name', how='anti')

    if len(_missing_minerals):
        print(f'Found {len(_missing_minerals)} missing minerals. Saving to a file in data/generated/ folder.')
        _missing_minerals.write_csv(f'data/generated/missing_minerals_{time.strftime("%Y%m%d_%H%M%S")}.csv',
                                    include_header=True)

        # Move unmatched mineral names to MINERAL_NOTE column
        data = data.with_columns(
            pl.when(pl.col('MINERAL').is_in(_missing_minerals.select(pl.col('MINERAL'))))
            .then(pl.col('MINERAL'))
            .otherwise(pl.lit(None))
            .alias('MINERAL_NOTE'),
            pl.when(pl.col('MINERAL').is_in(_missing_minerals.select(pl.col('MINERAL'))))
            .then(pl.lit(None))
            .otherwise(pl.col('MINERAL'))
            .alias('MINERAL')
        )
    data = _replace_enums(data)
    data = _convert_colnames(data)
    data = _convert_types(data)

    _filename = f'data/generated/georoc_{time.strftime("%Y%m%d_%H%M%S")}.csv'
    data.write_csv(_filename)


if __name__ == '__main__':
    main()









