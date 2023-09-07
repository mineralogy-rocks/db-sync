# -*- coding: UTF-8 -*-
import re
import json

import numpy as np
import pandas as pd

from django.utils.html import strip_tags

from src.constants import (
    STATUS_SYNONYM_UNCERTAIN,
    STATUS_UNCERTAIN_VARIETY,
    STATUS_POLYTYPE,
    IMA_STATUS_CHOICES,
    IMA_NOTES_CHOICES,
)


def prepare_minerals(minerals):
    minerals_ = minerals.copy()
    minerals_ = minerals_.replace(0, np.nan)
    minerals_["discovery_year"] = pd.to_numeric(
        minerals_["discovery_year"], errors="coerce"
    )
    minerals_["name"] = minerals_["name"].str.strip()

    minerals_["ima_status"] = minerals_["ima_status"].fillna(0).astype(np.int64)
    minerals_["ima_note"] = minerals_["ima_note"].fillna(0).astype(np.int64)
    minerals_["ima_status"] = minerals_["ima_status"].apply(
        lambda x: [option for option, value in IMA_STATUS_CHOICES.items() if value & x]
    )
    minerals_["ima_note"] = minerals_["ima_note"].apply(
        lambda x: [option for option, value in IMA_NOTES_CHOICES.items() if value & x]
    )

    minerals_.description = minerals_.description.replace(r"", np.nan)
    minerals_.ima_symbol = minerals_.ima_symbol.replace(r"", np.nan)

    minerals_.formula = minerals_.formula.str.strip()
    minerals_.formula = minerals_.formula.replace(r"", np.nan)
    minerals_.imaformula = minerals_.imaformula.apply(simpleformula)
    minerals_.imaformula = minerals_.imaformula.str.strip()
    minerals_.imaformula = minerals_.imaformula.replace(r"", np.nan)

    minerals_.note = minerals_.note.replace(r"", np.nan)
    minerals_.crystal_system = minerals_.crystal_system.str.lower()
    minerals_.crystal_system = minerals_.crystal_system.replace(r"", np.nan)

    minerals_.variety_of = minerals_.variety_of.str.strip()
    minerals_.synonym_of = minerals_.synonym_of.str.strip()
    minerals_.polytype_of = minerals_.polytype_of.str.strip()

    # minerals_ = migrate.minerals.copy()

    _strip_cols = [
        'physical_color',
        'physical_streak',
        'physical_cleavageNote',
        'physical_fractureNote',
        'physical_luminescence',
        'physical_lustreNote',

        'optical_type',
        'optical_sign',
        'optical_extinction',
        'optical_dispersion',
        'optical_anisotropism',
        'optical_bireflectance',
        'optical_dispersion',
        'optical_pleochroism',
        'optical_pleochroismNote',
    ]
    _array_cols = [
        'physical_transparency',
        'physical_tenacity',
        'physical_cleavage',
        'physical_fracture',
        'physical_lustre',
    ]
    _capitalize_cols = [
        'physical_color',
        'physical_luminescence',
        'physical_streak',

        'optical_color',
        'optical_tropic',
        'optical_anisotropism',
        'optical_bireflectance',
        'optical_dispersion',
    ]

    for _context in ['physical', 'optical']:
        _cols = [col for col in minerals_.columns if _context + '_' in col]
        for _col in _cols:
            if _col in _strip_cols:
                minerals_[_col] = minerals_[_col].str.strip()
                # _mask = minerals_[_col].notnull()
                # minerals_.loc[_mask, _col] = minerals_.loc[_mask, _col].apply(lambda x: x.strip() if x and isinstance(x, str) else np.nan)
            if _col in _array_cols:
                _mask = minerals_[_col].notnull()
                minerals_.loc[_mask, _col] = minerals_.loc[_mask, _col].apply(lambda x: x.split(',') if x else np.nan)
            if _col in _capitalize_cols:
                # minerals_[_col] = minerals_[_col].str.strip()
                minerals_[_col] = minerals_[_col].str.capitalize()

        _mask = minerals_[_cols].notnull().any(axis=1)
        _temp = minerals_.loc[_mask, _cols].copy()
        _temp.columns = _temp.columns.str.replace(_context + '_', '', regex=True)
        minerals_.loc[_mask, _context + '_context'] = _temp.to_dict(orient='records')

    return minerals_


def prepare_minerals_formula(minerals):
    minerals_ = minerals.copy()

    minerals_["source_id"] = 2
    minerals_ima_ = minerals_.loc[~minerals_["imaformula"].isna()][
        ["mindat_id", "name", "imaformula", "note"]
    ]
    del minerals_["imaformula"]
    minerals_ima_["source_id"] = 3
    minerals_ima_["note"] = np.nan
    minerals_ima_.rename(columns={"imaformula": "formula"}, inplace=True)
    minerals_formula = pd.concat([minerals_, minerals_ima_])
    minerals_formula.dropna(
        how="all",
        subset=[
            "formula",
            "note",
        ],
        inplace=True,
    )
    return minerals_formula


def prepare_minerals_relation_status(minerals):
    _minerals = minerals.copy()
    _minerals.dropna(how="all", inplace=True, subset=["variety_of", "synonym_of", "polytype_of"])
    _minerals = pd.melt(_minerals, id_vars=["name"], value_vars=["variety_of", "synonym_of", "polytype_of"], var_name="status_id",
            value_name="relation")

    _minerals = _minerals.loc[~_minerals["relation"].isna()]

    _minerals.loc[_minerals["status_id"] == "variety_of", "status_id"] = STATUS_UNCERTAIN_VARIETY
    _minerals.loc[_minerals["status_id"] == "synonym_of", "status_id"] = STATUS_SYNONYM_UNCERTAIN
    _minerals.loc[_minerals["status_id"] == "polytype_of", "status_id"] = STATUS_POLYTYPE

    _minerals['direct_status'] = True
    _minerals_mirror = _minerals[['relation', 'status_id', 'name']]
    _minerals_mirror.rename(columns={'relation': 'name','name': 'relation'}, inplace=True)
    _minerals_mirror['direct_status'] = False
    _minerals = pd.concat([_minerals, _minerals_mirror])

    return _minerals


# Utils functions from Pavel Martynov https://github.com/Medwar/mindatapi/blob/master/src/apps/api/utils.py
def allgreektome(txt):
    greek = {
        '\\ualpha': 'Α', '\\alpha': 'α',
        '\\ubeta': 'Β', '\\beta': 'β',
        '\\ugamma': 'Γ', '\\gamma': 'γ',
        '\\udelta': 'Δ', '\\delta': 'δ',
        '\\uepsilon': 'Ε', '\\epsilon': 'ε',
        '\\utheta': 'Θ', '\\theta': 'θ',
        '\\zeta': 'ζ', '\\uzeta': 'Ζ',
        '\\ueta': 'Η', '\\eta': 'η',

        '\\uiota': 'Ι', '\\iota': 'ι',
        '\\ukappa': 'Κ', '\\kappa': 'κ',

        '\\ulambda': 'Λ', '\\lambda': 'λ',
        '\\mu': 'μ', '\\umu': 'Μ',
        '\\nu': 'ν', '\\unu': 'Ν',
        '\\xi': 'ξ', '\\uxi': 'Ξ',
        '\\omicron': 'ο', '\\uomicron': 'Ο',
        '\\pi': 'π', '\\upi': 'Π',
        '\\rho': 'ρ', '\\urho': 'Ρ',
        '\\sigma': 'σ', '\\usigma': 'Σ',
        '\\tau': 'τ', '\\utau': 'Τ',
        '\\upsilon': 'υ', '\\uupsilon': 'Υ',
        '\\phi': 'φ', '\\uphi': 'Φ',
        '\\chi': 'χ', '\\uchi': 'Χ',
        '\\psi': 'ψ', '\\upsi': 'Ψ',
        '\\omega': 'ω', '\\uomega': 'Ω'
    }
    for k, v in greek.items():
        txt.replace(k, v)
    return txt


def fixGroupFormula(txt):
    for x in range(25):
        match = '$'+chr(97+x)
        rep = '<b>'+chr(65+x)+'</b>'
        txt.replace(match, rep)
    return txt


def isGroupFormula(txt):
    found = False
    for x in range(25):
        match = '$'+chr(97+x)
        if match in txt:
            found = True
    return found


def simpleformula(text):
    text = text.strip()
    text = strip_tags(text)
    text = text.replace("[]", "&#9723;")\
        .replace("≤", "<=")\
        .replace("≥", ">=")
    text = allgreektome(text)
    text = fixGroupFormula(text)

    extchr = False
    big = True
    anyc = False
    insup = False
    last_num = False
    subs = ""
    sups = ""
    out = ""

    for c in text:
        if c == '&':
            extchr = True
        if c == ';':
            extchr = False
        if c == '^' and not extchr:
            last_num = False
            if big:
                subs = ""
                big = False
            anyc = True
        elif c == '#' and not extchr:
            insup = not insup
        elif insup:
            sups = sups + c
        else:
            if not big and (c in "1234567890-+xyn@Σ<>=" or anyc):
                if c == '@':
                    c = "."
                subs = subs + c
                anyc = False
            elif sups or subs:
                if sups:
                    out = out + "<sup>" + sups + "</sup>"
                if subs:
                    out = out + "<sub>" + subs + "</sub>"
                big = True
                sups = ""
                subs = ""
            if big:
                if not last_num and c == ".":
                    c = ' &middot; '
                out = out + c
                last_num = c.isnumeric()

    if sups:
        out = out + "<sup>" + sups + "</sup>"
    if subs:
        out = out + "<sub>" + subs + "</sub>"

    out = fixGroupFormula(out)
    return out


def _prepare_rruff():
    with open("data/real-formulas-rruff.json", "r") as f:
        _formulas = json.load(f)
    for _item in _formulas:
        for index, _ in enumerate(_item['links']):
            if _ == "https://":
                _item['links'].pop(index)

    for _item in _formulas:
        for index, _ in enumerate(_item['links']):
            if re.match(r".*_database_code_amcsd.*", _):
                _item['amcsd_id'] = re.findall(r"_database_code_amcsd%20(\d+)", _)[0]

    formulas = pd.DataFrame(_formulas)
    formulas.replace(r"&nbsp;", "", regex=True, inplace=True)
    formulas.replace(r"\.$", "", regex=True, inplace=True)
    formulas.formula = formulas.formula.str.replace(r"_", "", regex=True)
    formulas.formula = formulas.formula.str.replace('*', '·')
    formulas.formula = formulas.formula.str.replace(r'<span .*?>.*?</span>', "[box]", regex=True)
    formulas.space_group = formulas.space_group.str.replace(r'<span class="o">(.*)</span>',
                                                            r'<span style="text-decoration: overline;">\1</span>',
                                                            regex=True)
    formulas.space_group = formulas.space_group.str.replace(r"<span .*>(.*)</span>", r"\1", regex=True)

    # check if formula contains any notes, e.g. regular words except for elements
    # _temp = formulas
    # _temp.formula = _temp.formula.str.replace(r"<.*?>", "", regex=True)
    # _temp = formulas.loc[formulas.formula.str.contains(r"[a-z]{4,}", regex=True, na=False)]

    formulas.note = formulas.note.str.replace(r"\xa0", "", regex=True)
    formulas.replace("", np.nan, inplace=True)
    formulas.drop_duplicates(subset=["mineral_name", "formula", "a", "b", "c", "alpha", "beta", "gamma", "volume"],
                             inplace=True)
    formulas.loc[:, ['a', 'b', 'c', 'alpha', 'beta', 'gamma', 'volume']] = \
        formulas.loc[:, ['a', 'b', 'c', 'alpha', 'beta', 'gamma', 'volume']].replace(r"[~_-°]", "", regex=True)

    for _column in ['a', 'b', 'c', 'alpha', 'beta', 'gamma', 'volume']:
        _calculate_sigma(formulas, _column)
    formulas = formulas.loc[:, ~formulas.columns.str.startswith('_')]

    formulas['is_html'] = formulas.formula.str.contains(r"<[a-z]+>", regex=True) | formulas.formula.isna()
    _mask = formulas.loc[~formulas.is_html]

    _mask.formula = _mask.formula.str.strip()
    _mask.formula = _mask.formula.str.replace(r" +", "", regex=True)
    _mask.formula = _mask.formula.str.replace(r"([A-Za-z\\+\\-\\)\]])([0-9\.Σ=]+(?![+-]))", r"\1<sub>\2</sub>", regex=True)
    _mask.formula = _mask.formula.str.replace(r"(?<=[A-Za-z])([0-9]?[+-][0-9]?)", r"<sup>\1</sup>", regex=True)

    formulas.loc[~formulas.is_html] = _mask
    formulas['source_id'] = 3
    formulas['cod_id'] = np.nan
    formulas['calculated_formula'] = np.nan
    formulas = formulas[[
        'mineral_name',
        'cod_id',
        'amcsd_id',
        'source_id',
        'a',
        'a_sigma',
        'b',
        'b_sigma',
        'c',
        'c_sigma',
        'alpha',
        'alpha_sigma',
        'beta',
        'beta_sigma',
        'gamma',
        'gamma_sigma',
        'volume',
        'volume_sigma',
        'space_group',
        'formula',
        'calculated_formula',
        'reference',
        'links',
        'note',
    ]]
    formulas = formulas.astype({
        'a': 'float64',
        'a_sigma': 'float64',
        'b': 'float64',
        'b_sigma': 'float64',
        'c': 'float64',
        'c_sigma': 'float64',
        'alpha': 'float64',
        'alpha_sigma': 'float64',
        'beta': 'float64',
        'beta_sigma': 'float64',
        'gamma': 'float64',
        'gamma_sigma': 'float64',
        'volume': 'float64',
        'volume_sigma': 'float64',
    }, errors='ignore')

    # del _mask
    # with open("data/rruff-real-formula.html", "w") as f:
    #     f.write(formulas.to_html(escape=False, index=False))

    # _formula = "(Na0.500.50)Σ=1(Fe2+1.94Al0.77Li0.15Mn0.10Mg0.04)Σ=3.00Al6.00(BO3)3(Si5.88Al0.12)Σ=6O18(OH)4"
    # _formula = re.sub(r"([A-Za-z\\+\\-\\)\]])([0-9\.Σ=]+(?![+-]))", r"\1<sub>\2</sub>", _formula)
    # _formula = re.sub(r"(?<=[A-Za-z])([0-9]?[+-][0-9]?)", r"<sup>\1</sup>", _formula)
    # print(_formula)
    formulas.replace(r"^\s+$", np.nan, regex=True, inplace=True)

    return formulas


def _prepare_cod(data):
    _cod_mr_mapping = pd.read_csv('data/cod-mr-mapping.csv')

    data.mineral_name = data.mineral_name.apply(lambda x: f'{x[0].upper()}{x[1:]}')
    data.formula = data.formula.str.replace(r"[(^\-)(\-$)(\s+)]", "", regex=True)
    data.calculated_formula = data.calculated_formula.str.replace(r"[(^\-)(\-$)(\s+)]", "", regex=True)

    data.formula = data.formula.str.replace(r"([A-Za-z\\+\\-\\)\]])([0-9\.Σ=]+(?![+-]))", r"\1<sub>\2</sub>", regex=True)
    data.formula = data.formula.str.replace(r"(?<=[A-Za-z])([0-9]?[+-][0-9]?)", r"<sup>\1</sup>", regex=True)

    data.calculated_formula = data.calculated_formula.str.replace(r"([A-Za-z\\+\\-\\)\]])([0-9\.Σ=]+(?![+-]))", r"\1<sub>\2</sub>", regex=True)
    data.calculated_formula = data.calculated_formula.str.replace(r"(?<=[A-Za-z])([0-9]?[+-][0-9]?)", r"<sup>\1</sup>", regex=True)

    data.space_group = data.space_group.str.replace(r"\s+", "", regex=True)
    data.space_group = data.space_group.str.replace(r'-(\d)',
                                                    r'<span style="text-decoration: overline;">\1</span>',
                                                    regex=True)

    data['source_id'] = 4
    data.rename(columns={'id': 'cod_id'}, inplace=True)
    # TODO: remove after migration
    # with open("data/cod-real-formula.html", "w") as f:
    #     f.write(data.to_html(escape=False, index=False))

    data = data.merge(_cod_mr_mapping, how='left', on='mineral_name')

    # substitute mineral_name with corr_mineral_name if exists
    data.loc[data.corr_mineral_name.notnull(), 'mineral_name'] = data['corr_mineral_name']

    data['note_x'] = data['note_x'].fillna('')
    data['note_y'] = data['note_y'].fillna('')
    data['note'] = data.apply(lambda x: x['note_y'] + '; ' + x['note_x'] if x['note_y'] else x['note_x'], axis=1)
    data.drop(columns=['note_x', 'note_y', 'corr_mineral_name', 'cod_id_y'], inplace=True)
    data.rename(columns={'cod_id_x': 'cod_id'}, inplace=True)
    data['note'] = data['note'].str.strip()
    data['note'] = data['note'].str.replace(r';+', ';', regex=True)
    data['note'] = data['note'].str.replace(r'^;|;$', '', regex=True)
    data['note'] = data['note'].replace('', np.nan)
    data.loc[~data[
        'amcsd_id'].isna(), 'amcsd_link'] = 'http://rruff.geo.arizona.edu/AMS/result.php?key=_database_code_amcsd%20' + \
                                            data.loc[~data['amcsd_id'].isna(), 'amcsd_id']

    data['links'] = data['links'].apply(lambda x: json.loads(x))
    data['links'] = data.apply(lambda x: x['links'] + [x['amcsd_link']] if x['amcsd_link'] is not np.nan else x['links'], axis=1)

    data.drop(columns=['amcsd_link'], inplace=True)

    return data


def _add_alternative_name(data, alternative_names):
    _columns = [
        'mineral_id',
        'cod_id',
        'amcsd_id',
        'source_id',
        'a',
        'a_sigma',
        'b',
        'b_sigma',
        'c',
        'c_sigma',
        'alpha',
        'alpha_sigma',
        'beta',
        'beta_sigma',
        'gamma',
        'gamma_sigma',
        'volume',
        'volume_sigma',
        'space_group',
        'formula',
        'calculated_formula',
        'reference',
        'links',
        'note',
    ]
    _alternative_names = alternative_names
    _special_characters = _alternative_names[_alternative_names.name.str.contains(r'[^\x00-\x7F]+')][
        ['name', 'mineral_id']]
    _special_characters.drop_duplicates(inplace=True)
    _special_characters['relation_name'] = _special_characters.name.str.normalize('NFKD').str.encode('ascii',
                                                                                                     errors='ignore') \
        .str.decode('utf-8')
    _special_characters.loc[_special_characters.name.str.contains('ø'), 'relation_name'] = _special_characters[
        'name'].str.replace('ø', 'o')
    _special_characters['relation_id'] = _special_characters.mineral_id
    _alternative_names = pd.concat([_alternative_names, _special_characters])
    _alternative_names.drop_duplicates(subset=['mineral_id', 'relation_id'], inplace=True)
    _alternative_names.sort_values(by=['name'], inplace=True)

    _insert_chunk_0 = data.merge(
        _alternative_names,
        how="inner",
        left_on="mineral_name",
        right_on="name",
    )
    _insert_chunk_0 = _insert_chunk_0[_columns + ['name']]
    _insert_chunk_0.drop_duplicates(
        subset=['mineral_id', 'formula', 'a', 'b', 'c', 'alpha', 'beta', 'gamma', 'volume',
                'reference', 'note'], inplace=True)

    _residual = data[~data.mineral_name.isin(_insert_chunk_0.name)]

    _insert_chunk_1 = _residual.merge(
        _alternative_names,
        how="left",
        left_on="mineral_name",
        right_on="relation_name",
        indicator=True,
    )

    # Activate this to check if there are any missing minerals
    # _missing = _insert_chunk_1.loc[_insert_chunk_1['_merge'] == 'left_only']
    # _missing.drop_duplicates(subset=['mineral_name'], inplace=True)
    # _missing.sort_values(by=['mineral_name'], inplace=True)
    # _missing['corr_mineral_name'] = np.nan
    # save missing minerals to a csv file
    # _missing[['cod_id', 'mineral_name', 'corr_mineral_name']].to_csv('data/_cod-mr-mapping.csv', index=False)

    _insert_chunk_1.sort_values(by=['mineral_name', 'priority'], inplace=True)
    _insert_chunk_1.drop_duplicates(subset=['relation_name', 'formula'], inplace=True, keep='first')
    _insert_chunk_1 = _insert_chunk_1[_columns + ['name']]

    insert = pd.concat([_insert_chunk_0, _insert_chunk_1])
    insert.sort_values(by=['name'], inplace=True)
    insert = insert[_columns]
    insert = insert[insert.mineral_id.notnull()]

    return insert


def prepare_mineral_structure(cod, alternative_names):
    # cod = migrate.cod
    _cod = _prepare_cod(cod)
    _rruff = _prepare_rruff()

    cod = _add_alternative_name(_cod, alternative_names)
    rruff = _add_alternative_name(_rruff, alternative_names)

    data = pd.concat([cod, rruff], ignore_index=True)
    data['_id'] = data.reset_index(drop=True).index

    _data = data.groupby(['amcsd_id'], dropna=True).agg(
        {
            '_id': lambda x: x.tolist(),
            'links': lambda x: list(set([_x for _x in x if _x is not np.nan][0])).sort()
        }
    )

    _data = _data.explode('_id')
    _data.sort_values(by=['_id'], inplace=True)
    data = data.merge(_data, how='left', on='_id', suffixes=('', '_y'))
    data.loc[data['links_y'].notnull(), 'links'] = data.loc[data['links_y'].notnull(), 'links_y']
    data.drop(columns=['links_y', '_id'], inplace=True)
    _amcsd = data.loc[~data['amcsd_id'].isna(),]
    _no_amcsd = data.loc[data['amcsd_id'].isna(),]
    _amcsd.sort_values(by=['amcsd_id', 'cod_id'], inplace=True)
    _amcsd.drop_duplicates(subset=['amcsd_id'], inplace=True, keep='first')

    data = pd.concat([_amcsd, _no_amcsd], ignore_index=True)

    # with open("data/cod-rruff-check.html", "w") as f:
    #     f.write(data.loc[0:1000].to_html(escape=False, index=False))

    return data


def _calculate_sigma(dataframe, column_name):
    _sigma_col = column_name + '_sigma'
    _precision_col = '_' + column_name + '_precision'

    # close brackets
    dataframe[column_name] = dataframe[column_name].str.replace(r'(\([0-9.]+(?!.*\)))', r'\1)', regex=True)

    dataframe[_sigma_col] = dataframe[column_name].str.extract(r"\((.*?)\)", expand=False)
    dataframe[_sigma_col] = dataframe[_sigma_col].str.replace(r"\.", "", regex=True)
    dataframe[column_name] = dataframe[column_name].str.replace(r"\(.*?\)", "", regex=True)
    dataframe[_precision_col] = dataframe[column_name].str.extract(r"\.(\d+)", expand=False).str.len()
    dataframe.loc[dataframe[_sigma_col].notna() & dataframe[_precision_col].isna(), _precision_col] = 0
    dataframe.loc[dataframe[_sigma_col].notna(), _sigma_col] = dataframe.loc[dataframe[_sigma_col].notna()] \
        .apply(lambda x: '0.' + int(x[_precision_col] - 1) * '0' + x[_sigma_col], axis=1)
