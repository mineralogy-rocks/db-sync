# -*- coding: UTF-8 -*-
import numpy as np
import pandas as pd

from src.constants import (
    STATUS_SYNONYM_UNCERTAIN,
    STATUS_UNCERTAIN_VARIETY,
    STATUS_POLYTYPE,
)


def prepare_minerals(minerals):
    minerals_ = minerals.copy()
    minerals_ = minerals_.replace(0, np.nan)
    minerals_["discovery_year"] = pd.to_numeric(
        minerals_["discovery_year"], errors="coerce"
    )
    minerals_["name"] = minerals_["name"].str.strip()
    minerals_.description = minerals_.description.replace(r"", np.nan)
    minerals_.ima_symbol = minerals_.ima_symbol.replace(r"", np.nan)
    minerals_.formula = minerals_.formula.replace(r"", np.nan)
    minerals_.imaformula = minerals_.imaformula.replace(r"", np.nan)
    minerals_.note = minerals_.note.replace(r"", np.nan)
    minerals_.crystal_system = minerals_.crystal_system.str.lower()
    minerals_.crystal_system = minerals_.crystal_system.replace(r"", np.nan)

    minerals_.variety_of = minerals_.variety_of.str.strip()
    minerals_.synonym_of = minerals_.synonym_of.str.strip()
    minerals_.polytype_of = minerals_.polytype_of.str.strip()

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
