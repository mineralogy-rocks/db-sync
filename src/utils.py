# -*- coding: UTF-8 -*-
import numpy as np
import pandas as pd


def prepare_minerals(minerals):
    minerals_ = minerals.copy()
    minerals_ = minerals_.replace(0, np.nan)
    minerals_["discovery_year"] = pd.to_numeric(
        minerals_["discovery_year"], errors="coerce"
    )
    minerals_.description = minerals_.description.replace(r"", np.nan)
    minerals_.ima_symbol = minerals_.ima_symbol.replace(r"", np.nan)
    minerals_.formula = minerals_.formula.replace(r"", np.nan)
    minerals_.imaformula = minerals_.imaformula.replace(r"", np.nan)
    minerals_.note = minerals_.note.replace(r"", np.nan)
    minerals_.crystal_system = minerals_.crystal_system.str.lower()
    minerals_.crystal_system = minerals_.crystal_system.replace(r"", np.nan)

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
