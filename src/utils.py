# -*- coding: UTF-8 -*-
import numpy as np
import pandas as pd


def prepare_minerals(minerals):
    minerals = minerals.replace(0, np.nan)
    minerals["discovery_year"] = pd.to_numeric(
        minerals["discovery_year"], errors="coerce"
    )
    minerals.description = minerals.description.replace(r"", np.nan)
    minerals.ima_symbol = minerals.ima_symbol.replace(r"", np.nan)
    minerals.formula = minerals.formula.replace(r"", np.nan)
    minerals.imaformula = minerals.imaformula.replace(r"", np.nan)
    minerals.note = minerals.note.replace(r"", np.nan)

    return minerals


def prepare_minerals_formula(minerals):
    minerals["source_id"] = 2
    minerals_ima_ = minerals.loc[~minerals["imaformula"].isna()][
        ["mindat_id", "name", "imaformula", "note"]
    ]
    del minerals["imaformula"]
    minerals_ima_["source_id"] = 3
    minerals_ima_.rename(columns={"imaformula": "formula"}, inplace=True)
    minerals_formula = pd.concat([minerals, minerals_ima_])
    minerals_formula.dropna(
        how="all",
        subset=[
            "formula",
            "note",
        ],
        inplace=True,
    )
    return minerals_formula
