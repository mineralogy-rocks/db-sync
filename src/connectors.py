# -*- coding: UTF-8 -*-
import concurrent.futures
import os
import re
import sys
import json
from datetime import datetime
from time import perf_counter

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from psycopg2 import Error as psycopgError
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values, Json
from psycopg2.pool import ThreadedConnectionPool

from src.queries import (
    delete_mineral_relation_suggestion,
    get_alternative_names,
    get_mineral_crystallography,
    get_mineral_formula,
    get_mineral_history,
    get_mineral_ima_status,
    get_mineral_ima_note,
    get_mineral_log,
    get_mineral_relation,
    get_mineral_relation_suggestion,
    get_mineral_status,
    get_minerals,
    get_relations,
    get_cod,
    insert_mineral_context,
    insert_mineral_crystallography,
    insert_mineral_formula,
    insert_mineral_status,
    insert_mineral_history,
    insert_mineral_ima_status,
    insert_mineral_ima_note,
    insert_mineral_log,
    insert_mineral_relation,
    insert_mineral_relation_suggestion,
    insert_mineral_structure,
    update_mineral_history,
    update_mineral_crystallography,
    update_mineral_log,
    update_mineral_relation_suggestion,
)
from src.utils import (
    prepare_minerals,
    prepare_minerals_formula,
    prepare_minerals_relation_status,
    prepare_mineral_structure,
)
from src.base import Migrator

register_adapter(np.int64, AsIs)
register_adapter(dict, Json)

load_dotenv(".envs/.local/.mindat")
load_dotenv(".envs/.prod/.cod")


class Migration(Migrator):
    def __init__(self, env="dev"):
        super().__init__(env)

        self.mineral_log = None
        self.mineral_context = None
        self.mineral_history = None
        self.mineral_ima_status = None
        self.mineral_ima_note = None
        self.mineral_formula = None
        self.mineral_status = None
        self.mineral_relation = None
        self.mineral_relation_suggestion = None

        self.minerals = None
        self.relations = None

        self.cod = None

    def fetch_tables(self):

        s = perf_counter()
        queries = [
            {"table_name": "mineral_log", "query": get_mineral_log},
            {"table_name": "mineral_history", "query": get_mineral_history},
            {"table_name": "mineral_formula", "query": get_mineral_formula},
            {"table_name": "mineral_crystallography", "query": get_mineral_crystallography},
            {
                "table_name": "mineral_relation_suggestion",
                "query": get_mineral_relation_suggestion,
            },
            {"table_name": "mineral_status", "query": get_mineral_status},
            {"table_name": "mineral_relation", "query": get_mineral_relation},
            {"table_name": "mineral_ima_status", "query": get_mineral_ima_status},
            {"table_name": "mineral_ima_note", "query": get_mineral_ima_note},
        ]

        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_table = {
                executor.submit(self.psql_pd_get, **query): query for query in queries
            }
            for future in concurrent.futures.as_completed(future_to_table):
                try:
                    future.result()
                except Exception as e:
                    print("an error occurred when running query: %s" % e)

        elapsed = perf_counter() - s
        print(f"Tables generated in {elapsed:0.2f} seconds.")


    def get_minerals(self):

        try:
            _minerals = (
                pd.read_sql_query(
                    get_minerals,
                    self.mindat_connection_params,
                )
                .fillna(value=np.nan)
                .sort_values("name")
                .reset_index(drop=True)
            )

            self.minerals = prepare_minerals(_minerals)

        except Exception as e:
            print(f"An error occurred when creating minerals: {e}")

    def get_relations(self):

        try:
            relations_ = (
                pd.read_sql_query(
                    get_relations,
                    self.mindat_connection_params,
                )
                .fillna(value=np.nan)
                .sort_values("id")
                .reset_index(drop=True)
            )

            self.relations = relations_

        except Exception as e:
            print(f"An error occurred when creating relations: {e}")

    def get_cod(self):
        try:
            _data = (
                pd.read_sql_query(
                    get_cod,
                    self.cod_connection_params,
                )
                .fillna(value=np.nan)
                .sort_values("id")
                .reset_index(drop=True)
            )

            self.cod = _data

        except Exception as e:
            print(f"An error occurred when creating cod data: {e}")

    def get_alternative_names(self):
        try:
            conn = self.pool.getconn()
            retrieved_ = (
                pd.read_sql_query(
                    get_alternative_names,
                    conn,
                )
                .fillna(value=np.nan)
                .reset_index(drop=True)
            )
            return retrieved_

        except Exception as e:
            print("An error occurred when retrieving %s: %s" % ("alternative names", e))

        finally:
            self.pool.putconn(conn)

    def sync_mineral_ima_status(self):

        assert self.mineral_ima_status is not None
        assert self.minerals is not None

        columns = [
            'name',
            'ima_status',
        ]
        _minerals = self.minerals[['name', 'ima_status']].explode('ima_status').dropna()

        outer_join = self.mineral_ima_status.merge(
            _minerals[columns], how='outer', on='name', indicator=True
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates(
            [
                "name",
                "ima_status",
            ]
        )
        insert = insert[columns]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_ima_status)
                self.save_report(
                    retrieved_, table_name="mineral_ima_status", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_ima_note(self):

        assert self.mineral_ima_note is not None
        assert self.minerals is not None

        columns = [
            'name',
            'ima_note',
        ]
        _minerals = self.minerals[['name', 'ima_note']].explode('ima_note').dropna()

        outer_join = self.mineral_ima_status.merge(
            _minerals[columns], how='outer', on='name', indicator=True
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates(
            [
                "name",
                "ima_note",
            ]
        )
        insert = insert[columns]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_ima_note)
                self.save_report(
                    retrieved_, table_name="mineral_ima_note", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_context(self):

        assert self.minerals is not None

        columns = [
            'name',
            'data',
            'context_id',
        ]
        # minerals = migrate.minerals.copy()
        _minerals = self.minerals[['name', 'physical_context', 'optical_context']].copy()
        _physical_context = _minerals[['name', 'physical_context']].dropna()
        _physical_context['context_id'] = 1
        _physical_context.rename(columns={'physical_context': 'data'}, inplace=True)
        _optical_context = _minerals[['name', 'optical_context']].dropna()
        _optical_context['context_id'] = 2
        _optical_context.rename(columns={'optical_context': 'data'}, inplace=True)
        insert = pd.concat([_physical_context, _optical_context], axis=0)
        insert['data'] = insert['data'].apply(lambda x: {k: v if not isinstance(v, float) or not np.isnan(v) else None for k, v in x.items()})
        insert = insert[columns]

        if len(insert) > 0:
            _conn = self.pool.getconn()
            with _conn.cursor() as cursor:
                cursor.execute("TRUNCATE TABLE mineral_context RESTART IDENTITY")
                _conn.commit()
                cursor.close()
            self.pool.putconn(_conn)
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_context)
                self.save_report(
                    retrieved_, table_name="mineral_context", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_log(self):

        assert self.mineral_log is not None
        assert self.minerals is not None

        columns_ = [
            "name",
            "description",
            "mindat_id",
            "ima_symbol",
        ]

        outer_join = self.mineral_log.merge(
            self.minerals[columns_], how="outer", on="name", indicator=True
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates("name")
        insert = insert[columns_]

        if len(insert) > 0:
            try:
                self.execute_query(insert, insert_mineral_log)
                self.save_report(insert, table_name="mineral_log", operation="insert")
            except Exception:
                # TODO: save log?
                pass

        # Update
        update = outer_join[(outer_join._merge == "both")].drop("_merge", axis=1)
        update = update.drop_duplicates("name")

        old_ = update[
            [
                "id",
                "description_x",
                "mindat_id_x",
                "ima_symbol_x",
            ]
        ]
        new_ = update[
            [
                "id",
                "description_y",
                "mindat_id_y",
                "ima_symbol_y",
            ]
        ]

        old_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)
        new_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)

        diff = old_.compare(new_, keep_shape=False).dropna(how="all", axis=1)

        if len(diff) > 0:
            update_ = update.loc[diff.index]
            update_.rename(
                columns={
                    "description_y": "description",
                    "mindat_id_y": "mindat_id",
                    "ima_symbol_y": "ima_symbol",
                },
                inplace=True,
            )
            update_ = update_[["id", "description", "mindat_id", "ima_symbol"]]
            try:
                retrieved_ = self.execute_query(update_, update_mineral_log)
                self.save_report(
                    retrieved_, table_name="mineral_log", operation="update"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_crystallography(self):

        assert self.mineral_crystallography is not None
        assert self.minerals is not None

        columns_ = [
            "name",
            "crystal_system",
        ]
        minerals_ = self.minerals[['mindat_id', 'name', 'crystal_system']].dropna(
            how="all",
            subset=[
                "crystal_system",
            ],
        )

        outer_join = self.mineral_crystallography.merge(
            minerals_, how="outer", on="name", indicator=True
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates(
            [
                "name",
                "crystal_system",
            ]
        )
        insert = insert[columns_]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_crystallography)
                self.save_report(
                    retrieved_, table_name="mineral_crystallography", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

        # Update
        update = outer_join[(outer_join._merge == "both")].drop("_merge", axis=1)
        old_ = update[
            [
                "name",
                "crystal_system_x",
            ]
        ]
        new_ = update[
            [
                "name",
                "crystal_system_y",
            ]
        ]
        old_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)
        new_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)

        diff = old_.compare(new_, keep_shape=False).dropna(how="all", axis=1)

        if len(diff) > 0:
            update_ = update.loc[diff.index]
            update_.rename(
                columns={
                    "crystal_system_y": "crystal_system",
                },
                inplace=True,
            )
            update_ = update_[
                [
                    "name",
                    "crystal_system",
                ]
            ]
            try:
                retrieved_ = self.execute_query(
                    update_, update_mineral_crystallography
                )
                self.save_report(
                    retrieved_,
                    table_name="mineral_crystallography",
                    operation="update",
                )
            except Exception:
                # TODO: save log?
                pass


    def sync_mineral_formula(self):

        assert self.mineral_formula is not None
        assert self.minerals is not None

        columns_ = [
            "name",
            "formula",
            "note",
            "source_id",
        ]
        minerals_ = prepare_minerals_formula(
            self.minerals[["mindat_id", "name", "formula", "imaformula", "note"]]
        )

        outer_join = self.mineral_formula.merge(
            minerals_.dropna(
                how="all",
                subset=[
                    "formula",
                    "note",
                ],
            ),
            how="outer",
            on="name",
            indicator=True,
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates(
            [
                "name",
                "source_id",
            ]
        )
        insert = insert[columns_]
        insert['type_id'] = 1
        insert['reference'] = insert.apply(lambda row: [], axis=1)

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_formula)
                self.save_report(
                    retrieved_, table_name="mineral_formula", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_status(self):

        assert self.mineral_status is not None
        assert self.minerals is not None

        _columns = [
            "name",
            "status_id",
            "direct_relation",
        ]
        _minerals = prepare_minerals_relation_status(
            self.minerals[["name", "variety_of", "synonym_of", "polytype_of"]]
        )

        outer_join = self.mineral_status.merge(
            _minerals[['name', 'status_id', 'direct_status']],
            how="outer",
            on=["name", "status_id", "direct_status"],
            indicator=True,
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates(
            [
                "name",
                "status_id",
                "direct_status",
            ]
        )
        insert = insert[['name', 'status_id', 'direct_status']]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_status)
                self.save_report(
                    retrieved_, table_name="mineral_status", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_relation(self):

        assert self.mineral_relation is not None
        assert self.minerals is not None

        _columns = [
            "name",
            "status_id",
            "relation",
            "direct_relation",
        ]
        _minerals = prepare_minerals_relation_status(
            self.minerals[["name", "variety_of", "synonym_of", "polytype_of"]]
        )

        outer_join = self.mineral_relation.merge(
            _minerals,
            how="outer",
            on=["name", "status_id", "relation"],
            indicator=True,
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates(
            [
                "name",
                "status_id",
                "relation",
                "direct_status",
            ]
        )
        insert = insert[['name', 'status_id', "relation", 'direct_status']]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_relation)
                self.save_report(
                    retrieved_, table_name="mineral_relation", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_relation_suggestion(self):

        assert self.mineral_relation_suggestion is not None
        assert self.relations is not None

        columns_ = [
            "id",
            "mineral_id",
            "relation_id",
            "relation_type_id",
        ]

        outer_join = self.mineral_relation_suggestion.merge(
            self.relations[columns_].dropna(
                how="all",
            ),
            how="outer",
            on="id",
            indicator=True,
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert[columns_]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(
                    insert, insert_mineral_relation_suggestion
                )
                if len(retrieved_):
                    self.save_report(
                        retrieved_,
                        table_name="mineral_relation_suggestion",
                        operation="insert",
                    )
            except Exception:
                # TODO: save log?
                pass

        # Update
        update = outer_join[(outer_join._merge == "both")].drop("_merge", axis=1)

        old_ = update[
            [
                "id",
                "mineral_id_x",
                "relation_id_x",
                "relation_type_id_x",
            ]
        ]
        new_ = update[
            [
                "id",
                "mineral_id_y",
                "relation_id_y",
                "relation_type_id_y",
            ]
        ]

        old_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)
        new_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)

        diff = old_.compare(new_, keep_shape=False).dropna(how="all", axis=1)

        if len(diff) > 0:
            update_ = update.loc[diff.index]
            update_.rename(
                columns={
                    "id_y": "id",
                    "mineral_id_y": "mineral_id",
                    "relation_id_y": "relation_id",
                    "relation_type_id_y": "relation_type_id",
                },
                inplace=True,
            )
            update_ = update_[
                [
                    "id",
                    "mineral_id",
                    "relation_id",
                    "relation_type_id",
                ]
            ]
            try:
                retrieved_ = self.execute_query(
                    update_, update_mineral_relation_suggestion
                )
                self.save_report(
                    retrieved_,
                    table_name="mineral_relation_suggestion",
                    operation="update",
                )
            except Exception:
                # TODO: save log?
                pass

        # Delete
        delete = outer_join[(outer_join._merge == "left_only")].drop("_merge", axis=1)
        delete = delete[["id", "mineral_id_x"]]

        if len(delete) > 0:
            try:
                retrieved_ = self.execute_query(
                    delete, delete_mineral_relation_suggestion
                )
                self.save_report(
                    retrieved_,
                    table_name="mineral_relation_suggestion",
                    operation="delete",
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_history(self):

        assert self.mineral_history is not None
        assert self.minerals is not None

        columns_ = [
            "name",
            "discovery_year",
            "ima_year",
            "approval_year",
            "publication_year",
        ]

        outer_join = self.mineral_history.merge(
            self.minerals[columns_].dropna(
                how="all",
                subset=[
                    "discovery_year",
                    "ima_year",
                    "approval_year",
                    "publication_year",
                ],
            ),
            how="outer",
            on="name",
            indicator=True,
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True
        )
        insert = insert.drop_duplicates("name")
        insert = insert[columns_]

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_history)
                self.save_report(
                    retrieved_, table_name="mineral_history", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

        # Update
        update = outer_join[(outer_join._merge == "both")].drop("_merge", axis=1)
        update = update.drop_duplicates("name")

        old_ = update[
            [
                "id",
                "discovery_year_x",
                "ima_year_x",
                "approval_year_x",
                "publication_year_x",
            ]
        ]
        new_ = update[
            [
                "id",
                "discovery_year_y",
                "ima_year_y",
                "approval_year_y",
                "publication_year_y",
            ]
        ]

        old_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)
        new_.rename(columns=lambda column_: re.sub("_[xy]$", "", column_), inplace=True)

        diff = old_.compare(new_, keep_shape=False).dropna(how="all", axis=1)

        if len(diff) > 0:
            update_ = update.loc[diff.index]
            update_.rename(
                columns={
                    "discovery_year_y": "discovery_year",
                    "ima_year_y": "ima_year",
                    "approval_year_y": "approval_year",
                    "publication_year_y": "publication_year",
                },
                inplace=True,
            )
            update_ = update_[
                [
                    "id",
                    "discovery_year",
                    "ima_year",
                    "approval_year",
                    "publication_year",
                ]
            ]
            try:
                retrieved_ = self.execute_query(update_, update_mineral_history)
                self.save_report(
                    retrieved_, table_name="mineral_history", operation="update"
                )
            except Exception:
                # TODO: save log?
                pass


    def sync_rruff_cod(self):
        _cod = self.cod
        _alternative_names = self.get_alternative_names()
        # alternative_names = migrate.get_alternative_names()
        insert = prepare_mineral_structure(_cod, _alternative_names)

        try:
            retrieved_ = self.execute_query(insert, insert_mineral_structure)
            self.save_report(
                retrieved_, table_name="mineral_structure", operation="insert"
            )
        except Exception:
            # TODO: save log?
            pass

# migrate = Migration()
# migrate.connect_db()
# migrate.get_minerals()
# migrate.get_relations()
# migrate.fetch_tables()
#
#
# migrate.sync_mineral_log()
# migrate.sync_mineral_history()
# migrate.sync_mineral_formula()
# migrate.sync_mineral_relation_suggestion()
#
# migrate.disconnect_db()
