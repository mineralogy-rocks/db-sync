# -*- coding: UTF-8 -*-
import concurrent.futures
import os
import re
import sys
from datetime import datetime
from time import perf_counter

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from psycopg2 import Error as psycopgError
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool

from src.queries import (
    delete_mineral_relation_suggestion,
    get_mineral_formula,
    get_mineral_history,
    get_mineral_log,
    get_mineral_relation_suggestion,
    get_minerals,
    get_relations,
    insert_mineral_formula,
    insert_mineral_history,
    insert_mineral_log,
    insert_mineral_relation_suggestion,
    update_mineral_history,
    update_mineral_log,
    update_mineral_relation_suggestion,
)
from src.utils import prepare_minerals, prepare_minerals_formula

register_adapter(np.int64, AsIs)
load_dotenv(".envs/.local/.mindat")
load_dotenv(".envs/.local/.mr")


class Migration:
    def __init__(self):
        self.mindat_connection_params = (
            f"mysql+pymysql://"
            f"{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}@127.0.0.1/"
            f"{os.getenv('MYSQL_DATABASE')}"
        )
        self.mr_connection_params = {
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
        }
        self.pool = None

        self.mineral_log = None
        self.mineral_history = None
        self.mineral_formula = None
        self.mineral_relation_suggestion = None

        self.minerals = None

    def connect_db(self):

        try:
            self.pool = ThreadedConnectionPool(
                minconn=1, maxconn=50, **self.mr_connection_params
            )
            print("Pool with MR database created.")
        except psycopgError as e:
            print("An error occurred when establishing a connection with mr db: %s" % e)
            sys.exit(1)

    def disconnect_db(self):
        print("disconnecting from db...")
        self.pool.closeall()

    def fetch_tables(self):

        s = perf_counter()
        queries = [
            {"table_name": "mineral_log", "query": get_mineral_log},
            {"table_name": "mineral_history", "query": get_mineral_history},
            {"table_name": "mineral_formula", "query": get_mineral_formula},
            {
                "table_name": "mineral_relation_suggestion",
                "query": get_mineral_relation_suggestion,
            },
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

    def psql_pd_get(self, query, table_name):
        try:
            conn = self.pool.getconn()
            retrieved_ = (
                pd.read_sql_query(
                    query,
                    conn,
                )
                .fillna(value=np.nan)
                .reset_index(drop=True)
            )
            setattr(self, table_name, retrieved_)

        except Exception as e:
            print("An error occurred when creating %s: %s" % (table_name, e))

        finally:
            self.pool.putconn(conn)

    def get_minerals(self):

        try:
            minerals_ = (
                pd.read_sql_query(
                    get_minerals,
                    self.mindat_connection_params,
                )
                .fillna(value=np.nan)
                .sort_values("name")
                .reset_index(drop=True)
            )

            self.minerals = prepare_minerals(minerals_)

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

        if len(insert) > 0:
            try:
                retrieved_ = self.execute_query(insert, insert_mineral_formula)
                self.save_report(
                    retrieved_, table_name="mineral_formula", operation="insert"
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

    def execute_query(self, df, query):

        df = df.replace({np.nan: None})
        tuples = [tuple(x) for x in df.to_numpy()]

        conn = self.pool.getconn()
        cursor = conn.cursor()
        # cursor.mogrify(query % tuples[:10])

        try:
            retrieved = execute_values(cursor, query, tuples, fetch=True)
        except psycopgError as e:
            print("An error occurred: %s" % e)
            conn.rollback()
            return 1

        else:
            conn.commit()
            print("The db was updated with %s records" % len(tuples))
            return retrieved

        finally:
            cursor.close()
            self.pool.putconn(conn)

    @staticmethod
    def save_report(data: pd.DataFrame, table_name: str, operation: str) -> None:

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        date = datetime.today().strftime("%d.%m.%Y__%H-%M")
        filename = f"{operation}_{table_name}_{date}.csv"

        data.to_csv(f"db/reports/{filename}", index=False)


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
