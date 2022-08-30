# -*- coding: UTF-8 -*-
import concurrent.futures
import sys
from datetime import datetime
from time import perf_counter

import numpy as np
import pandas as pd
from psycopg2 import Error as psycopgError
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool

from src.queries import (
    get_mineral_formula,
    get_mineral_history,
    get_mineral_log,
    get_minerals,
    insert_mineral_history,
    insert_mineral_log,
    update_mineral_history,
    update_mineral_log,
)
from src.utils import prepare_minerals, prepare_minerals_formula


class Migration:
    def __init__(self):
        self.mindat_connection_params = "mysql+pymysql://mindat:root@127.0.0.1/mindat"
        self.mr_connection_params = {
            "dbname": "postgres",
            "user": "gpminerals",
            "password": "gpmineralsDB",
            "host": "127.0.0.1",
            "port": 5432,
        }
        self.pool = None

        self.mineral_log = None
        self.mineral_history = None
        self.mineral_formula = None

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

    def sync_mineral_log(self):

        assert len(self.mineral_log)
        assert len(self.minerals)

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
            columns={
                "mindat_id_y": "mindat_id",
                "description_y": "description",
                "ima_symbol_y": "ima_symbol",
            },
            inplace=True,
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

        old_.rename(
            columns={
                "mindat_id_x": "mindat_id",
                "description_x": "description",
                "ima_symbol_x": "ima_symbol",
            },
            inplace=True,
        )
        new_.rename(
            columns={
                "mindat_id_y": "mindat_id",
                "description_y": "description",
                "ima_symbol_y": "ima_symbol",
            },
            inplace=True,
        )

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

        assert len(self.mineral_formula)
        assert len(self.minerals)

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
            columns={
                "mindat_id_y": "mindat_id",
                "formula_y": "formula",
                "note_y": "note",
                "source_id_y": "source_id",
            },
            inplace=True,
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
                retrieved_ = self.execute_query(insert, insert_mineral_history)
                self.save_report(
                    retrieved_, table_name="mineral_history", operation="insert"
                )
            except Exception:
                # TODO: save log?
                pass

    def sync_mineral_history(self):

        assert len(self.mineral_history)
        assert len(self.minerals)

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
            columns={
                "discovery_year_y": "discovery_year",
                "ima_year_y": "ima_year",
                "approval_year_y": "approval_year",
                "publication_year_y": "publication_year",
            },
            inplace=True,
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

        old_.rename(
            columns={
                "discovery_year_x": "discovery_year",
                "ima_year_x": "ima_year",
                "approval_year_x": "approval_year",
                "publication_year_x": "publication_year",
            },
            inplace=True,
        )
        new_.rename(
            columns={
                "discovery_year_y": "discovery_year",
                "ima_year_y": "ima_year",
                "approval_year_y": "approval_year",
                "publication_year_y": "publication_year",
            },
            inplace=True,
        )

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


migrate = Migration()
migrate.connect_db()
migrate.get_minerals()
migrate.fetch_tables()

migrate.sync_mineral_log()
migrate.sync_mineral_history()
outer_join = migrate.mineral_log.merge(
    migrate.minerals, how="outer", on="name", indicator=True
)
