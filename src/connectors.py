# -*- coding: UTF-8 -*-
import sys

import numpy as np
import pandas as pd
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool


class Migration:
    def __init__(self):
        self.mindat_connection_str = "mysql+pymysql://mindat:root@127.0.0.1/mindat"
        self.pool = None
        self.mineral_log = None
        self.minerals = None

    def connect_db(self):
        self.pool = self.create_pool()

    def disconnect_db(self):
        print("disconnecting from db...")
        self.pool.closeall()

    def create_pool(self):

        connection_params = {
            "dbname": "postgres",
            "user": "gpminerals",
            "password": "gpmineralsDB",
            "host": "127.0.0.1",
            "port": 5432,
        }

        try:
            print("Creating pool with PostgreSQL database...")
            pool = ThreadedConnectionPool(minconn=1, maxconn=50, **connection_params)
            return pool
        except Exception as e:
            print(e)
            sys.exit(1)

    def get_mineral_log(self):

        try:
            conn = self.pool.getconn()
            self.mineral_log = (
                pd.read_sql_query(
                    "SELECT ml.id, ml.name, ml.formula, ml.note, ml.created_at, ml.updated_at,"
                    "ml.seen, ml.description, ml.mindat_id FROM mineral_log ml;",
                    conn,
                )
                .fillna(value=np.nan)
                .sort_values("name")
                .reset_index(drop=True)
            )

        except Exception as e:
            print(f"An error occurred when creating mineral_log: {e}")

        finally:
            self.pool.putconn(conn)

    def get_minerals(self):

        try:
            self.minerals = (
                pd.read_sql_query(
                    "SELECT ml.id, ml.name AS name, ml.imayear AS ima_year, ml.yeardiscovery AS discovery_year, "
                    "ml.approval_year AS approval_year, ml.publication_year AS publication_year, "
                    "ml.description       "
                    "FROM minerals ml     "
                    "WHERE ml.id IN (     "
                    "   SELECT ml.id      "
                    "   FROM minerals ml  "
                    "   WHERE ml.name REGEXP '^[A-Za-z0-9]+' "
                    ");",
                    self.mindat_connection_str,
                )
                .fillna(value=np.nan)
                .sort_values("name")
                .reset_index(drop=True)
            )

            self.minerals = self.minerals.replace(0, np.nan)
            self.minerals["discovery_year"] = pd.to_numeric(
                self.minerals["discovery_year"], errors="coerce"
            )
            self.minerals.description = self.minerals.description.replace(r"", np.nan)

        except Exception as e:
            print(f"An error occurred when creating mineral_log: {e}")

    def update_mineral_log(self):
        assert self.mineral_log
        assert self.minerals

        outer_join = self.mineral_log.merge(
            self.minerals, how="outer", on="name", indicator=True
        )

        # Insert
        insert = outer_join[(outer_join._merge == "right_only")].drop("_merge", axis=1)
        insert.drop(insert.filter(regex="_x$").columns, axis=1, inplace=True)
        insert.rename(
            columns={"id_y": "mindat_id", "description_y": "description"}, inplace=True
        )
        insert = insert.drop_duplicates("name")
        insert = insert[["name", "description", "mindat_id"]]

        if len(insert) > 0:
            query_ = """
                INSERT INTO mineral_log (name, description, mindat_id) VALUES %s;
            """
            self.execute_query(insert, query_)

        # Update
        update = outer_join[(outer_join._merge == "both")].drop("_merge", axis=1)
        update.rename(
            columns={"id_x": "id", "mindat_id": "mindat_id_x", "id_y": "mindat_id_y"},
            inplace=True,
        )
        update = update.drop_duplicates("name")

        old_ = update[["id", "description_x", "mindat_id_x"]]
        new_ = update[["id", "description_y", "mindat_id_y"]]

        old_.rename(
            columns={"mindat_id_x": "mindat_id", "description_x": "description"},
            inplace=True,
        )
        new_.rename(
            columns={"mindat_id_y": "mindat_id", "description_y": "description"},
            inplace=True,
        )

        diff = old_.compare(new_, keep_shape=False).dropna(how="all", axis=1)

        if len(diff) > 0:
            update_ = update.loc[diff.index]
            update_.rename(
                columns={"description_y": "description", "mindat_id_y": "mindat_id"},
                inplace=True,
            )
            update_ = update_[["id", "description", "mindat_id"]]

            query_ = (
                "UPDATE mineral_log AS ml SET "
                "description = new.description, "
                "mindat_id = new.mindat_id "
                "FROM (VALUES %s) AS new (id, description, mindat_id) "
                "WHERE ml.id::uuid = new.id::uuid;"
            )
            self.execute_query(update_, query_)

    def execute_query(self, df, query):

        df = df.where(pd.notnull(df), None)
        tuples = [tuple(x) for x in df.to_numpy()]

        conn = self.pool.getconn()
        cursor = conn.cursor()
        # cursor.mogrify(query % tuples)

        try:

            execute_values(cursor, query, tuples)
            conn.commit()
            print("The db was updated with %s records" % len(tuples))

        except Exception as e:
            print("An error occurred: %s" % e)
            conn.rollback()
            return 1

        finally:
            cursor.close()
            self.pool.putconn(conn)


migrate = Migration()
migrate.connect_db()
migrate.get_minerals()
migrate.get_mineral_log()
outer_join = migrate.mineral_log.merge(
    migrate.minerals, how="outer", on="name", indicator=True
)
