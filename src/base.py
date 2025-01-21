# -*- coding: UTF-8 -*-
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from psycopg2 import Error as psycopgError
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values, Json
from psycopg2.pool import ThreadedConnectionPool


register_adapter(np.int64, AsIs)
register_adapter(dict, Json)

load_dotenv(".envs/.local/.mindat")
load_dotenv(".envs/.prod/.cod")


class Migrator:
    def __init__(self, env="dev"):
        if env == "dev":
            load_dotenv(".envs/.local/.mr")
        elif env == "prod":
            load_dotenv(".envs/.prod/.mr")
        else:
            sys.exit("Wrong environment!")

        self.mindat_connection_params = (
            f"mysql+pymysql://"
            f"{os.getenv('MINDAT_MYSQL_USER')}:{os.getenv('MINDAT_MYSQL_PASSWORD')}@127.0.0.1/"
            f"{os.getenv('MINDAT_MYSQL_DATABASE')}"
        )
        self.cod_connection_params = (
            f"mysql+pymysql://"
            f"{os.getenv('COD_MYSQL_USER')}@{os.getenv('COD_MYSQL_HOST')}/"
            f"{os.getenv('COD_MYSQL_DATABASE')}"
        )
        self.mr_connection_params = {
            "dbname": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "host": os.getenv("POSTGRES_HOST"),
            "port": os.getenv("POSTGRES_PORT"),
        }
        self.pool = None

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


    def execute_query(self, df, query):

        # TODO: make this method generic
        if isinstance(df, pd.DataFrame):
            df = df.replace({np.nan: None})

        tuples = [tuple(x) for x in df.to_numpy()]

        _conn = self.pool.getconn()
        _conn = db.pool.getconn()
        _cursor = _conn.cursor()
        _cursor.mogrify(insert_chem_measurement % tuples[:10])

        try:
            retrieved = execute_values(_cursor, query, tuples, fetch=True)
        except psycopgError as e:
            print("An error occurred: %s" % e)
            _conn.rollback()
            return 1

        else:
            _conn.commit()
            print("The db was updated with %s records" % len(tuples))
            return retrieved

        finally:
            _cursor.close()
            self.pool.putconn(_conn)

    @staticmethod
    def save_report(data: pd.DataFrame, table_name: str, operation: str) -> None:

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame(data)

        date = datetime.today().strftime("%d.%m.%Y__%H-%M")
        filename = f"{operation}_{table_name}_{date}.csv"

        data.to_csv(f"db/reports/{filename}", index=False)


