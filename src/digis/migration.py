import os
import sys
import time

import polars as pl
from dotenv import load_dotenv

from src.constants import TECTONIC_SETTING_CHOICES, ALTERATION_CHOICES, PRIMARY_SECONDARY_CHOICES
from src.queries import get_chem_measurement, insert_chem_measurement
from src.base import Migrator


PATH = 'data/generated/'
FORMAT = 'csv'
PATTERN = 'georoc_'


def _get_most_recent_version():
    files = [f for f in os.listdir(PATH) if PATTERN in f]
    files.sort(key=lambda x: os.path.getmtime(PATH + x))
    file = files[-1]
    return file


def _insert_chem_measurement(data, db):
    _insert = data.select([
        'external_key', 'mineral', 'mineral_note', 'sample_name', 'grain_size', 'rock_name', 'rock_texture',
        'alteration', 'is_primary', 'tectonic_setting', 'citation', 'latitude_min', 'latitude_max', 'longitude_min',
        'longitude_max', 'elevation_min', 'elevation_max', 'location', 'location_note'
    ])
    db.execute_query(_insert, insert_chem_measurement)



def main():
    load_dotenv(".envs/.local/.mr")

    db = Migrator()
    db.connect_db()

    file = _get_most_recent_version()
    data = pl.read_csv(PATH + file, ignore_errors=True, encoding='utf8-lossy', null_values=['', ' '])


    uri = f'postgresql://{os.getenv("POSTGRES_USER")}:{os.getenv("POSTGRES_PASSWORD")}@{os.getenv("POSTGRES_HOST")}:{os.getenv("POSTGRES_PORT")}/{os.getenv("POSTGRES_DB")}'
    _old = pl.read_database_uri(get_chem_measurement, uri=uri)
    _new = data.filter(
        ~pl.col('external_key').is_in(_old['key'])
    )

    if len(_new):
        _insert_chem_measurement(_new, db)

    db.disconnect_db()



if __name__ == "__main__":
    main()
