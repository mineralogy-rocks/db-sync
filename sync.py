# -*- coding: UTF-8 -*-
from src.connectors import Migration

def main():

    migrate = Migration(env='dev')

    try:
        migrate.connect_db()

        migrate.get_minerals()
        # migrate.get_relations()
        # migrate.get_cod()
        # migrate.cod.loc[migrate.cod['id'].isin([9000333, 7048271]), 'reference'].values

        migrate.fetch_tables()

        # migrate.sync_rruff_cod()
        # migrate.sync_mineral_log()
        # migrate.sync_mineral_status()
        # migrate.sync_mineral_relation()
        # migrate.sync_mineral_history()
        # migrate.sync_mineral_ima_status()
        # migrate.sync_mineral_ima_note()
        migrate.sync_mineral_context()
        # migrate.sync_mineral_crystallography()
        # migrate.sync_mineral_formula()
        # migrate.sync_mineral_relation_suggestion()

    except Exception as e:
        print("An error occurred: %s" % e)

    finally:
        migrate.disconnect_db()


if __name__ == "__main__":
    main()
