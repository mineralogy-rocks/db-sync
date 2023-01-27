# -*- coding: UTF-8 -*-
from src.connectors import Migration


def main():

    migrate = Migration()

    try:
        migrate.connect_db()

        migrate.get_minerals()
        migrate.get_relations()
        migrate.fetch_tables()

        migrate.sync_mineral_log()
        migrate.sync_mineral_history()
        migrate.sync_mineral_crystallography()
        migrate.sync_mineral_formula()
        migrate.sync_mineral_relation_suggestion()

    except Exception as e:
        print("An error occurred: %s" % e)

    finally:
        migrate.disconnect_db()


if __name__ == "__main__":
    main()
