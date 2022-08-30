# -*- coding: UTF-8 -*-
from src.connectors import Migration


def main():

    migrate = Migration()

    try:
        migrate.connect_db()

        migrate.get_minerals()
        migrate.fetch_tables()

        migrate.sync_mineral_log()
        migrate.sync_mineral_history()
        migrate.sync_mineral_formula()

    except Exception as e:
        print("An error occurred: %s" % e)

    finally:
        migrate.disconnect_db()


if __name__ == "__main__":
    main()
