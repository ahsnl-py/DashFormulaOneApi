import datetime
import logging
import argparse

from db.service.manage_db import DashF1DatabaseManager
from db.service.utils_db import DashF1DatbaseUtils

def main():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    args_parser = argparse.ArgumentParser(description='Postgres database management')
    args_parser.add_argument("--action",
                             metavar="action",
                             choices=['list', 'list_dbs', 'restore', 'backup', 'restore-s', 'static'],
                             required=True)
    args_parser.add_argument("--date",
                             metavar="YYYY-MM-dd",
                             help="Date to use for restore (show with --action list)")
    args_parser.add_argument("--dest-db",
                             metavar="dest_db",
                             default=None,
                             help="Name of the new restored database")
    args_parser.add_argument("--verbose",
                             default=True,
                             help="verbose output")
    args_parser.add_argument("--configfile",
                             required=True,
                             help="Database configuration file")
    args_parser.add_argument("--filename",
                             required=False,
                             help="Database file name for processing")
    args_parser.add_argument("--schema",
                            required=False,
                            default=False,
                            help="Database backup with schema only")
    
    args = args_parser.parse_args()
    dbmanager = DashF1DatabaseManager(args.configfile, 'dev_dashpg')
    dbutils = DashF1DatbaseUtils()

    if args.action == "list_dbs":
        # list of db instance 
        result = dbmanager.list_dash_db()
        for line in result.splitlines():
            logger.info(line)

    elif args.action == 'backup':
        # backup db
        logger.info('Backing up {} database to {}'.format(
            dbmanager.dbConfig['database'], dbmanager.fileTempName
        ))
        result, backup_file = dbmanager.backup_dash_db()
        for line in result.splitlines():
            logger.info(line)
        logger.info("Backup complete")

        # compress db backup file
        logger.info("Compressing {}".format(backup_file))
        dbutils.compress_file(backup_file)
        logger.info("Backup compression complete")

if __name__ == '__main__':
    main()