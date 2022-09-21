import datetime
from difflib import restore
import logging
import argparse

from db.service.manage_db import DashF1DatabaseManager
from db.service.utils_db import DashF1DatbaseUtils
import db.static.dash_db_request_type_code as request_code

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
    args_parser.add_argument("--env",
                            choices=['dev', 'uat', 'prod'],
                            required=True,
                            default="dev",
                            help="Choose database environment type")
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
    dbmanager = DashF1DatabaseManager(args.configfile, "{}_dashpg".format(args.env))
    dbutils = DashF1DatbaseUtils()

    if args.action == "list_dbs":
        """
            List all database instances
        """
        # list of db instance 
        result = dbmanager.list_dash_db()
        for line in result.splitlines():
            logger.info(line)

    elif args.action == 'backup':
        """
            backup database from target name
        """
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

    elif args.action == 'restore':
        """
            Restore database in default mode - with intermediate database as buffer before swap to target database
        """
        if not args.filename:
            logger.warning('No filename was given.')
        else:
            # create temp db before restore dump file
            logger.info('Creating temp database for restore : {}'.format(dbmanager.dbConfig['database']))
            temp_db_name = '{}_restore'.format(dbmanager.dbConfig['database'])
            dbmanager.create_dash_db(temp_db_name) # create temp_db from dbname postgres
            logger.info("Created temp database for restore : {}".format(temp_db_name))
            
            # restore from dump file to new created temp db
            logger.info("Restore starting")
            dbmanager.dbConfig['database'] = temp_db_name # change database name to temp_db name
            result = dbmanager.restore_dash_db(args.filename)
            for line in result.splitlines():
                logger.info(line)
            logger.info("Restore complete")
           
            # drop and rename to orginal db by 
            restored_db_name = temp_db_name.replace('_restore', '')
            logger.info("Switching restored database with active one : {} > {}".format(
                    temp_db_name, restored_db_name
                ))
            dbmanager.swap_restore_active(temp_db_name, restored_db_name)
            logger.info("Database restored and active.")
    
    elif args.action == 'restore-s':
        """
            Restore database in simple mode - clean same db before restore
        """
        if not args.filename:
            logger.warning('No filename was given.')
        else:
            logger.info("Start simple restore : {}".format(dbmanager.dbConfig['database']))
            dbmanager.restore_simple_dash_db(args.filename)
            logger.info("Complete restore database.")

    elif args.action == 'static':
        """
            Populate static values after restoring database
        """
        logger.info("Start inserting static values...")
        query, vals = request_code.static_request_type_code()
        dbmanager.insert_many(query, vals)
        logger.info("Complete")

if __name__ == '__main__':
    main()