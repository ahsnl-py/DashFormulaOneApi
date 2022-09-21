import datetime
import subprocess
import os
import configparser

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from config.config import set_config


class DashF1DatabaseManager():

    def __init__(self, db_config_file:str, db_config_sec:str):
        self.params = set_config(config_file_name = db_config_file, config_sec_name = db_config_sec)
        self.dbConfig = {
            'host': '',
            'database': '',
            'port': '',
            'user': '',
            'password': ''
        }
        self.fileTempPath = '{0}\{1}'.format(os.path.dirname(os.path.realpath(__file__)), 'tmp')  
        self.timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        self.fileTempName = ''

        def init_config():
            for k, val in self.params.items():
                if k in self.dbConfig:
                    self.dbConfig[k] = val

            self.fileTempName = 'backup-{}-{}.dump'.format(self.timestr, self.dbConfig['database'])

        init_config()

    def list_dash_db(self):
        """
            Backup postgres db to a file.
        """
        try:
            process = subprocess.Popen(
                [
                    'psql', 
                    '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                        self.dbConfig['user'], 
                        self.dbConfig['password'], 
                        self.dbConfig['host'],
                        self.dbConfig['port'],
                        self.dbConfig['database']
                    ), 
                    '--list'
                ],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output
        except Exception as e:
            print(e)
            exit(1)


    def backup_dash_db(self, is_schema:bool=False):
        """
            Backup database to a dump file
        """
        dest_file = "{0}\{1}".format(self.fileTempPath, self.fileTempName)
        try:
            dump_cmd = [
                'pg_dump', 
                '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                    self.dbConfig['user'], 
                    self.dbConfig['password'], 
                    self.dbConfig['host'],
                    self.dbConfig['port'],
                    self.dbConfig['database']
                ), 
                '-Fc', 
                '-f', dest_file, 
                '-v'
            ]
            if is_schema: dump_cmd.append('-s')
            process = subprocess.Popen(dump_cmd, stdout=subprocess.PIPE)
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))
                exit(1)
            return output, dest_file
        except Exception as e:
            print(e)
            exit(1)
    
    def restore_dash_db(self, file_name:str):
        backup_file = "{0}\{1}.dump".format(self.fileTempPath, file_name)
        try:
            process = subprocess.Popen(
                [
                    'pg_restore', 
                    '--no-owner', 
                    '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                        self.dbConfig['user'], 
                        self.dbConfig['password'], 
                        self.dbConfig['host'],
                        self.dbConfig['port'],
                        self.dbConfig['database']
                    ), 
                    '-v', backup_file
                ],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))

            return output
        except Exception as e:
            print("Issue with the db restore : {}".format(e))

    def restore_simple_dash_db(self, file_name:str):
        backup_file = "{0}\{1}.dump".format(self.fileTempPath, file_name)
        """
        Restore postgres db from a file in simple mode.
        """    
        try:
            process = subprocess.Popen(
                ['pg_restore',
                    '-c',
                    '--no-owner',
                    '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                            self.dbConfig['user'], 
                            self.dbConfig['password'], 
                            self.dbConfig['host'],
                            self.dbConfig['port'],
                            self.dbConfig['database']
                        ),
                    '-v',
                    backup_file],
                stdout=subprocess.PIPE
            )
            output = process.communicate()[0]
            if int(process.returncode) != 0:
                print('Command failed. Return code : {}'.format(process.returncode))

            return output
        except Exception as e:
            print("Issue with the db restore : {}".format(e))

    def create_dash_db(self, database_name:str):
        self.dbConfig['database'] = 'postgres'
        try:
            con = psycopg2.connect(**self.dbConfig)
        except Exception as e:
            print(e)
            exit(1)
        
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = con.cursor()
        try:
            cur.execute("DROP DATABASE {} ;".format(database_name))
        except Exception as e:
            print('DB does not exist, nothing to drop')
        cur.execute("CREATE DATABASE {} ;".format(database_name))
        cur.execute("GRANT ALL PRIVILEGES ON DATABASE {} TO {} ;".format(database_name, self.dbConfig['user']))
        return database_name
    
    def swap_restore_active(self, restore_db:str, active_db:str):
        self.dbConfig['database'] = 'postgres'
        try:
            con = psycopg2.connect(**self.dbConfig)
            con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = con.cursor()
            cur.execute("SELECT pg_terminate_backend( pid ) "
                        "FROM pg_stat_activity "
                        "WHERE pid <> pg_backend_pid( ) "
                        "AND datname = '{}'".format(active_db))
            cur.execute("DROP DATABASE {}".format(active_db))
            cur.execute('ALTER DATABASE "{}" RENAME TO "{}";'.format(restore_db, active_db))
        except Exception as e:
            print(e)
            exit(1)

    def insert_many(self, sql:str, values):
        """
            INSERT INTO TABLE obj with row more than one
        """
        try:
            conn = psycopg2.connect(**self.params)
            # create a new cursor
            cur = conn.cursor()
            cur.executemany(sql,values)
            conn.commit()
        except Exception as e:
            print(e)
            exit(1)