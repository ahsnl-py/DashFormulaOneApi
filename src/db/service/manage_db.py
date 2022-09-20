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
        self.conn = psycopg2.connect(**self.params)
        self.dbConfig = {
            'host': '',
            'database': '',
            'port': '',
            'user': '',
            'password': ''
        }
        self.fileTempPath = '{0}\{1}'.format(os.path.dirname(os.path.realpath(__file__)), 'tmp')  
        self.timestr = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
        self.fileTempName = 'backup-{}-{}.dump'.format(self.timestr, self.dbConfig['database'])

        def init_config():
            for k, val in self.params.items():
                if k in self.dbConfig:
                    self.dbConfig[k] = val

        init_config()

    def list_dash_db(self):
        """
            Backup postgres db to a file.
        """
        try:
            process = subprocess.Popen(
                ['psql',
                '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                    self.dbConfig['user'], 
                    self.dbConfig['password'], 
                    self.dbConfig['host'],
                    self.dbConfig['port'],
                    self.dbConfig['database']
                ),
                '--list'],
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
        