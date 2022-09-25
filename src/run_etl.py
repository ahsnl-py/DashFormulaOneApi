from datetime import datetime, timezone
from urllib import request
from etl.etl_jobs import Box
from etl.System.db_conn import ConnectorDB

import datetime
from difflib import restore
import logging
import argparse


def main():
    """
        Entry point to execute job based on given input args from cmd
    """
    
    args_parser = argparse.ArgumentParser(description='DashF1 ETL management system')
    args_parser.add_argument("--env",
                            choices=['dev', 'uat', 'prod'],
                            required=True,
                            default="dev",
                            help="Choose database environment type")
    args_parser.add_argument("--jobconfig",
                             required=True,
                             help="Job configuration file")
    args_parser.add_argument("--jobname",
                             required=True,
                             help="Job name to execute")
    args_parser.add_argument("--rdate",
                             metavar="YYYY-MM-dd",
                             help="Race date event")
    args_parser.add_argument("--verbose",
                             default=True,
                             help="verbose output")
    args = args_parser.parse_args()

    db_config_file = ''
    if args.env in ('dev', 'uat'):
        db_config_file = 'dbconfig.ini'
    elif args.env == 'prod':
        db_config_file = 'prod_dbconfig.ini'

    db = ConnectorDB(db_config_file, "{}_dashpg".format(args.env))
    etl = Box(
        args.jobconfig, 
        args.jobname,
        args.env,
        db_config_file
    )

    if not args.rdate:
        print("race date is not given.")
    else:
        replace_param = etl.jobParams
        if args.jobname == 'df1_uf_gp_schedule':
            race_date = args.rdate.replace('-', '')
            replace_param['job_rdate'] = race_date
            etl.run_job()

        elif args.jobname == 'df1_ufl_race_results_yearly':
            race_dates = db.get_list_race_date_by_year(args.rdate[:4])
            for i in range(len(race_dates[0][:4])): # <-- change back to normal enum
                replace_param['job_rdate'] = race_dates[0][i]
                etl.run_job()

        elif args.jobname == 'df1_ufl_race_results':
            race_date = args.rdate.replace('-', '')
            replace_param['job_rdate'] = race_date
            etl.run_job()


if __name__ == '__main__':
    main()