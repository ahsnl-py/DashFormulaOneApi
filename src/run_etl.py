from datetime import datetime, timezone
from urllib import request
from etl.Upstream.load_gp_schedule import LoadGPSchedule
from etl.System.db_conn import ConnectorDB


db = ConnectorDB('dbconfig.ini')
config = {
    "job_name": "df1_ul_gp_schedule",
    "job_code": "gp-schedule",
    "job_type": ["extract", "transform"],
    "job_status": [99, 99],
    "job_rdate": "2021-04-04"
}

load_gp_schedule = LoadGPSchedule(config)
if load_gp_schedule.run_job():
    print("no issue with job")
