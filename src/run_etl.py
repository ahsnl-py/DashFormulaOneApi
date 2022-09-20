from datetime import datetime, timezone
from urllib import request
from etl.etl_jobs import Box
from etl.System.db_conn import ConnectorDB



job = Box('etlconfig.ini')
job.get_gp_schedule('df1_uf_gp_schedule')
# job.get_gp_race_results('df1_ufl_race_results')


