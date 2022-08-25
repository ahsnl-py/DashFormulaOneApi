from datetime import datetime, timezone
from urllib import request
from etl.etl_jobs import Box

job = Box('etlconfig.ini')
job.get_gp_schedule('df1_uf_gp_schedule')