from etl.Upstream.load_gp_schedule import LoadGPSchedule
from config.config import set_config


class Box:

    def __init__(self, job_config_file_name:str):
        self.jobConfigName = job_config_file_name

    def get_gp_schedule(self, job_config_section):
        job_params = set_config(
            config_file_name=self.jobConfigName,
            config_sec_name=job_config_section
        )
        return LoadGPSchedule(
                config=job_params, 
                job_name=job_config_section
            ).run_job()