from etl.Upstream.load_gp_schedule import LoadGPSchedule
from etl.Upstream.load_race_results import LoadRaceResults
from config.config import set_config


class Box:

    def __init__(self, job_config_file_name:str):
        self.jobConfigName = job_config_file_name

    def set_job_config(self, config_section:str):
        return set_config(
            config_file_name=self.jobConfigName,
            config_sec_name=config_section
        )

    def get_gp_schedule(self, job_config_section):
        return LoadGPSchedule(
                config=self.set_job_config(job_config_section), 
                job_name=job_config_section
            ).run_job()

    def get_gp_race_results(self, job_config_section):
        return LoadRaceResults(
                config=self.set_job_config(job_config_section), 
                job_name=job_config_section
            ).run_job()