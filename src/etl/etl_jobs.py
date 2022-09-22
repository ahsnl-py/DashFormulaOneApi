from select import select
from etl.Upstream.load_gp_schedule import LoadGPSchedule
from etl.Upstream.load_race_results import LoadRaceResults
from config.config import set_config


class Box:

    def __init__(self, job_config_file_name:str, job_name:str, env_type:str):
        self.jobConfigName = job_config_file_name
        self.jobName = job_name
        self.jobEnvType = env_type
        self.jobParams = set_config(
            config_file_name=self.jobConfigName,
            config_sec_name=self.jobName
        )

    def set_job_config(self):
        return 

    def run_job(self):
        if self.jobName == 'df1_uf_gp_schedule':
            return LoadGPSchedule(
                job_config=self.jobParams, 
                job_name=self.jobName,
                env_config='dbconfig',
                env_type=self.jobEnvType
            ).exec_job()

        elif self.jobName in ('df1_ufl_race_results', 'df1_ufl_race_results_yearly'):
            return LoadRaceResults(
                config=self.jobParams, 
                job_name=self.jobName,
                env_config='dbconfig',
                env_type=self.jobEnvType
            ).exec_job()
