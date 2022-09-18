import datetime
from time import strftime
import pandas as pd
from etl.System.fastf1_conn import ConnectorFastF1 as cff1
from etl.System.db_conn import ConnectorDB as cdb
from etl.System.handle_config import HandleJobConfig

class LoadRaceResults(HandleJobConfig):
    
    def __init__(self, config:dict, job_name:str) -> None:
        HandleJobConfig.__init__(self)
        HandleJobConfig.__call__(self, **config)
        self.jobName = job_name
        self.db = cdb('dbconfig.ini', 'postgresql')

    def run_job(self):
        # create request 
        init_status_id = 99
        self.jobConfig['job_name'] = self.jobName
        req_id = self.db.create_request(
            request_code=self.jobConfig['job_code'],
            race_date=self.jobConfig['job_rdate'],
            request_status_id=init_status_id,
            request_start_date=datetime.datetime.now(),
            request_end_date="null"
        )

        req_id, is_load = req_id[0], False if req_id[1] != 1 else True
        print(req_id, is_load)
        if not is_load:
            msg = ""
            self.jobConfig['job_id'] = req_id         
            self.jobConfig['job_status'] = init_status_id
            # iterate through job type
            for id, job in enumerate(self.jobConfig["job_type"]):
                status = ""
                if job == "extract": 
                    if not self.check_job_status(job):
                        request = self.extractData()
                        if request:
                            self.jobState[id]['isComplete'] = True 
                            status = "success" 
                        else:
                            status = "fail"
                            msg = "Issue detect while extracting request data"
                    self.check_job_status(job, status)
             
        else:
            # if data already load
            self.jobConfig['job_status'] = 6
            msg = "Data already loaded for job {} on {}".format(
                self.jobConfig['job_name'],
                self.jobConfig['job_rdate']
            )
        
        if self.jobConfig['job_status'] == 1:
            # do auditing? 
            pass
        elif self.jobConfig['job_status'] == 2:
            print(msg)
            return  
        elif self.jobConfig['job_status'] == 6:
            print(msg)
            return 
        else:
            print("one of the step fails. Re-load job")

        # Close request once job complete based on each item from job config obj
        self.db.close_request(
            request_id=self.jobConfig['job_id'],
            request_status_id=self.jobConfig['job_status'],
            request_end_date=datetime.datetime.now(),
            request_type_code=self.jobConfig['job_code']
        )
        return 
        

    def extractData(self):
        # get get_race_info_by_date
        df_race_info = self.db.get_race_info_by_date(self.jobConfig['job_rdate'])
        res_list = []
        for i in range(len(df_race_info)):
            # append tuple to a list
            res_list.append((
                    self.jobConfig['job_id'],
                    self.jobConfig['job_rdate'],
                    df_race_info.loc[i]['race_type'],
                    cff1().get_race_results(
                        gp=df_race_info.loc[i]['race_name'],
                        race_year=self.raceYear,
                        race_type=df_race_info.loc[i]['race_type'],
                        schema=self.get_race_results_schema()
                    )
                )
            )
        
        # dump to db
        # rid, race_date, race_type, data
        # --------------------------------------------
        # 1, 2022-08-29, Practice 1, {'columns':list, 'data':json_data}
        status = self.db.dump_race_results(res_list)
        return True if status else False 
    
    def transformData(self):
        
        pass

    def get_race_results_schema(self) -> list:
        return [
            'BroadcastName'
            , 'FullName'
            , 'TeamName'
            , 'Position'
            , 'GridPosition'
            , 'Q1'
            , 'Q2'
            , 'Q3'
            , 'Time'
            , 'TeamColor'
            , 'Points'
        ]
