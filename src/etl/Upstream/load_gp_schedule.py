import datetime
from pdb import run
from select import select
from time import strftime
import sys
import pandas as pd
from etl.System.fastf1_conn import ConnectorFastF1 as cff1
from etl.System.db_conn import ConnectorDB as cdb
from etl.System.handle_config import HandleJobConfig

    
class LoadGPSchedule(HandleJobConfig):

    def __init__(self, config:dict, job_name:str):
        HandleJobConfig.__init__(self)
        HandleJobConfig.__call__(self, **config)
        self.jobName = job_name
        self.db = cdb('dbconfig.ini', 'postgresql')
     
    def run_job(self):
        init_status_id = 99
        self.jobConfig['job_name'] = self.jobName
        req_id = self.db.create_request(
            request_code=self.jobConfig['job_code'],
            race_date=self.jobConfig['job_rdate'],
            request_status_id=init_status_id,
            request_start_date=datetime.datetime.now(),
            request_end_date="null"
        )
        
        req_id, status_id = req_id[0], True if req_id[1] != 1 else False
        if status_id: 
            msg = ""
            self.jobConfig['job_id'] = req_id         
            self.jobConfig['job_status'] = init_status_id
            # iterate through job type
            for id, job in enumerate(self.jobConfig["job_type"]):
                status = "fail"
                if job == "extract": 
                    if not self.check_job_status(job):
                        request, obj = self.extractData()
                        if request:
                            self.jobState[id]['isComplete'] = True 
                            status = "success" 
                    self.check_job_status(job, status, obj[0])
                
                elif job == "transform":  
                    if not self.check_job_status(job):                    
                        request = self.transformData()
                        if request:
                            self.jobState[id]['isComplete'] = True 
                            status = "success" 
                    self.check_job_status(job, status)

                else: 
                    self.loadData()
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
        file_name = f"gp_schedule_{self.raceYear}.csv"
        request, response = cff1(self.raceYear).get_race_schedule()
        if response:
            request = request.loc[:, self.gp_schedule_schema()]
            request['request_id'] = self.jobConfig['job_id']
            self.isJobComplete = self.db.load_to_csv(
                df=request, 
                file_name=file_name
            )
            
        return True if self.isJobComplete else False, [file_name]

    def transformData(self):
        # delete file after loading
        return self.db.load_from_csv(f"gp_schedule_{self.raceYear}.csv", 'fact_race_gp_schedule')

    def loadData(self):
        self.jobConfig["job_type"][0]
        pass

    def gp_schedule_schema(self):
        return [    
            'RoundNumber'
            , 'Country'
            , 'Location'
            , 'OfficialEventName'
            , 'EventDate'
            , 'EventName'
            , 'EventFormat'
            , 'Session1'
            , 'Session1Date'
            , 'Session2'
            , 'Session2Date'
            , 'Session3'
            , 'Session3Date'
            , 'Session4'
            , 'Session4Date'
            , 'Session5'
            , 'Session5Date'
        ]