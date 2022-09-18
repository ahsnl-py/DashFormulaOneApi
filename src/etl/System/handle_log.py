from etl.System.handle_config import HandleJobConfig
import datetime

class HandleJobLog(HandleJobConfig):
    def __init__(self) -> None:
        HandleJobConfig.__init__(self)
        self.jobTime = datetime.datetime.now()
        # use locally to assign step from this object
        self.jobState = [
            {
                'type': "extract",
                'isComplete': False,
                'status': ""
            },
            {
                'type': "transform",
                'isComplete': False,
                'status': ""
            },
            {
                'type': "load",
                'isComplete': False,
                'status': ""
            }
        ]

    def check_job_status(self, job_type, status="", destinations=""):
        get_job_state = lambda key, inputs: [opt_type[key] for opt_type in inputs]
        get_job_type = get_job_state('type', self.jobState)
        job_complete = get_job_state('isComplete', self.jobState)
        job_idx = get_job_type.index(job_type)

        # check if job complete
        if not job_complete[job_idx]:
            self.log_status(job_idx)
            return self.jobState[job_idx]['isComplete']
        
        # if complete log status
        if status == "fail":
            self.jobState[job_idx]['status'] = 'Failed'
        elif status == "success":
            self.jobState[job_idx]['status'] = 'Success'
        
        self.log_config_details()
        self.log_status_details(self.jobState[job_idx]['status'], job_type, destinations)
        self.log_status(job_idx)  

    def log_status(self, idx:int):
        jobState = "End" if self.jobState[idx]['isComplete'] else "Start"

        # when package is ending, log the status id of each job type (extract, transform, load)
        if jobState == "End":
            # get job_type from jobState class attr 
            # job_type = self.jobState[idx]['type']
            # find the index of jobConfig current status
            # job_config_idx = self.jobConfig['job_type'].index(job_type)
            # get status id based on current job state 
            job_status_id = self.get_job_status(self.jobState[idx]['status'])
            # set job status to jobConfig class attr
            self.jobConfig['job_status'] = job_status_id

        jobInfo = f"| {self.jobTime} | {jobState} {self.jobConfig['job_type'][idx]} job {self.jobConfig['job_name']} ({self.jobConfig['job_code']}) on {self.jobTime.strftime('%x')} |"
        item_to_log = [
            len(jobInfo)*"-",
            jobInfo,
            len(jobInfo)*"-"
        ]
        return print(*item_to_log, sep='\n')

    def log_status_details(self, status, job_type, destinations=""):
        to_file_name = f'to {destinations}' if len(destinations) > 0 else ''
        print(f"Status: {status}")
        print(f"Detail: {job_type} data for {self.jobConfig['job_name']} {to_file_name}")

    def log_config_details(self):
        for k, v in self.jobConfig.items():
            if k != 'job_type' and k != 'job_status':
                print("{}: {}".format(k,v))

    def get_job_status(self, status_type:int) -> str:
        status = ["Success", "Failed", "Purge", "Terminated", "Waiting", "Loaded"]
        return status.index(status_type)+1 