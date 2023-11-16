import boto3
from botocore.exceptions import ClientError
import time
from datetime import datetime


class GlueJobRunner:
    ALL_JOB_STATUS = [
        "STARTING",
        "RUNNING",
        "STOPPING",
        "STOPPED",
        "SUCCEEDED",
        "FAILED",
        "TIMEOUT",
        "ERROR",
        "WAITING",
    ]
    FINISHED_JOB_STATUS = ["SUCCEEDED", "FAILED", "TIMEOUT", "ERROR"]
    FAILED_JOB_STATUS = ["FAILED", "TIMEOUT", "ERROR"]
    SUCCESS_JOB_STATUS = ["SUCCEEDED"]

    def __init__(self, job_name: str, job_timeout: int = 1200, check_interval: int = 5):
        self.job_name = job_name
        self.job_timeout = job_timeout
        self.check_interval = check_interval
        self.session = boto3.session.Session()
        self.glue_client = self.session.client("glue")
        self.start_time = None
        self.job_run_id = None
        self.job_running = False
        self.job_status = None

    def run_and_monitor_job(self, arguments={}):
        self.run_job(arguments)
        self.monitor_job()

    def run_job(self, arguments={}):
        try:
            self.start_time = datetime.now()
            job_run_response = self.glue_client.start_job_run(
                JobName=self.job_name, Arguments=arguments
            )
            print(job_run_response)
            self.job_run_id = job_run_response["JobRunId"]
            self.job_running = True
        except ClientError as e:
            raise Exception("boto3 client error in run_job: " + e.__str__())
        except Exception as e:
            raise Exception("Unexpected error in run_job: " + e.__str__())

    def monitor_job(self):
        while self.job_running:
            self.check_job_timeout(self.check_interval)
            job_run = self.glue_client.get_job_run(
                JobName=self.job_name, RunId=self.job_run_id, PredecessorsIncluded=False
            )
            self.job_status = job_run["JobRun"]["JobRunState"]
            self.job_running = self.check_job_running()

    def check_job_running(self):
        print(f"Job status = {self.job_status}")
        if self.job_status in GlueJobRunner.FAILED_JOB_STATUS:
            raise Exception(f"Job Faile with the status {self.job_status}")
        if self.job_status in GlueJobRunner.SUCCESS_JOB_STATUS:
            print("Job Succeded")
            return False
        if self.job_status not in GlueJobRunner.ALL_JOB_STATUS:
            print("Job Should be started before checked!")
            return False
        return True

    def check_job_timeout(self, wait_time=0):
        time.sleep(wait_time)
        time_delta = datetime.now() - self.start_time
        print(f"Elapsed time = {time_delta.total_seconds():.2f}s")
        if time_delta.total_seconds() >= self.job_timeout:
            raise Exception("Job Timed Out!")


if __name__ == "__main__":
    GlueJobRunner("teste-log-python-shell").run_and_monitor_job()
