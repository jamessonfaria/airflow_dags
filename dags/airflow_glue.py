from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

glue_job_name = "prudential-python-engine-example"
glue_iam_role = "AmazonMWAA-MyAirflowEnvironment-BgcOGL"
region_name = "us-east-1"

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG(dag_id = 'airflow_glue', default_args = default_args, schedule_interval = None, 
         catchup=False) as dag:


    glue_job_step = GlueJobOperator(
        task_id = 'airflow_glue_task',
        job_name =glue_job_name,
        script_location = 's3://aws-glue-assets-906346695041-us-east-1/scripts/prudential-python-engine-example.py',
        region_name = region_name,
        iam_role_name = glue_iam_role,
        script_args={
            "--enable-job-insights" : "false",  
            "--job-language" : "python",
            "--TempDir" : "s3://aws-glue-assets-906346695041-us-east-1/temporary/",
            "--enable-glue-datacatalog" : "true",
            "--spark-event-logs-path": "s3://aws-glue-assets-906346695041-us-east-1/sparkHistoryLogs/",
            "--continuous-log-logGroup": "/aws-glue/python-jobs",
            "library-set": "analytics"
        },
        # num_of_dpus=1,
        s3_bucket = "aws-glue-assets",
        aws_conn_id="aws_s3_connection",
        create_job_kwargs={'GlueVersion': '3.0', 'Command': {"Name": "pythonshell"}, "NumberOfWorkers": 1, 'WorkerType': 'G.2X'},
        verbose=True,
        dag = dag
        )







#arn:aws:iam::906346695041:policy/service-role/MWAA-Execution-Policy-ea0e6643-ba45-4581-a719-d9490e127ad1
#MWAA-Execution-Policy-ea0e6643-ba45-4581-a719-d9490e127ad1


# job_name (str) – unique job name per AWS Account

# script_location (str | None) – location of ETL script. Must be a local or S3 path

# job_desc (str) – job description details

# concurrent_run_limit (int | None) – The maximum number of concurrent runs allowed for a job

# script_args (dict | None) – etl script arguments and AWS Glue arguments (templated)

# retry_limit (int) – The maximum number of times to retry this job if it fails

# num_of_dpus (int | float | None) – Number of AWS Glue DPUs to allocate to this Job.

# region_name (str | None) – aws region name (example: us-east-1)

# s3_bucket (str | None) – S3 bucket where logs and local etl script will be uploaded

# iam_role_name (str | None) – AWS IAM Role for Glue Job Execution. If set iam_role_arn must equal None.

# iam_role_arn (str | None) – AWS IAM ARN for Glue Job Execution. If set iam_role_name must equal None.

# create_job_kwargs (dict | None) – Extra arguments for Glue Job Creation

# run_job_kwargs (dict | None) – Extra arguments for Glue Job Run

# wait_for_completion (bool) – Whether to wait for job run completion. (default: True)

# deferrable (bool) – If True, the operator will wait asynchronously for the job to complete. This implies waiting for completion. This mode requires aiobotocore module to be installed. (default: False)

# verbose (bool) – If True, Glue Job Run logs show in the Airflow Task Logs. (default: False)

# update_config (bool) – If True, Operator will update job configuration. (default: False)

# stop_job_run_on_kill (bool) 