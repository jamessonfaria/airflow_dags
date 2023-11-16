from airflow import DAG
from datetime import datetime
from glue_job_runner_operator import GlueJobRunnerOperator

default_args = {
    'start_date': datetime(2020, 1, 1)
}

with DAG(dag_id = 'airflow_glue_100000', description="Gluer 100.000 rows", 
         default_args = default_args, schedule_interval = None, catchup=False) as dag:
    
    glue_job_step = GlueJobRunnerOperator(
        task_id="glue_job_step",
        job_name="teste-position-file-100000", 
        dag = dag)

