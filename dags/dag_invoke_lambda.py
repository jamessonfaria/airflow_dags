from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.sensors.lambda_function import LambdaFunctionStateSensor

from datetime import datetime


dag = DAG('dag_invoke_lambda', description="DAG Invoke Lambda", 
            schedule_interval=None, start_date=datetime(2023,11,1),
            catchup=False)

wait_for_lambda = LambdaFunctionStateSensor(
    task_id='wait__invoke_lambda_function',
    function_name="prudential-call-dag-airflow",
    aws_conn_id='aws_s3_connection',
    dag=dag
)

invoke_lambda = AwsLambdaInvokeFunctionOperator(
    task_id='invoke_lambda_function',
    function_name='prudential-call-dag-airflow',
    aws_conn_id='aws_s3_connection',
    payload='{}',
    dag=dag
)

wait_for_lambda >> invoke_lambda