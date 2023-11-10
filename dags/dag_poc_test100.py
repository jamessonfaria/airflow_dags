from airflow import DAG
from position_fields_operator import PositionFieldsOperator
from datetime import datetime

dag = DAG('dag_poc_test100', description="DAG POC Test 100", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)


process_100_rows_task = PositionFieldsOperator(task_id="process_100_rows_task", 
                                                path_input_file='/opt/airflow/data/sample_100 1.txt',
                                                path_output_file='/opt/airflow/data/sample_100_1.json',
                                                position_fields = [(0, 2), (2, 5), (5, 9), (9, 14), (14, 20)],
                                                dag=dag)

process_100_rows_task
