from airflow import DAG
from position_fields_operator import PositionFieldsOperator
from datetime import datetime

dag = DAG('dag_poc_test10000', description="DAG POC Test 10000", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)


# process_10000_rows_task = PositionFieldsOperator(task_id="process_10000_rows_task", 
#                                                 path_input_file='/opt/airflow/data/sample_10000.txt',
#                                                 path_output_file='/opt/airflow/data/sample_10000.json',
#                                                 position_fields = [(0, 1), (4, 6), (8, 11), (12, 16), (20, 25), (28, 34), (36, 43), (44, 52)],
#                                                 dag=dag)

# process_10000_rows_task
