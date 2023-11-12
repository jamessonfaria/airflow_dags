from airflow import DAG
from position_fields_operator import PositionFieldsOperator
from datetime import datetime

dag = DAG('dag_poc_test1000', description="DAG POC Test 1000", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)


# process_1000_rows_task = PositionFieldsOperator(task_id="process_1000_rows_task", 
#                                                 path_input_file='/opt/airflow/data/sample_1000 1.txt',
#                                                 path_output_file='/opt/airflow/data/sample_1000_1.json',
#                                                 position_fields = [(0, 6), (6, 17), (17, 23), (23, 30), (30, 38)],
#                                                 dag=dag)

# process_1000_rows_task
