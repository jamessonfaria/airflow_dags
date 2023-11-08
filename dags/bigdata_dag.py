from airflow import DAG
from datetime import datetime
from big_data_operator import BigDataOperator

dag = DAG('bigdata_dag', description="Dag BigData", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)

big_data_parquet_task = BigDataOperator(task_id="big_data_parquet_task",
                                path_to_csv_file="/opt/airflow/data/Churn.csv", 
                                path_to_save_file="/opt/airflow/data/Churn.parquet",
                                file_type="parquet", dag=dag)

big_data_json_task = BigDataOperator(task_id="big_data_json_task",
                                path_to_csv_file="/opt/airflow/data/Churn.csv", 
                                path_to_save_file="/opt/airflow/data/Churn.json",
                                file_type="parquet", dag=dag)

big_data_parquet_task
big_data_json_task