from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import json

dag = DAG('dag_poc_test1', description="DAG POC Test1", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)

def get_fields(lin):
    fields = []
    start = 0
    for i in range(1, len(lin)):
        end = start + i + 1
        field = lin[start:end].strip()
        if field:
            fields.append(field)
        start = end
    return fields

def transform_data():
    df = pd.read_csv('/opt/airflow/data/sample_100 1.txt', header=None)

    df['fields'] = df[0].apply(get_fields)
    df['row'] = df.index + 1
    df = df[['row', 'fields']]

    data_dict = df.to_dict(orient='records')

    with open('/opt/airflow/data/sample_100_1.json', 'w') as json_file:
        json.dump(data_dict, json_file, indent=4)

process_100_rows_task = PythonOperator(task_id="process_100_rows_task", python_callable=transform_data, dag=dag)

process_100_rows_task
