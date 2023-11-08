from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG('banco_de_dados', description="Banco de Dados", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)

def print_result(ti):
    task_instance = ti.xcom_pull(task_ids='query_data_task')
    print('Resultado da consulta:')
    for row in task_instance:
        print(row)

create_table_task = PostgresOperator(task_id='postgres_task', postgres_conn_id='Postgres', 
                       sql='create table if not exists teste(id int);',
                       dag=dag)

insert_data_task = PostgresOperator(task_id='insert_data_task', postgres_conn_id='Postgres', 
                       sql='insert into teste values (1);',
                       dag=dag)

query_data_task = PostgresOperator(task_id='query_data_task', postgres_conn_id='Postgres', 
                       sql='select * from teste;',
                       dag=dag)

print_result_task = PythonOperator(task_id='print_result_task', python_callable=print_result, 
                                   provide_context=True, dag=dag)


create_table_task >> insert_data_task >> query_data_task >> print_result_task