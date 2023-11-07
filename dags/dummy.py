from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

dag = DAG('dummy', description="Dummy", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)

task1 = BashOperator(task_id="tsk1", bash_command="sleep 1", dag=dag)
task2 = BashOperator(task_id="tsk2", bash_command="sleep 1", dag=dag)
task3 = BashOperator(task_id="tsk3", bash_command="sleep 1", dag=dag)
task4 = BashOperator(task_id="tsk4", bash_command="sleep 1", dag=dag)
task5 = BashOperator(task_id="tsk5", bash_command="sleep 1", dag=dag)
dummy = DummyOperator(task_id="tskdummy", dag=dag)

[task1, task2, task3] >> dummy >> [task4, task5]