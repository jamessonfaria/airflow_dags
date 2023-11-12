from airflow import DAG
from position_fields_operator import PositionFieldsOperator
from s3_download_operator import S3DownloadOperator
from s3_upload_operator import S3UploadOperator
from datetime import datetime

dag = DAG('dag_poc_test100', description="DAG POC Test 100", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)

download_file_s3 = S3DownloadOperator(task_id="download_file_s3",
                                    aws_connection="aws_s3_connection",
                                    file="sample_100_1.txt",
                                    path="data/raw/",
                                    bucket="brsp-airflow-prudential-s3", 
                                    dag=dag)

process_100_rows_task = PositionFieldsOperator(task_id="process_100_rows_task", 
                                                output_file='sample_100_1.json',
                                                position_fields = [(0, 2), (2, 5), (5, 9), (9, 14), (14, 20)],
                                                dag=dag)

upload_file_s3 = S3UploadOperator(task_id="upload_file_s3",
                                    aws_connection="aws_s3_connection",
                                    file="sample_100_1.json",
                                    path="data/output/",
                                    bucket="brsp-airflow-prudential-s3", 
                                    dag=dag)

download_file_s3 >> process_100_rows_task >> upload_file_s3
