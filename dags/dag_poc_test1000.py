from airflow import DAG
from position_fields_operator import PositionFieldsOperator
from s3_download_operator import S3DownloadOperator
from s3_upload_operator import S3UploadOperator
from encoding_operator import EncodingOperator
from datetime import datetime

dag = DAG('dag_poc_test1000', description="DAG POC Test 1000", 
            schedule_interval=None, start_date=datetime(2023,3,5),
            catchup=False)

download_file_s3_task = S3DownloadOperator(task_id="download_file_s3_task",
                                    aws_connection="aws_s3_connection",
                                    file="sample_1000 1.txt",
                                    path="data/raw/",
                                    bucket="brsp-airflow-prudential-s3", 
                                    dag=dag)

identify_encoding_task = EncodingOperator(task_id="identify_encoding_task", dag=dag)

process_1000_rows_task = PositionFieldsOperator(task_id="process_1000_rows_task", 
                                                output_file='sample_1000_1.json',
                                                position_fields = [(0, 6), (6, 17), (17, 23), (23, 30), (30, 38)],
                                                dag=dag)

upload_file_s3_task = S3UploadOperator(task_id="upload_file_s3_task",
                                    aws_connection="aws_s3_connection",
                                    file="sample_1000_1.json",
                                    path="data/output/",
                                    bucket="brsp-airflow-prudential-s3", 
                                    dag=dag)

download_file_s3_task >> identify_encoding_task >> process_1000_rows_task >> upload_file_s3_task