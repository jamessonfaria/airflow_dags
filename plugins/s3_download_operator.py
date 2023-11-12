from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.models import Variable

class S3DownloadOperator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_connection, file, path, bucket, *args, **kwars) -> None:

        super().__init__(*args, **kwars)
        self.aws_connection = aws_connection
        self.file = file
        self.path = path
        self.bucket = bucket       

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_connection)
        input_path_file = s3.download_file(key=self.path + self.file,
                                        bucket_name=self.bucket,
                                        preserve_file_name=True)   
        Variable.set("input_path_file", input_path_file)

