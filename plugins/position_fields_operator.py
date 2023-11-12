from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.models import Variable
import pandas as pd
import json

class PositionFieldsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, output_file, 
                 position_fields, have_header=False, *args, **kwars) -> None:

        super().__init__(*args, **kwars)
        self.output_file = output_file
        self.position_fields = position_fields
        self.have_header = have_header

    def get_fields(self, lin):
       return [lin[start:end].strip() for start, end in self.position_fields]
    
    # def get_file_s3(self):
    #     s3 = S3Hook(aws_conn_id="aws_s3_connection")
    #     path_file = s3.download_file(key=f"data/raw/{self.path_input_file}",
    #                                  bucket_name="brsp-airflow-prudential-s3",
    #                                  preserve_file_name=True)   
    #     return path_file
      
    def transform_data(self, **kwarg):
        path_file = Variable.get("path_file")
        skip_rows = 1 if self.have_header else 0
        df = pd.read_csv(path_file, header=None, skiprows=skip_rows)

        df['fields'] = df[0].apply(self.get_fields)
        df['row'] = df.index + 1

        data_dict = df[['row', 'fields']].to_dict(orient='records')
        
        json_output = json.dumps(data_dict, indent=4)

        s3 = S3Hook(aws_conn_id="aws_s3_connection")
        s3.load_string(string_data=json_output, 
                     key=f"data/output/{self.output_file}", 
                     bucket_name="brsp-airflow-prudential-s3", 
                     replace=True)

    def execute(self, context):
        # path_file = self.get_file_s3()
        self.transform_data()
