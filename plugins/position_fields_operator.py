from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
from airflow.models import Variable
import pandas as pd
import json
import tempfile
import os

class PositionFieldsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, output_file, position_fields, 
                 have_header=False, *args, **kwars) -> None:

        super().__init__(*args, **kwars)
        self.output_file = output_file
        self.position_fields = position_fields
        self.have_header = have_header

    def get_path_file(self):
        temp_dir = tempfile.mkdtemp()
        return os.path.join(temp_dir, self.output_file)

    def get_fields(self, lin):
       return [lin[start:end].strip() for start, end in self.position_fields]
      
    def transform_data(self, **kwarg):
        input_path_file = Variable.get("input_path_file")
        skip_rows = 1 if self.have_header else 0
        df = pd.read_csv(input_path_file, header=None, skiprows=skip_rows)

        df['fields'] = df[0].apply(self.get_fields)
        df['row'] = df.index + 1

        data_dict = df[['row', 'fields']].to_dict(orient='records')

        output_path_file = self.get_path_file()
        Variable.set("output_path_file", output_path_file)
        
        with open(output_path_file, 'w') as json_file:
            json.dump(data_dict, json_file, indent=4)

    def execute(self, context):
        self.transform_data()
