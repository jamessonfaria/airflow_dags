from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import pandas as pd
import json
import tempfile
import os

class PositionFieldsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, output_file, position_fields=None, position_fields_tab=False, 
                 remove_header=False, *args, **kwars) -> None:

        super().__init__(*args, **kwars)
        self.output_file = output_file
        self.position_fields = position_fields
        self.position_fields_tab = position_fields_tab
        self.remove_header = remove_header

    def get_path_file(self):
        temp_dir = tempfile.mkdtemp()
        return os.path.join(temp_dir, self.output_file)

    def get_fields(self, lin):
       return [lin[start:end].strip() for start, end in self.position_fields]

    def get_df_transformed(self):
        input_path_file = Variable.get("input_path_file")
        encoding_file = Variable.get("encoding_file")
        skip_rows = 1 if self.remove_header else 0
        
        if self.position_fields_tab:
            df = pd.read_csv(input_path_file, header=None, skiprows=skip_rows, 
                         encoding=encoding_file, delimiter='\t')
            df['fields'] = df.apply(list, axis=1)
        else:
            df = pd.read_csv(input_path_file, header=None, skiprows=skip_rows, 
                         encoding=encoding_file)
            df['fields'] = df[0].apply(self.get_fields)
        df['row'] = df.index + 1
        return df

    def transform_data(self, **kwarg):
        df = self.get_df_transformed()

        data_dict = df[['row', 'fields']].to_dict(orient='records')

        output_path_file = self.get_path_file()
        Variable.set("output_path_file", output_path_file)
        
        with open(output_path_file, 'w') as json_file:
            json.dump(data_dict, json_file, indent=4)

    def execute(self, context):
        self.transform_data()
