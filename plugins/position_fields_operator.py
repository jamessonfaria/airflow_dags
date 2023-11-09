from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import json

class PositionFieldsOperator(BaseOperator):

    @apply_defaults
    def __init__(self, path_input_file, path_output_file, 
                 position_fields, *args, **kwars) -> None:

        super().__init__(*args, **kwars)
        self.path_input_file = path_input_file
        self.path_output_file = path_output_file
        self.position_fields = position_fields

    def get_fields(self, lin):
        fields = []
        start = 0
        for start, end in self.position_fields:
            field = lin[start:end].strip()
            fields.append(field)
        return fields
    

    def transform_data(self):
        df = pd.read_csv(self.path_input_file, header=None)

        df['fields'] = df[0].apply(self.get_fields)
        df['row'] = df.index + 1
        df = df[['row', 'fields']]

        data_dict = df.to_dict(orient='records')

        with open(self.path_output_file, 'w') as json_file:
            json.dump(data_dict, json_file, indent=4)

    def execute(self, context):
        self.transform_data()
