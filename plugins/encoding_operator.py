from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
import chardet

class EncodingOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwars) -> None:
        super().__init__(*args, **kwars)

    def execute(self, context):   
        input_path_file = Variable.get("input_path_file")
        with open(input_path_file, 'rb') as f:
            encoding_file = chardet.detect(f.read())['encoding']
            Variable.set("encoding_file", encoding_file)