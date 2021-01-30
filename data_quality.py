from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    The DataQualityOperator count the number of records from 4 dimention tables to check
    if the data are properly transformed and loaded, namely ["users","songs","artists","time"].
    If the table is empty, the task will raise an error upon execution
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables_list = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    def execute(self, context):
        for table in self.tables_list:
            
            redshift_hook = PostgresHook(self.redshift_conn_id)
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")