from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    The LoadFactOperator executes queries to join staging data (logs and songs) to form
    the songplays table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_data_query = "",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_data_query = load_data_query
        self.table = table

    def execute(self, context):
 
        self.log.info('Starting to load data to fact table.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run(self.load_data_query.format(self.table))
        self.log.info("Data loaded to the fact table {}.".format(self.table))
