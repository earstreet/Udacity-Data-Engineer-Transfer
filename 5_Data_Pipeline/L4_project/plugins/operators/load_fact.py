from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Insert data from the staging tables to the fact table.

    Parameters:
    redshift_conn_id: connection ID to Redshift cluster
    table: name of the staging table
    sql_querie: querie for the fact table
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id = '',
                 table = '',
                 sql_querie = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_querie = sql_querie

    def execute(self, context):
        self.log.info('Create postgres hook to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Insert songplays table')
        formatted_sql = f'INSERT INTO {self.table} ({self.sql_querie})'
        redshift.run(formatted_sql)

