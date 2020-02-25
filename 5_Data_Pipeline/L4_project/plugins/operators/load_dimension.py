from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Insert data from the staging tables to the dimension tables.
    
    Parameters:
    redshift_conn_id: connection ID to Redshift cluster
    table: name of the staging table
    sql_querie: querie for the dimension table
    insert_mode: 'overwrite' or 'append' the data to the existing table
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id = '',
                 table = '',
                 sql_querie = '',
                 insert_mode = 'overwrite',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_querie = sql_querie
        self.insert_mode = insert_mode

    def execute(self, context):
        self.log.info('Create postgres hook to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.insert_mode == 'overwrite':
            self.log.info(f'Insert and overwrite {table} table')
            formatted_sql = f'TRUNCATE {self.table}; INSERT INTO {self.table} ({self.sql_querie})'
        elif self.insert_mode == 'append':
            self.log.info(f'Append {self.table} table')
            formatted_sql = f'INSERT INTO {self.table} ({self.sql_querie})'
        else:
            raise ValueError(f'Insert mode <{self.insert_mode}> not known. Choose <insert> or <overwrite> instead.')
        redshift.run(formatted_sql)