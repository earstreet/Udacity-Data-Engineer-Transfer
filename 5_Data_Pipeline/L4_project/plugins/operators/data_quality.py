from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Test the data quality in the tables for the given SQL checks which 
    are compared to the expected results.

    Parameters:
    redshift_conn_id: connection ID to Redshift cluster
    dq_checks: Dictionaries with the SQL queries in 'check_sql' and 
    the 'expected_result'
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define operators params (with defaults)
                 redshift_conn_id = '',
                 dq_checks = '',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Create postgres hook to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Solution according https://knowledge.udacity.com/questions/54406
        self.log.info("Checking data quality")
        error_count = 0
        for check in self.dq_checks:
            sql = check.get('check_sql')
            self.log.info(f"Checking: {sql}")
            exp_result = check.get('expected_result')
            self.log.info(f"Expecting: {exp_result}")

            records = redshift.get_records(sql)[0]
            
            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)
 
        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failing_tests)
            raise ValueError('Data quality check failed')