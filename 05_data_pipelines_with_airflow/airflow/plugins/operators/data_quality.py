from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    Operator to check data quality output by previous tasks.
    Takes a list of pairs in which the first element is the SQL
    query and the second elment is the expected result.
    An error is raised if the result doesn't meet the expecation
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 sql_checks_and_expects=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_checks_and_expects = sql_checks_and_expects

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for check, expectation in self.sql_checks_and_expects:
            records = redshift.get_records(check)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. \"{check}\" returned no results")
            result = records[0][0]
            if result != expectation:
                raise ValueError(f"Data quality check failed. "
                                 "{check}\" expected {expectation} but returned {result} ")
            self.log.info(f"Data quality check \"{check}\" passed.")
        self.log.info("Successfully completed all data quality checks")
            