from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    """
    Operator to load dimension tables.  Takes optional
    "drop_first" and "sql_create" params to drop the table
    and create it before loading rows.
    """
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 sql_create="",
                 drop_first=False,
                 sql_insert="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_create = sql_create
        self.drop_first = drop_first
        self.sql_insert = sql_insert
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.sql_create:
            if self.drop_first:
                self.log.info(f"Dropping table {self.table}")
                redshift.run(f"DROP TABLE IF EXISTS {self.table}")
            self.log.info(f"Creating table {self.table}")
            redshift.run(self.sql_create)
            
        if self.truncate:
            self.log.info(f"Clearing data from table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")   
            
        redshift.run(self.sql_insert)
        self.log.info(f"Successfully loaded dim table: {self.table}")