from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    """
    Operator to load s3 JSON files into Redshift.
    Parameters can be provided to customize the 
    underlying Redshift COPY command.
    """
    
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        JSON '{}'
        REGION '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json="auto",
                 region="us-west-2",
                 sql_create="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json = json
        self.region = region
        self.aws_credentials_id = aws_credentials_id
        self.sql_create = sql_create

    def execute(self, context):
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        if self.sql_create:
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
            self.log.info(f"Creating table {self.table}")
            redshift.run(self.sql_create)

            self.log.info("Copying data from S3 to Redshift")
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            if self.json != 'auto':
                self.json = "s3://{}/{}".format(self.s3_bucket, self.json)
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                self.json,
                self.region,
                credentials.access_key,
                credentials.secret_key,
            )
            redshift.run(formatted_sql)

        self.log.info(f"Successfully staged {self.table}")




