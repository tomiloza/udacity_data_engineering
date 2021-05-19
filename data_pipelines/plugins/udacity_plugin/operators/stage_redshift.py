from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from dateutil import parser
from psycopg2 import errors

table_does_not_exist = errors.lookup('42P01')


class StageToRedshiftOperator(BaseOperator):
    """
        Operator populates the staging table from the given S3 data buckets. Backfilling is supported so based on the execution time
    """
    ui_color = '#358140'

    COPY_SQL = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'
        """

    template_fields = ("execution_date",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='us-west-2',
                 json_path='auto',
                 backfill_data=False,
                 execution_date='',
                 *args, **kwargs):
        """
        :param redshift_conn_id: Connection Id of stored Postgres Connection in Airflow
        :param aws_credentials_id: AWS credentials Id stored in Airflow
        :param table: Staging table name
        :param s3_bucket: S3 bucket name
        :param s3_key: S3 key name
        :param region: S3 region
        :param json_path: Path of the json description file for the data
        :param backfill_data: This flag is set to true if we want to backfill the data based on the execution time
        :param execution_date: Execution date of dag
        """
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_path = json_path
        self.execution_date = execution_date
        self.backfill_data = backfill_data

    def execute(self, context):
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Date:' + self.execution_date)
        date = parser.parse(self.execution_date)

        self.log.info("Backfill_data: {}".format(self.backfill_data))
        s3_bucket_key = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        if self.backfill_data:
            s3_path = s3_bucket_key + '/' + str(date.year) + '/' + str(date.month)
        else:
            s3_path = s3_bucket_key
        self.log.info("S3 path: {}".format(s3_path))

        self.log.info("Deleting data from table {}.".format(self.table))

        try:
            redshift.run("DELETE FROM {}".format(self.table))
        except table_does_not_exist as ex:
            self.log.info("Andrea does not exist")

        copy_sql = self.COPY_SQL.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        self.log.info("SQL Statement Executing on Redshift: {}".format(copy_sql))
        redshift.run(copy_sql)
