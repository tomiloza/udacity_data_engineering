from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
        This operator execute the SQL checks and validates the results
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 sql_check=[],
                 expected_results=[],
                 *args, **kwargs):
        """

        :param redshift_conn_id: Connection Id of stored Postgres Connection in Airflow
        :param sql_check: SQL checks executes by operators
        :param expected_results: The results that are expected for the cheks should be placed in the same order the the sql_checks
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_check = sql_check
        self.expected_results = expected_results

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)
        for num, query in enumerate(self.sql_check):
            self.log.info("Executing data quality check query:")
            self.log.info(query)
            result = redshift.get_records(query)
            if len(result) < 1 or len(result[0]) < 1:
                raise ValueError("Data quality check failed. No rows returns for query {}".format(query))
            num_records = result[0][0]
            if self.expected_results[num] != num_records:
                raise ValueError("Data quality check failed: {}. Expecting num of records {}, but returned {}".format(
                    query, self.expected_results[num], num_records))
        self.log.info("All data quality checks are passed")
