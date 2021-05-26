from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
        Loads the dimension tables with data. For inserting data into the table the given SQL statement is used
    """
    ui_color = '#80BD9E'

    INSERT_SQL = 'INSERT INTO {} ({})'
    DELETE_SQL = 'DELETE FROM {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql='',
                 delete_all_rows=False,
                 *args, **kwargs):
        """
        :param redshift_conn_id: Connection Id of stored Postgres Connection in Airflow
        :param table: Table name for insert and delete operations
        :param sql: SQL for inserting into the dimension table
        :param delete_all_rows: Set to <b>true</b> if it is necessary to truncate the table before loading
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.delete_all_rows = delete_all_rows

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_all_rows:
            self.log.info("Deleting all rows from dimension table {}".format(self.table))
            delete_stmt = self.DELETE_SQL.format(self.table)
            self.log.info(delete_stmt)
            redshift.run(delete_stmt)

        insert_stmt = self.INSERT_SQL.format(self.table, self.sql)
        self.log.info("Insert statement for dimension {}".format(insert_stmt))
        redshift.run(insert_stmt)
