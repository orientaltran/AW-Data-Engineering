from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    # Define your operators params (with defaults) here
    def __init__(
        self,
        connection_id: str,
        table: str,
        sql_query: str,
        is_truncated: bool = True,
        *args, **kwargs
    ):
        """
        Initializes the LoadFactOperator for loading data into a fact table.

        :param connection_id: The Airflow connection ID to utilize for the PostgreSQL database.
        :param table: The name of the destination fact table.
        :param sql_query: The SQL query to be executed for loading data into the fact table.
        :param is_truncated: A flag to indicate whether the fact table should be truncated before loading new data.
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.table = table
        self.sql_query = sql_query
        self.is_truncated = is_truncated
		# Map params here
    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)
        self.log.info(f'Loading data into {self.table} fact table.')

        if self.is_truncated:
            self.log.info(f"Clearing data from {self.table} table...")
            postgres_hook.run(f"TRUNCATE TABLE {self.table}")
            self.log.info(f"Table {self.table} has been cleared.")

        insert_sql = f"INSERT INTO {self.table} {self.sql_query};"
        postgres_hook.run(insert_sql)

        self.log.info(f'Data has been loaded into {self.table} fact table.')
