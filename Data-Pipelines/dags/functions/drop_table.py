from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class DropTableOperator(BaseOperator):
    ui_color = '#358140'
    def __init__(
        self,
        tables=None,
        connection_id="redshift",
        *args, **kwargs
    ):
        """
        Sets up the operator for dropping tables in a Redshift database.

        :param tables: A collection of table names slated for removal.
        :param connection_id: The Airflow connection ID assigned to the Redshift database.
        """
        super().__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.tables = tables or []  # Handle table null

    def execute(self, context):
        """
        Initiates the table dropping procedure.
        """
        
        if self.tables:
            postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)
            self.log.info(f"Dropping all tables.{self.tables}")

            for table in self.tables:
                drop_query = f"DROP TABLE IF EXISTS public.{table};"
                postgres_hook.run(drop_query)
                self.log.info(f"Table .{table} dropped successfully.")
        else:
            self.log.info("There are no tables to remove.")
