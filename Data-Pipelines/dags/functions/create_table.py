from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

class CreateTableOperator(BaseOperator):
    ui_color = '#358140'
    def __init__(self, connection_id="redshift", sql_file="", *args, **kwargs):
        """
        Initialize function create table.

        :param connection_id: The Airflow connection ID assigned to the Redshift database.
        :param sql_file: URL of the SQL file containing the commands to be executed.
        """
        super().__init__(*args, **kwargs)
        self.connection_id = connection_id
        self.sql_file = sql_file

    def execute(self, context):
        """
        Execute the SQL commands provided in the specified file.
        """
        postgres_hook = PostgresHook(postgres_conn_id=self.connection_id)

        with open(self.sql_file, 'r') as file:
            sql_commands = file.read().split(';')

        # Remove any empty or whitespace-only commands.
        sql_commands = [cmd.strip() for cmd in sql_commands if cmd.strip()]

        for command in sql_commands:
            self.log.info(f"Run SQL command: {command}")
            postgres_hook.run(command)
