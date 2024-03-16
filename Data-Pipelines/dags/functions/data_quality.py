from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Airflow operator designed for conducting data quality checks on tables within Redshift.

    :param redshift_conn_id: The Airflow connection ID for the Redshift database.
    :param tables: Collection of table names for conducting data quality checks.
    """
    @apply_defaults
    def __init__(
        self,
        redshift_conn_id: str,
        tables: list,
        *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables or []  # Ensuring that "tables" is a list to prevent a TypeError if it is None.

    def execute(self, context):
        self.log.info('Initiating Data Quality Checks...')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        failed_checks = []
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                failed_checks.append(table)

        # if check false
        if len(failed_checks) == 0:
            raise ValueError(f"Data quality check failed for the tables: {', '.join(failed_checks)}. They yielded no results or 0 records.")
        
        for table in self.tables:
            self.log.info(f"Data quality check succeeded for {table} with records present.")

