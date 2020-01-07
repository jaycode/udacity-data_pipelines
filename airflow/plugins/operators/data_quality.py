from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    count_sql="SELECT COUNT(*) FROM {table_name};"

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 non_empty_tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.non_empty_tables = non_empty_tables

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)

        for table in self.non_empty_tables:
            self.log.info("Counting rows in {} table...".format(table))

            sql = DataQualityOperator.count_sql.format(table_name=table)            
            records = redshift.get_records(sql)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results.")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows.")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records.")