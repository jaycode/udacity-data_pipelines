from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """DROP TABLE IF EXISTS {dest_table};
                    CREATE TABLE {dest_table} AS
                    {select_sql}"""

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 table='',
                 query='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        sql = LoadFactOperator.insert_sql.format(
            dest_table=self.table,
            select_sql=self.query)
        redshift_hook.run(sql)