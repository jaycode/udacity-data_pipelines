from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """INSERT INTO {dest_table}
                    {select_sql};"""

    @apply_defaults
    def __init__(self,
                 conn_id='redshift',
                 table='',
                 query='',
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.append_only = append_only

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.conn_id)
        if self.append_only:
            self.log.info("Parameter `append_only` is set to True. We will only append data into `{}`".format(self.table))
        else:
            self.log.info("Parameter `append_only` is set to False. We will drop and recreate `{}`".format(self.table))
            redshift_hook.run("DELETE FROM {}".format(self.table))

        self.log.info("Insert into table {}".format(self.table))
        sql = LoadDimensionOperator.insert_sql.format(
            dest_table=self.table,
            select_sql=self.query)
        redshift_hook.run(sql)
