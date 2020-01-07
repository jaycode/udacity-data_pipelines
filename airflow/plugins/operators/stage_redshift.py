from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-west-2'
        JSON '{}'
        COMPUPDATE OFF
    """

    @apply_defaults
    def __init__(self,
                 aws_conn_id='aws_credentials',
                 db_conn_id='redshift',
                 table='staging_table_name',
                 data_path='',
                 jsonpaths_path='auto',
                 count_query="""SELECT COUNT(*) FROM table_name;""",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.db_conn_id = db_conn_id
        self.table = table
        self.data_path = data_path
        self.jsonpaths_path = jsonpaths_path
        self.count_query = count_query

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.db_conn_id)

        # Do not copy if rows already exists, as copying songs takes too much time!
        result = redshift.get_records(self.count_query)
        numrows = result[0][0]
        if numrows == 0:
            self.log.info("Table {} is empty, so we run the COPY operation.".format(self.table))
            formatted_sql = StageToRedshiftOperator.json_copy_sql.format(
                self.table,
                self.data_path,
                credentials.access_key,
                credentials.secret_key,
                self.jsonpaths_path)
            redshift.run(formatted_sql)
        else:
            self.log.info("Table {} has {} rows. Skip the COPY operation since the table is not empty.".format(self.table, numrows))
