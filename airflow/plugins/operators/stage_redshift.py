from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    json_copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
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
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.db_conn_id = db_conn_id
        self.table = table
        self.data_path = data_path,
        self.jsonpaths_path = jsonpaths_path

    def execute(self, context):
        aws_hook = AwsHook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.db_conn_id)
        redshift_hook.run(json_copy_sql.format(self.table,
                                               self.data_path,
                                               credentials.access_key,
                                               credentials.secret_key,
                                               self.jsonpaths_path))
