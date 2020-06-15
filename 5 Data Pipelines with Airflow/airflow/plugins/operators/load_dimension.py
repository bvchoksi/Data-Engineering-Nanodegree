from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    if self.truncate == "y":
        sql = """
            truncate table {};
            insert into {} {};
            commit;
        """
    else:
        sql = """
            insert into {} {};
            commit;
        """
    
    @apply_defaults
    def __init__(self,
                 table = "",
                 truncate = "y",
                 redshift_conn_id = "",
                 
                 sql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.truncate = truncate
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Loading dim table {}.".format(self.table))
        
        if self.truncate == "y":
            formatted_sql = LoadDimensionOperator.sql.format(
                self.table,
                self.table,
                self.sql
            )
        else:
            formatted_sql = LoadDimensionOperator.sql.format(
                self.table,
                self.sql
            )
        redshift.run(formatted_sql)
