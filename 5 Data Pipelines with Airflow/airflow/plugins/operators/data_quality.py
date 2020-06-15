from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 dq_checks = [],
                 tables = "",
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for check in self.dq_checks:
            error_count = 0
            
            check_name = check.get("check_name")
            check_sql = check.get("check_sql")
            expected_result = check.get("expected_result")
            
            for table in self.tables:
                formatted_sql = check_sql.format(table)
                
                records = redshift.get_records(formatted_sql)[0]
                
                if expected_result != records[0]:
                    self.log.info("Failed to load any records in table {}.".format(self.table))
                    error_count += 1
                    
            if error_count > 0:
                raise ValueError("Data quality check '{}' failed.".format(check_name))