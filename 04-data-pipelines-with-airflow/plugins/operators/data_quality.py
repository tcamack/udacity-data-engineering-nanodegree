from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 quality_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.quality_checks = quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        tests_failed = 0
        failed_tests = []

        self.log.info('Beginning data quality checks')

        for quality_check in self.quality_checks:
            expected_result = quality_check.get('expected_result')
            quality_sql = quality_check.get('quality_sql')

            quality_result = redshift_hook.get_records(quality_sql)[0]

            if expected_result != quality_result[0]:
                tests_failed += 1
                failed_tests.append(quality_sql)

        if tests_failed > 0:
            self.log.info(f'\n{tests_failed} total tests failed: {failed_tests}')
            raise ValueError('Data quality check failed')

        self.log.info('Data quality checks complete')
