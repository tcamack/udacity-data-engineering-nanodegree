from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

import re


class DataQualityCheckOperator(BaseOperator):
    ui_color = '#D2DA4C'

    def __init__(self,
                 postgres_conn_id='',
                 mode='',
                 quality_check_sql=[],
                 *args, **kwargs):

        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.quality_check_sql = quality_check_sql
        self.mode = mode

    def execute(self, context):
        hook = PostgresHook.get_hook(self.postgres_conn_id)

        tests_failed = 0
        failed_tests = []

        self.log.info('Beginning data quality checks')

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for check in self.quality_check_sql:
                    result = check.get('result')
                    check_sql = check.get('check_sql')

                    cur.execute(check_sql)

                    query_result = cur.fetchall()

                    if self.mode == '>':
                        if query_result <= result:
                            tests_failed += 1
                            check_sql = re.sub(r'\b\s+\b', ' ', check_sql)
                            failed_tests.append(check_sql)

                    elif self.mode == '<':
                        if query_result >= result:
                            tests_failed += 1
                            check_sql = re.sub(r'\b\s+\b', ' ', check_sql)
                            failed_tests.append(check_sql)

                    elif self.mode == '==':
                        if not query_result != result:
                            tests_failed += 1
                            check_sql = re.sub(r'\b\s+\b', ' ', check_sql)
                            failed_tests.append(check_sql)

                    elif self.mode == '!=':
                        if query_result == result:
                            tests_failed += 1
                            check_sql = re.sub(r'\b\s+\b', ' ', check_sql)
                            failed_tests.append(check_sql)

                    else:
                        self.log.error(f"Mode '{self.mode}' not recognized")
                        raise ValueError('An error occured before the '
                                         'data quality check could begin')

        if tests_failed > 0:
            self.log.warning(f'{tests_failed} total tests '
                             f'failed: {failed_tests}')
            raise ValueError('Data quality checks failed')

        self.log.info('Data quality checks complete')
