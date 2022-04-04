from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator


class CoursesOfferedUpdateOperator(BaseOperator):
    ui_color = '#D49EA1'

    def __init__(self,
                 postgres_conn_id='',
                 orig_table='',
                 dest_table='',
                 num_columns=1,
                 column_names=[],
                 *args, **kwargs):

        super(CoursesOfferedUpdateOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.orig_table = orig_table
        self.dest_table = dest_table
        self.num_columns = num_columns
        self.column_names = column_names

    def execute(self, context):
        hook = PostgresHook.get_hook(self.postgres_conn_id)

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for i in range(0, self.num_columns):
                    col = self.column_names[i]
                    sql = f"""
                           UPDATE {self.dest_table}
                              SET {col} = TRUE
                             FROM (SELECT opeid6, cip_code
                                     FROM {self.orig_table}
                                    WHERE cred_level = {i + 1}
                                    ORDER BY opeid6, cip_code) sub
                            WHERE {self.dest_table}.opeid6 = sub.opeid6
                              AND {self.dest_table}.cip_code = sub.cip_code;
                           """
                    cur.execute(sql)
