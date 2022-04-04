from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

from pathlib import Path
import json


class StageToPostgresOperator(BaseOperator):
    ui_color = '#A5D49E'
    template_fields = ()

    def __init__(self,
                 postgres_conn_id='',
                 file_format='',
                 delimiter=',',
                 table='',
                 data_loc='',
                 *args, **kwargs):

        super(StageToPostgresOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.file_format = file_format
        self.delimiter = delimiter
        self.table = table
        self.data_loc = data_loc

    def execute(self, context):
        hook = PostgresHook.get_hook(self.postgres_conn_id)
        path = Path(self.data_loc)

        if self.file_format == 'csv':
            if path.exists():
                sql = f"""
                       COPY {self.table}
                       FROM STDIN
                       WITH CSV HEADER DELIMITER AS '{self.delimiter}'
                       """
                hook.copy_expert(sql, self.data_loc)
            else:
                raise FileNotFoundError(
                    f'File "{self.data_loc}" does not exist.'
                )
        elif self.file_format == 'json':
            if path.exists():
                with open(self.data_loc, 'r+') as file:
                    with hook.get_conn() as conn:
                        with conn.cursor() as cur:
                            data = json.load(file)
                            sql = f"""
                                   INSERT INTO {self.table}
                                   SELECT *
                                     FROM json_populate_recordset
                                          (NULL::{self.table}, %s);
                                   """
                            cur.execute(sql, (json.dumps(data),))
            else:
                raise FileNotFoundError(
                    f'File "{self.data_loc}" does not exist.'
                )
        else:
            raise Exception(
                f'File format "{self.file_format}" is not supported.'
            )
