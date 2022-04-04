from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators


class CapstonePlugin(AirflowPlugin):
    name = 'capstone_plugin',
    operators = [
        operators.StageToPostgresOperator,
        operators.CoursesOfferedUpdateOperator,
        operators.DataQualityCheckOperator
    ]
