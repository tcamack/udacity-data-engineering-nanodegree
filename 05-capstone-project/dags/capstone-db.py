from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
from airflow.models.log import Log

from datetime import datetime, timedelta

from operators import (CoursesOfferedUpdateOperator,
                       DataQualityCheckOperator,
                       StageToPostgresOperator)

default_args = {
    'owner': 'capstone-db-student',
    'depends_on_past': False,
    'catchup': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG('udacity_capstone_db_project',
         description='Load and transform data in PostgreSQL with Airflow.',
         start_date=datetime(2021, 12, 1),
         schedule_interval='@yearly',
         default_args=default_args) as dag:

    drop_tables = PostgresOperator(
        task_id='drop_all_tables',
        postgres_conn_id='postgres',
        sql='sql/capstone-db/drop-tables/drop_all_tables.sql'
    )

    with TaskGroup(group_id='table_creation') as table_creation:
        create_staging_tables = PostgresOperator(
            task_id='create_staging_tables',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/create-tables/create_staging_tables.sql'
        )

        create_database_tables = PostgresOperator(
            task_id='create_database_tables',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/create-tables/create_database_tables.sql'
        )

    with TaskGroup(group_id='data_staging') as data_staging:
        stage_education_data = StageToPostgresOperator(
            task_id='stage_education_data',
            postgres_conn_id='postgres',
            file_format='csv',
            table='staging_education',
            data_loc='./data/capstone-db/county_education.csv'
        )

        stage_income_data = StageToPostgresOperator(
            task_id='stage_income_data',
            postgres_conn_id='postgres',
            file_format='csv',
            table='staging_income',
            data_loc='./data/capstone-db/county_income_data.csv'
        )

        stage_fips_county_state_data = StageToPostgresOperator(
            task_id='stage_fips_county_state_data',
            postgres_conn_id='postgres',
            file_format='csv',
            table='staging_fips_county_state',
            data_loc='./data/capstone-db/fips_county_state.csv'
        )

        stage_fips_zip_data = StageToPostgresOperator(
            task_id='stage_fips_zip_data',
            postgres_conn_id='postgres',
            file_format='json',
            table='staging_fips_zip',
            data_loc='./data/capstone-db/fips_zip.json'
        )

        stage_institution_data = StageToPostgresOperator(
            task_id='stage_institution_data',
            postgres_conn_id='postgres',
            file_format='csv',
            table='staging_schools',
            data_loc='./data/capstone-db/institution_data.csv'
        )

        stage_institution_information_data = StageToPostgresOperator(
            task_id='stage_institution_information_data',
            postgres_conn_id='postgres',
            file_format='json',
            table='staging_school_information',
            data_loc='./data/capstone-db/institution_information.json'
        )

        stage_rent_data = StageToPostgresOperator(
            task_id='stage_rent_data',
            postgres_conn_id='postgres',
            file_format='csv',
            table='staging_rent',
            data_loc='./data/capstone-db/fmr.csv'
        )

        stage_population_data = StageToPostgresOperator(
            task_id='stage_population_data',
            postgres_conn_id='postgres',
            file_format='csv',
            table='staging_population',
            data_loc='./data/capstone-db/population_estimates.csv'
        )

    with TaskGroup(group_id='load_tables') as load_tables:
        insert_fips = PostgresOperator(
            task_id='insert_fips',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_fips.sql'
        )

        insert_zip = PostgresOperator(
            task_id='insert_zip',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_zip.sql'
        )

        insert_schools = PostgresOperator(
            task_id='insert_schools',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_schools.sql'
        )

        insert_courses = PostgresOperator(
            task_id='insert_courses',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_courses.sql'
        )

        insert_courses_offered = PostgresOperator(
            task_id='insert_courses_offered',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_courses_offered.sql'
        )

        insert_local_education = PostgresOperator(
            task_id='insert_local_education',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_local_education.sql'
        )

        insert_industries = PostgresOperator(
            task_id='insert_industries',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_industries.sql'
        )

        insert_local_income = PostgresOperator(
            task_id='insert_local_income',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_local_income.sql'
        )

        insert_local_rent = PostgresOperator(
            task_id='insert_local_rent',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_local_rent.sql'
        )

        insert_local_population = PostgresOperator(
            task_id='insert_local_population',
            postgres_conn_id='postgres',
            sql='sql/capstone-db/insert-tables/insert_population.sql'
        )

        dummy = DummyOperator(
            task_id='dummy_operator'
        )

        insert_prereqs = [insert_fips,
                          insert_zip >> insert_schools,
                          insert_courses,
                          insert_industries]

        insert_secondary = [insert_courses_offered,
                            insert_local_education,
                            insert_local_income,
                            insert_local_rent,
                            insert_local_population]

        insert_prereqs >> dummy >> insert_secondary

    update_courses_offered = CoursesOfferedUpdateOperator(
        task_id='update_courses_offered',
        postgres_conn_id='postgres',
        orig_table='staging_schools',
        dest_table='courses_offered',
        num_columns=8,
        column_names=['undergrad_cert',
                      'associates',
                      'bachelors',
                      'post_bacc',
                      'masters',
                      'doctoral',
                      'first_pro_deg',
                      'grad_cert']
    )

    with TaskGroup(group_id='data_quality_checks') as data_quality_checks:
        db_data_exists_check = DataQualityCheckOperator(
            task_id='db_data_exists_check',
            postgres_conn_id='postgres',
            mode='!=',
            quality_check_sql=[
                {'check_sql': """SELECT COUNT(*)
                                   FROM fips;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM zip;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM schools;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM courses;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM courses_offered;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_education;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM industries;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_income;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_rent;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_population;""",
                 'result': 0}
            ]
        )

        db_data_correct_check = DataQualityCheckOperator(
            task_id='db_data_correct_check',
            postgres_conn_id='postgres',
            mode='==',
            quality_check_sql=[
                {'check_sql': """SELECT COUNT(*)
                                   FROM fips
                                  WHERE fips IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM zip
                                  WHERE zip IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM schools
                                  WHERE opeid6 IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM courses
                                  WHERE cip_code IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM courses_offered
                                  WHERE (opeid6, cip_code) IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_education
                                  WHERE fips IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM industries
                                  WHERE naics_code IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_income
                                  WHERE fips IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_rent
                                  WHERE fips IS NULL;""",
                 'result': 0},
                {'check_sql': """SELECT COUNT(*)
                                   FROM local_population
                                  WHERE fips IS NULL;""",
                 'result': 0}
            ]
        )

    drop_staging_tables = PostgresOperator(
        task_id='drop_staging_tables',
        dag=dag,
        postgres_conn_id='postgres',
        sql='sql/capstone-db/drop-tables/drop_staging_tables.sql'
    )

drop_tables >> table_creation >> data_staging
data_staging >> load_tables >> update_courses_offered
update_courses_offered >> data_quality_checks >> drop_staging_tables
