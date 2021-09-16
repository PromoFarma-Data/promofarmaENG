import logging
from pathlib import Path

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import dates
from redshift_to_s3_operator import RedshiftToS3Operator
from s3_to_snowflake_operator import S3ToSnowflakeOperator

process_name = Path(__file__).stem[4:]
globals_variables = Variable.get('variables_secret', deserialize_json=True)
default_args = {
    'process_name': process_name,
    'start_date': dates.days_ago(1),
    'email': globals_variables['email_data'],
    'email_on_failure': False,
    'include_header': True,
    'unload_options': ['ALLOWOVERWRITE', 'PARALLEL ON', 'ADDQUOTES', "DELIMITER AS '|'"],
    'file_format': globals_variables['file_format_csv']['es'],
    'warehouse': globals_variables['snowflake_warehouse_loading']['es'],
    'delete_s3_file': False,
    'on_error': 'abort_statement',  # on_error='continue' just for full unload of redshift
    'truncate_table': True
    # 'trigger_rule': 'all_done', only for full unload of redshift for not stopping the migration for just one failure,'
}


def delete_previous_files(table, schema):
    s3 = S3Hook()
    s3_bucket = globals_variables['s3_bucket']

    to_process_files = s3.list_keys(
        bucket_name=s3_bucket,
        prefix=f'migration/{schema}/{table}/{table}')

    s3.delete_objects(bucket=s3_bucket,
                      keys=to_process_files)
    logging.info('Previous files deleted')


with DAG(process_name,  # migrate tables with same name in redshift and snowflake
         default_args=default_args,
         schedule_interval=None
         ) as dag:

    for schema in ['public']:  # schema to be modified manually
        files_route = f'/opt/airflow/dags/migrate_redshift_to_snowflake/mig_{schema}'  # text file with a list of tables
        # to migrate, each one in a different line
        with open(files_route) as f:
            tables_list = f.read()
        tables_list = tables_list.split('\n')

        operators = []
        for table in tables_list:

            unload = RedshiftToS3Operator(
                task_id=f'unload_{schema}_{table}',
                s3_key=f'migration/{schema}/{table}/{table}',
                table=table,
                schema=schema,
                include_load_datetime=False,  # set to True if redshift table doesn't have the column and you want to add it to the unload
                load_datetime_value='NULL')
            operators.append(unload)

            copy = S3ToSnowflakeOperator(
                task_id=f'copy_{schema}_{table}',
                s3_key=f'migration/{schema}/{table}/{table}',
                table=table,
                schema=schema,
                database='dwh_es',
                )
            operators.append(copy)

for i in range(len(operators) - 1):
    operators[i] >> operators[i + 1]


with DAG('migrate_diff_tables',  # migrate tables with different names in redshift and snowflake
         default_args=default_args,
         schedule_interval=None
         ) as dag_diff:

    migrations = {
        'mig_01': {'source': ('public', 'table_name'),
                   'target': ('ext', 'table_name_2'),
                   'unload_options': [],  # these will replace the default values
                   'file_format': [],
                   'include_load_datetime': False
                   }
    }
    operators_diff = []

    for mig_key, mig_info in migrations.items():
        source_schema = mig_info['source'][0]
        source_table = mig_info['source'][1]
        target_schema = mig_info['target'][0]
        target_table = mig_info['target'][1]
        unload_options = mig_info['unload_options']
        file_format = mig_info['file_format']

        delete_diff = PythonOperator(
            task_id=f'delete_{source_schema}_{source_table}_files',
            python_callable=delete_previous_files,
            op_kwargs={"table": source_table, "schema": source_schema}
        )
        operators_diff.append(delete_diff)

        unload_diff = RedshiftToS3Operator(
            task_id=f'unload_{source_schema}_{source_table}',
            s3_key=f'migration/{source_schema}/{source_table}/{source_table}',
            table=source_table,
            schema=source_schema,
            unload_options=unload_options
        )
        operators_diff.append(unload_diff)

        copy_diff = S3ToSnowflakeOperator(
            task_id=f'copy_{target_schema}_{target_table}',
            s3_key=f'migration/{source_schema}/{source_table}/{source_table}',
            table=target_table,
            schema=target_schema,
            database='dwh_es',
            file_format=file_format
        )
        operators_diff.append(copy_diff)

for i in range(len(operators_diff) - 1):
    operators_diff[i] >> operators_diff[i + 1]
