"""Each table to migrate is a migration unit. There is a function to create the tasks for each migration. This
function is called by another function that returns a SubDag. And finally, the function that creates a SubDag is
called from a SubDagOperator within the maing DAG
"""
import logging
from pathlib import Path

import pandas as pd
from redshift_to_s3_operator import RedshiftToS3Operator
from s3_to_snowflake_operator import S3ToSnowflakeOperator
from snowflake_operator import SnowflakeOperator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils import dates

process_name = Path(__file__).stem[4:]
globals_variables = Variable.get("variables_secret", deserialize_json=True)

default_args = {
    "process_name": process_name,
    "start_date": dates.days_ago(1),
    "email": globals_variables["email_data"],
    "email_on_failure": False,
    "include_header": True,
    "warehouse": globals_variables["snowflake_warehouse_loading"]["es"],
    "check_constraint_keys": True,
    "on_error": "abort_statement",
    "trigger_rule": "all_done",
}


def get_date_range(start_date, end_date, period):
    # fixed frequency on month start MS now, table ods still failing sys out on agg by year
    freq_by = {
        "day": "D",
        "week": "W",
        "month": "MS",
        "year": "YS",
    }
    return pd.date_range(start=start_date, end=end_date, freq=freq_by[period])


def delete_previous_files(prefix):
    """So that if you do several unloads, the s3 to snowflake operator doesn't copy the files of previous unloads"""
    s3 = S3Hook()
    s3_bucket = globals_variables["s3_bucket"]
    logging.info(f"Print s3_bucket to use: {s3_bucket}")
    logging.info(f"Print prefix to use: {prefix}")
    to_process_files = s3.list_keys(bucket_name=s3_bucket, prefix=prefix)
    logging.info(f"Print s3.list_keys as to_process_files: {to_process_files}")

    s3.delete_objects(bucket=s3_bucket, keys=to_process_files)

    logging.info("Previous files deleted")


# Redshift UNLOAD options variations
# https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
unload_options_01 = ["ALLOWOVERWRITE", "PARALLEL ON", "HEADER", "ADDQUOTES", "ESCAPE", "DELIMITER AS '*'"]

# Snowflake COPY file_format variations
# https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html
file_format_01 = """(type = csv
                     skip_header = 1
                     field_delimiter = '*'
                     field_optionally_enclosed_by = '"'
                     escape = "\\"
                     null_if = ('')
                     compression = NONE)"""

# Each migration (mig_00, mig_01...) below will be executed within a SubDag, each subdag containing operators for each
# period between the end and the start. For the example below, there would be operators for the days 1, 2, 3, 4, 5, 6
# 7 and 8. Use this feature for heavy tables that need to be split.
migrations = {
    "mig_00": {
        "source": ("public", "ods_coupons_catalog_historic"),
        "target": ("public", "ods_coupons_catalog_historic"),
        "unload_options": unload_options_01,
        "file_format": file_format_01,
        "include_load_datetime": False,
        "unload_by_date": {
            "column": "date_catalog",
            "period": "day",
            "start": "2021-04-01",
            "end": "2021-04-08",
        },
    }
}


def load_migration_tasks(unload_info, unload_by_date=False, unload_dt=False):
    mig_tasks = []

    source_schema = unload_info["source"][0]
    source_table = unload_info["source"][1]
    target_schema = unload_info["target"][0]
    target_table = unload_info["target"][1]
    unload_options = unload_info["unload_options"]
    file_format = unload_info["file_format"]
    include_load_datetime = unload_info["include_load_datetime"]
    redshift_unload_query = False
    snowflake_previous_query = False
    snowflake_after_query = False
    unload_params = False
    copy_after_params = False

    if "source_unload_query" in unload_info:
        redshift_unload_query = unload_info["source_unload_query"]
        unload_params = {"source_schema": source_schema, "source_table": source_table}

    if "target_previous_query" in unload_info:
        snowflake_previous_query = unload_info["target_previous_query"]

    if "target_after_query" in unload_info:
        snowflake_after_query = unload_info["target_after_query"]
        copy_after_params = {"target_schema": target_schema, "target_table": target_table}

    if unload_by_date:
        unload_date = unload_dt
        unload_date_str = str(unload_date).replace("-", "_")
        s3_key = f"migration/{source_schema}/{source_table}/{unload_date_str}/{source_table}"

        delete_task_id = f"delete_{source_schema}_{source_table}_{unload_date_str}_files"
        unload_task_id = f"unload_{source_schema}_{source_table}_{unload_date_str}"

        unload_by_date_target_column = unload_by_date["column"]
        if "target_column" in unload_by_date:
            unload_by_date_target_column = unload_by_date["target_column"]

        if redshift_unload_query:
            unload_params["unload_period"] = unload_by_date["period"]
            unload_params["unload_column"] = unload_by_date["column"]
            unload_params["unload_date"] = unload_date
        else:
            redshift_unload_query = f"""
                SELECT *
                FROM {source_schema}.{source_table}
                WHERE date_trunc('{unload_by_date['period']}', {unload_by_date['column']}) = DATE('{unload_date}')
                ;
                """

        if not snowflake_previous_query:
            snowflake_previous_query = f"""
                DELETE
                FROM {target_schema}.{target_table}
                WHERE date_trunc('{unload_by_date['period']}', {unload_by_date_target_column}) = DATE('{unload_date}')
                ;
                """

        if snowflake_after_query:
            snowflake_previous_query += f"""\n
                CREATE OR REPLACE TRANSIENT TABLE {target_schema}.{target_table}_{unload_date_str}_format_stg 
                LIKE {target_schema}.{target_table}
                ;
                """

            copy_after_params["date_str"] = f"_{unload_date_str}"
            target_table = f"{target_table}_{unload_date_str}_format_stg"
            copy_after_id = f"copy_after_{source_schema}_{source_table}_{unload_date_str}"

        copy_task_id = f"copy_{source_schema}_{source_table}_{unload_date_str}"
        truncate_on_load = False
        delete_files = True

    else:  # not unload by date
        s3_key = f"migration/{source_schema}/{source_table}/{source_table}"
        unload_task_id = f"unload_{source_schema}_{source_table}"
        copy_task_id = f"copy_{source_schema}_{source_table}"
        delete_task_id = f"delete_{source_schema}_{source_table}_files"
        copy_after_id = f"copy_after_{source_schema}_{source_table}"

        if not snowflake_previous_query and source_schema != "desarrollo" and source_schema != "del":
            snowflake_previous_query = f"""
                CREATE TRANSIENT TABLE IF NOT EXISTS {target_schema}.{target_table}
                 LIKE {source_schema}.{source_table};
                 """

            if "clone_target_table" in unload_info:
                snowflake_previous_query += f"""\n
                            CREATE OR REPLACE TRANSIENT TABLE {target_schema}.{target_table.replace('redshift', 'snowflake')}
                            CLONE {source_schema}.{source_table};
                            """

        if snowflake_after_query:
            snowflake_previous_query += f"""\n
            CREATE OR REPLACE TRANSIENT TABLE {target_schema}.{target_table}_format_stg LIKE {target_schema}.{target_table}
            ;
            """
            target_table = f"{target_table}_format_stg"
            copy_after_id = f"copy_after_{source_schema}_{source_table}"

        truncate_on_load = True
        delete_files = False

    delete = PythonOperator(
        task_id=delete_task_id,
        python_callable=delete_previous_files,
        op_kwargs={"prefix": s3_key},
    )

    unload = RedshiftToS3Operator(
        task_id=unload_task_id,
        s3_bucket=globals_variables["s3_bucket"],
        s3_key=s3_key,
        table=source_table,
        schema=source_schema,
        query=redshift_unload_query,
        unload_options=unload_options,
        include_load_datetime=include_load_datetime,
        params=unload_params,
    )

    copy = S3ToSnowflakeOperator(
        task_id=copy_task_id,
        s3_bucket=globals_variables["s3_bucket"],
        s3_key=s3_key,
        table=target_table,
        schema=target_schema,
        file_format=file_format,
        previous_queries=snowflake_previous_query,
        truncate_table=truncate_on_load,
        delete_s3_file=delete_files,
        database="dwh_es",
    )
    delete >> unload >> copy

    mig_tasks.append(delete)
    mig_tasks.append(unload)
    mig_tasks.append(copy)

    if snowflake_after_query:
        copy_after = SnowflakeOperator(
            task_id=copy_after_id,
            sql=snowflake_after_query,
            check_constraint_keys=False,
            database="dwh_es",
            schema=target_schema,
            params=copy_after_params,
        )
        copy >> copy_after
        mig_tasks.append(copy_after)

    return mig_tasks


def load_mig_subdag(task_name, unload_info):
    with DAG(
        dag_id=f"{process_name}.{task_name}",
        catchup=False,
        default_args=default_args,
        schedule_interval=None,
        max_active_runs=1,
    ) as subdag:

        subdag_operators = []

        table = unload_info["source"][1]
        sb = DummyOperator(task_id=f"start_{table}_mig")
        eb = DummyOperator(task_id=f"end_{table}_mig")
        subdag_operators.append(sb)

        if "unload_by_date" in unload_info:

            unload_date_info = unload_info["unload_by_date"]
            unload_period = unload_date_info["period"]
            unload_by_date_range = get_date_range(
                unload_date_info["start"], unload_date_info["end"], unload_period
            )

            unload_by_period_tasks = []
            n_parallel_periods = 5

            for u_dt in unload_by_date_range:
                unload_dt = u_dt.date().strftime("%Y-%m-%d")
                date_str = unload_dt.replace("-", "_")

                period_tasks = load_migration_tasks(mig_info, unload_date_info, unload_dt)
                subdag_operators[-1] >> period_tasks[0] >> period_tasks[1] >> period_tasks[2]
                unload_by_period_tasks.append(period_tasks)

                if (
                    len(unload_by_period_tasks) == n_parallel_periods
                ):  # this is for not having so many parallel jobs
                    #  that can make Redshift overload. Modify accordingly to your Redshift cluster
                    cb = DummyOperator(task_id=f"closes_at_{date_str}")

                    for pt in unload_by_period_tasks:
                        pt[-1] >> cb
                        subdag_operators.append(pt)
                    subdag_operators.append(cb)
                    unload_by_period_tasks = []

            if len(unload_by_period_tasks) > 0:
                for pt in unload_by_period_tasks:
                    pt[-1] >> eb
                    subdag_operators.append(pt)

        else:  # not unload by date
            subdag_tasks = load_migration_tasks(mig_info)
            subdag_operators[-1] >> subdag_tasks[0] >> subdag_tasks[1] >> subdag_tasks[2] >> eb
            subdag_operators.append(subdag_tasks)

        subdag_operators.append(eb)

        return subdag


with DAG(process_name, default_args=default_args, schedule_interval=None) as dag:
    operators = []
    parallel_migrations = []
    n_parallel_migrations = 3
    parallel_bunch = 1

    start = DummyOperator(task_id="start_migrations")
    operators.append(start)

    for mig_key, mig_info in migrations.items():

        mig_subdag = f'{mig_info["source"][0]}_{mig_info["source"][1]}'

        subdag = SubDagOperator(
            subdag=load_mig_subdag(mig_subdag, mig_info),
            task_id=mig_subdag,
        )

        parallel_migrations.append(subdag)

        if (
            len(parallel_migrations) == n_parallel_migrations
        ):  # For not overloading Redshift/Airflow, there is a limit
            # in the number of parallel subdags (migrations) that can be run. Modify to your needs.
            mb = DummyOperator(task_id=f"interlude_{parallel_bunch}")

            operators[-1] >> parallel_migrations
            parallel_migrations >> mb

            operators.append(parallel_migrations)
            operators.append(mb)

            parallel_migrations = []
            parallel_bunch += 1

    if len(parallel_migrations) > 0:
        operators[-1] >> parallel_migrations
        operators.append(parallel_migrations)

    end = DummyOperator(task_id="end_migrations")
    operators[-1] >> end
