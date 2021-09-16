from typing import Any, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pf_utils.db import constraint_keys_check


class S3ToSnowflakeOperator(BaseOperator):
    """
    Executes an COPY command to load files from s3 to Snowflake

    :param s3_key: reference to s3 key(s). For example, if you set the argument as an str 'try_file' and within
        the bucket there are files called try_file_01, try_file_053, try_file_again_trying.csv and so on,
        all of them will be copied.
    :type s3_key: str
    :param table: reference to a specific table in snowflake database to copy the data in
    :type table: str
    :param schema: reference to a specific schema in snowflake database
    :type schema: str
    :param database: reference to a specific database in Snowflake connection
        (will overwrite any database defined in the connection's extra JSON)
    :type database: str
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param s3_bucket: reference to a specific S3 bucket. By default it is promofarma-data
    :type s3_bucket: str
    :param snowflake_conn_id: reference to a specific snowflake database
    :type snowflake_conn_id: str
    :param file_format: reference to a specific file format
    :type file_format: str
    :param stage: reference to a specific snowflake stage. If the stage's schema is not the same as the
        table one, it must be specified
    :type stage: str
    :param previous_queries: queries to be executed before the copy. For instance, create statements. It can be an sql
        file or a query
    :type previous_queries: str
    :param truncate_table: whether or not to truncate the table before copying the data on it
    :type truncate_table: bool
    :param on_error: what snowflake should behave when there is an error during the copy. It can take the following
        values: CONTINUE | SKIP_FILE | SKIP_FILE_<num> | SKIP_FILE_<num>% | ABORT_STATEMENT
    :type on_error: str
    :param columns_array: reference to a specific columns array in snowflake database
    :type columns_array: list
    :param file_columns: reference to specific columns in the s3 source file, support column reorder, table.metadata
     and load transformations on param ex.: file_columns=["$1", "metadata$filename", "split_part($2, '.csv',  0)"].
     More info in https://docs.snowflake.com/en/user-guide/data-load-transform.html
    :type file_columns: list
    :param fail_when_empty_copy: if the copy processes 0 files, whether the operator shall fail or not.
    :type fail_when_empty_copy: bool
    :param check_constraint_keys: check if constraint keys (primary key or unique key if any) have duplicated values.
        If bool, it will check the constraint keys on the above specified table and schema. If list, it must be
        a list of tuples with two strings: first table schema, followed by table name. For example [('int', 'table_1')
    :type check_constraint_keys: Union[bool, list]
    :param constraint_types: table constraints to be checked as string elements of a list. Possible values are
        'primary' and 'unique'.
    :type constraint_types: list
    :param move_s3_file: there are some processes where is useful to have a folder to_process and other
        called processed. This set to True will move files from to_process to processed.
    :type move_s3_file: bool
    :param delete_s3_file: whether or not to delete the source file in S3 after copying the data. We store the files in
        s3 in two 'folders': sta and stg. Sta is starting, and it stores the original data from the processes, being
        there for months. Stg is the staging area, where the processed data is meant to be there just temporarily, until
        the file is copied to s3. This is True by default but it will check if the the data comes from sta or
        stg folders. If it comes from sta, it won't delete the file.
    :type delete_s3_file: bool
    :param s3_stg: s3 prefix for our staging s3 area
    :type s3_stg: str
    :param s3_sta: s3 prefix for our starting s3 area
    :type s3_sta: str
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :type autocommit: bool
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :type role: str
    """

    template_fields = (
        "s3_key",
        "previous_queries",
        "table",
        "s3_bucket",
        "s3_sta",
        "s3_stg",
        "file_format",
    )
    template_ext = (".sql",)

    def __init__(
        self,
        *,
        s3_key: str,
        table: str,
        schema: str,
        database: str,
        warehouse: str,
        s3_bucket: str = "{{ var.json.get('variables_secret').s3_bucket }}",
        snowflake_conn_id: str = "data_db_snowflake_airflow",
        file_format: str = "{{ var.json.get('variables_secret').file_format_csv_global }}",
        stage: str = "ods.s3",
        previous_queries: Optional[str] = None,
        truncate_table: bool = False,
        on_error: str = "abort_statement",
        columns_array: Optional[list] = None,
        file_columns: Optional[list] = None,
        fail_when_empty_copy: bool = True,
        check_constraint_keys: Union[bool, list] = True,
        constraint_types: list = None,
        move_s3_file: bool = False,
        delete_s3_file: bool = True,
        s3_stg: str = "{{ var.json.get('variables_secret').s3_stg }}",
        s3_sta: str = "{{ var.json.get('variables_secret').s3_sta }}",
        autocommit: bool = True,
        role: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_key = s3_key
        self.table = table
        self.schema = schema
        self.database = database
        self.warehouse = warehouse
        self.s3_bucket = s3_bucket
        self.snowflake_conn_id = snowflake_conn_id
        self.file_format = file_format
        self.stage = stage
        self.previous_queries = previous_queries
        self.truncate_table = truncate_table
        self.on_error = on_error
        self.columns_array = columns_array
        self.file_columns = file_columns
        self.fail_when_empty_copy = fail_when_empty_copy
        self.check_constraint_keys = check_constraint_keys

        if constraint_types is None:
            self.constraint_types = ["primary", "unique"]
        else:
            self.constraint_types = constraint_types

        self.autocommit = autocommit
        self.move_s3_file = move_s3_file
        self.delete_s3_file = delete_s3_file
        self.s3_stg = s3_stg
        self.s3_sta = s3_sta
        self.role = role

    def execute(self, context: Any) -> None:
        snowflake_hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

        if self.previous_queries:
            self.log.info("Executing previous queries...")
            snowflake_hook.run(self.previous_queries, self.autocommit)
            self.log.info("Queries completed...")

        if self.truncate_table:
            self.log.info("Truncating table prior to COPY command")
            snowflake_hook.run(f"TRUNCATE TABLE {self.schema}.{self.table}", self.autocommit)

        if self.file_columns:
            base_sql = f"""
                FROM (
                    SELECT {",".join(self.file_columns)}
                    FROM @{self.stage}/{self.s3_key}
                )
                file_format={self.file_format}
                """
        else:
            base_sql = f"""
                FROM @{self.stage}/{self.s3_key}
                file_format={self.file_format}
            """

        if self.columns_array:
            copy_query = f'COPY INTO {self.schema}.{self.table}({",".join(self.columns_array)}) {base_sql}'
        else:
            copy_query = f"COPY INTO {self.schema}.{self.table} {base_sql}"

        copy_query += f"\nON_ERROR='{self.on_error}'"

        self.log.info("Executing COPY command...")
        info_rows = snowflake_hook.run([f"USE SCHEMA {self.schema}", copy_query], self.autocommit)
        if self.fail_when_empty_copy and info_rows[0]["status"] == "Copy executed with 0 files processed.":
            raise AirflowException("The copy processed 0 files")
        self.log.info("COPY command completed")

        if self.check_constraint_keys:
            constraint_keys_check(
                schema=self.schema,
                table=self.table,
                constraint_types=self.constraint_types,
                hook=snowflake_hook,
                log=self.log,
            )

        s3 = S3Hook(aws_conn_id="aws_default")

        if self.move_s3_file:
            s3_objects = s3.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key)
            for key in s3_objects:
                processed_key = key.replace("/to_process/", "/processed/")
                s3.copy_object(
                    source_bucket_key=key,
                    dest_bucket_key=processed_key,
                    source_bucket_name=self.s3_bucket,
                    dest_bucket_name=self.s3_bucket,
                )
                self.log.info(f"File moved successfully from {key} to {processed_key}")

        if self.delete_s3_file:
            s3_objects = s3.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key)
            s3_folder = self.s3_key.lstrip("/").split("/")[1]

            if s3_folder == self.s3_sta and not self.move_s3_file:
                self.log.info(f"The S3 file comes from {self.s3_sta}, so it will not be deleted")
            else:
                for key in s3_objects:
                    s3.delete_objects(bucket=self.s3_bucket, keys=key)
                    self.log.info(f"File {key} deleted from S3")
