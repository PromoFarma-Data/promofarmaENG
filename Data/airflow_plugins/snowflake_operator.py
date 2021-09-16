from typing import Any, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pf_utils.db import constraint_keys_check, get_constraint_keys, get_tables_to_check_pk_from_query


class SnowflakeOperator(BaseOperator):
    """
    Executes sql code in a Snowflake database. There are two ways of using this operator:
    - sql query: you use the sql argument and execute one or several queries
    - incremental insert: you use incr_table and other support arguments and the operator will execute automatically
    an incremental insert based on the stg table of incr_table. First, it will delete the present elements on the table
    that are also in the stg table, using their primary keys. Then, the data in the stg table will be inserted in the
    specified table. Finally, the stg table will or not be dropped.


    :param database: name of database (will overwrite database defined
        in the connection's extra JSON)
    :type database: str
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param snowflake_conn_id: reference to specific snowflake connection id
    :type snowflake_conn_id: str
    :param sql: the sql code to be executed.
    :type sql: Union[str, list]
    :param parameters: the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param incr_table: name of the table to make the incremental insert in. Please note that if your incr_table is
        `table`, there must be an existing table called `stg_table`.
    :type str
    :param schema: name of schema (will overwrite schema defined in connection). If the argument incr_table is used,
        schema must be defined as well.
    :type schema: str
    :param incr_distinct: if this parameter is set to True, the incremental insert will be based on `SELECT DISTINCT *`
        instead of `SELECT *``
    :type incr_distinct: bool
    :param incr_columns: by default, the incremental insert is based on the primary keys, but you can make it based on
        other specific columns using this argument
    :type incr_columns: list
    :param check_constraint_keys: check if constraint keys have duplicated values. If incr_table is used,
        this argument is not necessary because it will use the defined primary keys of the incr_table.
        There is also an automatic process that will try to find tables with INSERT in the sql code. If there is
        any table found, you don't have to pass this argument. Otherwise, you should pass a list of tuples
        with two strings: first the table's schema, followed by the table name.
        For example [('int', 'table_1')
    :type check_constraint_keys: list
    :param constraint_types: table constraints to be checked as string elements of a list. Possible values are
        'primary' and 'unique'.
    :type constraint_types: list
    :param drop_stg_table: whether or not to drop the stg table after the incremental insert. By default is True, but
        if you are debugging, it can be useful to set it to False.
    :param drop_stg_table: bool
    :param autocommit: if True, each command is automatically committed.
    :type autocommit: bool
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :type role: str
    """

    template_fields = (
        "sql",
        "incr_table",
    )
    template_ext = (".sql",)
    ui_color = "#baeeef"

    def __init__(
        self,
        *,
        database: str,
        warehouse: str,
        snowflake_conn_id: str = "data_db_snowflake_airflow",
        sql: Optional[Union[str, list]] = None,
        parameters: Optional[dict] = None,
        incr_table: Optional[str] = None,
        schema: Optional[str] = None,
        incr_distinct: bool = False,
        incr_columns: Optional[list] = None,
        check_constraint_keys: Union[bool, list] = True,
        constraint_types: list = None,
        drop_stg_table: bool = True,
        autocommit: bool = True,
        role: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.database = database
        self.warehouse = warehouse
        self.snowflake_conn_id = snowflake_conn_id
        self.sql = sql
        self.parameters = parameters
        self.incr_table = incr_table
        self.schema = schema
        self.incr_distinct = "DISTINCT" if incr_distinct else ""
        self.incr_columns = incr_columns
        self.check_constraint_keys = check_constraint_keys

        if constraint_types is None:
            self.constraint_types = ["primary", "unique"]
        else:
            self.constraint_types = constraint_types

        self.drop_stg_table = drop_stg_table
        self.autocommit = autocommit
        self.role = role
        self.hook = None

    def get_hook(self) -> None:
        self.hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
        )

    def execute_statement(self, statement: str) -> None:
        self.log.info("Executing: %s", statement)
        if statement.lower().startswith("select"):
            raise AirflowException(
                """You have a SELECT statement in your query, that will potentially generate a huge log 
                and does nothing. Please modify it."""
            )
        else:
            self.hook.run(statement, autocommit=self.autocommit, parameters=self.parameters)

    def execute(self, context: Any) -> None:
        self.get_hook()

        if self.incr_table:
            final_table = f"{self.schema}.{self.incr_table}"
            stg_table = f"{self.schema}.stg_{self.incr_table}"
            self.log.info(f"Starting incremental insert into {final_table}")

            if not self.incr_columns:
                primary_keys_info = get_constraint_keys(
                    self.schema, self.incr_table, ["primary"], self.hook, self.log
                )
                try:
                    primary_keys_string = primary_keys_info[0][2]
                    primary_keys = primary_keys_string.split(", ")
                    self.incr_columns = primary_keys
                except IndexError:
                    raise AirflowException(
                        "The source table has not primary keys and you have not provided any."
                        "Please introduce the argument incr_columns."
                    )

            stg_column = f"{stg_table}.{self.incr_columns[0]}"
            table_column = f"{final_table}.{self.incr_columns[0]}"
            delete_statement = f"""
            DELETE FROM {final_table}
            USING {stg_table}
            WHERE (({table_column} = {stg_column}) OR ({table_column} IS NULL AND {stg_column} IS NULL))
            """

            for column in self.incr_columns[1:]:
                stg_column = f"{stg_table}.{column}"
                table_column = f"{final_table}.{column}"
                delete_statement += f" AND (({table_column} = {stg_column}) OR ({table_column} IS NULL AND {stg_column} IS NULL))"
            delete_statement += ";"

            insert_statement = f"""INSERT INTO {final_table}
                                   SELECT {self.incr_distinct} *, SYSDATE()
                                   FROM {stg_table};"""
            insert_and_delete = delete_statement + insert_statement
            self.execute_statement(insert_and_delete)
            self.log.info(f"Incremental insert to {final_table} finished correctly")

            if self.drop_stg_table:
                drop_stg_statement = f"DROP TABLE IF EXISTS {stg_table}"
                self.execute_statement(drop_stg_statement)
                self.log.info(f"Stg table {stg_table} dropped")

        if self.sql:
            self.execute_statement(self.sql)

        if self.check_constraint_keys:
            if self.incr_table and self.schema:
                constraint_keys_check(
                    schema=self.schema,
                    table=self.incr_table,
                    constraint_types=self.constraint_types,
                    hook=self.hook,
                    log=self.log,
                )
            else:
                try:  # This try-except is because by default, the argument check_constraint_keys is True, but it
                    # can be that you don't have set the constraint keys, the script hasn't detect tables to check pk
                    # in the query, nor use the argument incr_table. Thus, it will try to iterate through the bool
                    # True value of check_constraint_keys and will throw TypeError.
                    if self.check_constraint_keys is True:  # the value is the default True
                        tables_to_check_pk = get_tables_to_check_pk_from_query(self.sql, self.log)
                    else:
                        tables_to_check_pk = self.check_constraint_keys

                    for table_to_check in tables_to_check_pk:
                        schema = table_to_check[0]
                        table = table_to_check[1]
                        constraint_keys_check(
                            schema=schema,
                            table=table,
                            constraint_types=self.constraint_types,
                            hook=self.hook,
                            log=self.log,
                        )
                except TypeError:
                    raise AirflowException(
                        """You have not specified the constraint keys to check and the table(s) don't
                        have them defined. Please define them or disable the constraint keys check."""
                    )
