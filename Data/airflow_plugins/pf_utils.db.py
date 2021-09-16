import re
import pandas as pd

from airflow.exceptions import AirflowException


def convert_statement_from_redshift_to_snowflake(query):
    # remove commented lines
    new_query = re.sub(r"\n--[^\n]+", "", query, 0, re.IGNORECASE)

    # don't need varchar length
    new_query = re.sub(r"VARCHAR\([^)]+\)", "VARCHAR", new_query, 0, re.IGNORECASE)

    # no compression
    new_query = re.sub(r"ENCODE [^\n\s]+", "", new_query, 0, re.IGNORECASE)
    # no distribution key
    new_query = re.sub(r"DISTSTYLE [^\n]+", "", new_query, 0, re.IGNORECASE)
    new_query = re.sub(r"DISTKEY[^(]*[^)]+\)", "", new_query, 0, re.IGNORECASE)
    # no sort key
    new_query = re.sub(r"SORTKEY[^(]*[^)]+\)", "", new_query, 0, re.IGNORECASE)

    # no analyze
    new_query = re.sub(r"ANALYZE [^;]+;", "", new_query, 0, re.IGNORECASE)

    # SYSDATE is SYSDATE()
    new_query = re.sub(r"([^A-Z]+)SYSDATE([^A-Z]+)", "\\1SYSDATE()\\2", new_query, 0, re.IGNORECASE)

    # ~ is REGEXP
    new_query = new_query.replace("~", "REGEXP")

    # TO_NUMBER defaults to 0 precision
    new_query = re.sub(r"TO_NUMBER\((.+,\s*'9+D9+')\)", "TO_NUMBER(\\1, 38, 3)", new_query, 0, re.IGNORECASE)

    # CREATE TABLE ... (LIKE ...) is CREATE TABLE ... LIKE
    new_query = re.sub(r"\(LIKE([^)]+)\)", " LIKE \\1", new_query, 0, re.IGNORECASE)

    # these 2 are optional
    new_query = new_query.replace("CREATE TABLE IF NOT EXISTS", "CREATE TRANSIENT TABLE IF NOT EXISTS")
    new_query = new_query.replace("CREATE TABLE", "CREATE TRANSIENT TABLE")

    # DROP TABLE cannot receive a list of tables
    matches = re.findall(r"DROP TABLE [^,;]+,[^;]+;", new_query)
    for match in matches:
        drop_tables = match.replace("DROP TABLE ", "").replace("\n", "").replace(";", "").split(",")

        new_drop_tables_stmt = ""
        for table in drop_tables:
            new_drop_tables_stmt += "DROP TABLE " + table.strip() + ";\n"

        new_query = new_query.replace(match, new_drop_tables_stmt)

    # commands need schema name
    commands = r"(DROP TABLE(?! IF EXISTS)|DROP TABLE IF EXISTS|CREATE TABLE(?! IF NOT EXISTS)|CREATE TABLE IF NOT EXISTS|LIKE|INSERT INTO|ALTER TABLE|RENAME TO)"
    new_query = re.sub(commands + r" ([^\s.\']+[\s\n;)])", "\\1 public.\\2", new_query, 0, re.IGNORECASE)

    # for tables that you want to change their name between redshift and snowflake
    dw_tables_ref = [
        {"redshift.table": "snowflake.equivalent"},
    ]

    for table in dw_tables_ref:
        for key in table:
            new_query = new_query.replace(key, table[key])

    return new_query


def get_primary_keys_query(schema, table):
    return f"""
            SELECT
              att.attname
            FROM pg_index ind, pg_class cl, pg_attribute att
            WHERE
              cl.oid = '{schema}."{table}"'::regclass
              AND ind.indrelid = cl.oid
              AND att.attrelid = cl.oid
              and att.attnum = ANY(string_to_array(textin(int2vectorout(ind.indkey)), ' '))
              and attnum > 0
              AND ind.indisprimary
            order by att.attnum;"""


def get_constraint_keys(schema, table, constraint_types, hook, log):
    """
    Returns list of lists with the columns composing the primary/unique keys for the specified schema.table.
    The function is meant to be called by operators working with Snowflake.
    The constraint types allowed are 'primary', 'unique' or both.
    Those correspond to 'PRIMARY KEY' and 'UNIQUE' constraint types in Snowflake.

    :param schema: the name of the schema in Snowflake
    :type schema: str
    :param table: the name of the table in the Snowflake schema
    :type table: str
    :param constraint_types: a list with one or both of the strings 'primary' or 'unique'
    :type constraint_types: list of strings
    :param hook: the airflow hook to establish the connection with database
    :type hook: an airflow hook object
    :param log: the object we need to print logging messages to the console
    :type log:  an airflow log object coming from the BaseOperator
    """
    constraint_dfs = []
    for constraint_type in constraint_types:
        log.info(f'Getting {constraint_type} keys ...')
        constraint_df = hook.get_pandas_df(f'show {constraint_type} keys in table {schema}.{table}')
        constraint_df['constraint_type'] = constraint_type
        constraint_df['column_name'] = '"' + constraint_df['column_name'] + '"'
        constraint_dfs.append(constraint_df)

    constraint_keys_df = pd.concat(constraint_dfs, ignore_index=True)

    constraint_keys_df = constraint_keys_df.groupby(
        ['constraint_type', 'constraint_name']
    )['column_name'].apply(', '.join).reset_index(name='constraint_key')
    constraint_keys = constraint_keys_df.to_numpy().tolist()

    return constraint_keys


def constraint_keys_check(schema, table, constraint_types, hook, log):
    """
    This function is meant to be called by different airflow operators working with Snowflake tables.
    It tests for each constraint key in the specified schema.table if there are duplicated values.
    The constraint types allowed are 'primary', 'unique' or both.
    Those correspond to 'PRIMARY KEY' and 'UNIQUE' constraint types in Snowflake.
    The function raises an airflow exception if the test fails.
    Test fails if any of the constraint keys has at least one duplicated value.

    :param schema: the name of the schema in Snowflake
    :type schema: str
    :param table: the name of the table in the Snowflake schema
    :type table: str
    :param constraint_types: a list with one or both of the strings 'primary' or 'unique'
    :type constraint_types: list of strings
    :param hook: the airflow hook to establish the connection with database
    :type hook: an airflow hook object
    :param log: the object we need to print logging messages to the console
    :type log:  an airflow log object coming from the BaseOperator
    """
    constraint_keys = get_constraint_keys(
        schema=schema,
        table=table,
        constraint_types=constraint_types,
        hook=hook,
        log=log
    )

    if constraint_keys:
        for constraint_key in constraint_keys:
            constraint_type = constraint_key[0]
            key_columns = constraint_key[2]
            log.info(f'Checking {constraint_type} keys with key columns {key_columns}')

            duplicates_query = f"""
            SELECT COUNT(*) FROM 
                (
                SELECT COUNT(*), {key_columns} 
                FROM {schema}.{table}
                GROUP BY {key_columns}
                HAVING COUNT(*) > 1
                )"""
            log.info(duplicates_query)
            duplicates_tuple = hook.get_first(duplicates_query)
            duplicates = int(duplicates_tuple[0])
            if duplicates > 0:
                raise AirflowException(
                    f"\n\nThere are {duplicates} duplicates in the table {schema}.{table}. \
                    \nCheck for {constraint_type} keys failed \
                    \n{duplicates_query}\n")
            else:
                log.info(f"The {constraint_type} keys check has been successful for {schema}.{table}")
    else:
        log.info(f'There are no defined constraint keys for the table {schema}.{table}. Skipping check')


def get_tables_to_check_pk_from_query(query, log):
    """The aim of this function is to automate as maximum as possible the check primary key process. When there is not
    an incremental table insert, the query to insert must be read from an SQL file. This function searches the sql code
    looking for the tables that are going to have data inserted, in order to get its primary keys without needing to
    introduce them manually, so that the PK check is completed successfully automatically"""

    tables_with_new_data = re.findall(r"INSERT INTO ([^(\n ]*)", query)
    tables_with_new_data = list(filter(None, tables_with_new_data))  # Sometimes we get empty strings because of extra
    # spaces that should not be there
    dropped_tables = re.findall(r"DROP TABLE IF EXISTS ([^(\n ;]*)", query)
    dropped_tables = list(filter(None, dropped_tables))

    drop_pos = {}
    insert_pos = {}
    for table in dropped_tables:
        drop_pos[table] = query.rfind(f'DROP TABLE IF EXISTS {table}')
    for table in tables_with_new_data:
        insert_pos[table] = query.find(f'INSERT INTO {table}')

    tables_to_check_pk = [
        re.sub(re.escape('_new') + '$', '', table)
        # so that the check is (i.e) for d_countries instead of d_countries_new
        for table in tables_with_new_data
        if table not in dropped_tables or drop_pos[table] < insert_pos[table]
    ]  # we have to make sure that for not including a table in the list to check, the drop happens after the insert and
    # not before
    tables_to_check_pk = list(set(tables_to_check_pk))

    log_sustantive = 'tables' if len(tables_to_check_pk) > 1 else 'table'
    log_verb = 'are' if len(tables_to_check_pk) > 1 else 'is'
    log.info(f"The {log_sustantive} to check PK {log_verb} {', '.join(tables_to_check_pk)}")

    tables_to_check_pk = [tuple(table.split('.')) for table in tables_to_check_pk]  # for having them
    # as [(schema, table]), as the SnowflakeOperator requires

    return tables_to_check_pk
