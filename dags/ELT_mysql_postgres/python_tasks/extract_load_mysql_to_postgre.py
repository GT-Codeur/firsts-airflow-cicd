"""
Author: Germain
Descripition: Extract data from MySQL source and load into Postgres `sales` table.
Parameters: None
Return Type: None

Provides helper functions used by the ELT DAG: `extract_mysql` reads records
from the MySQL source, `generate_random_timestamp` returns a pseudo-random
timestamp used to populate a date field, and `load_postgres` inserts the
records into the Postgres `sales` table.
"""

import logging
import random
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def extract_mysql():
    """
    Author: Germain
    Descripition: Retrieve all records from the MySQL source table.
    Parameters: None
    Return Type: list of tuples

    Connects to MySQL using the connection id `mysql_connection` and returns
    the rows from `db_videos_games_source.sales` as a list of tuples.
    """
    mysql_hook = MySqlHook(mysql_conn_id='mysql_connection')
    records = mysql_hook.get_records('SELECT * FROM db_videos_games_source.sales')
    return records


def generate_random_timestamp():
    """
    Author: Germain
    Descripition: Generate a pseudo-random timestamp within January 2024.
    Parameters: None
    Return Type: datetime

    Used to append a timestamp to imported records so downstream tables have a
    non-null date-like field. The function returns a `datetime` between
    2024-01-01 and 2024-01-31 inclusive.
    """
    base_date = datetime(2024, 1, 1)
    random_days = random.randint(0, 30)  # Generates random days between 0 and 30
    return base_date + timedelta(days=random_days)


def load_postgres(records):
    """
    Author: Germain
    Descripition: Insert records into the Postgres `sales` table.
    Parameters: records (list of tuples) - rows retrieved from MySQL
    Return Type: None

    For each record, append a generated timestamp and execute an `INSERT`
    statement against the Postgres connection `postgres_connection`.
    """
    logging.getLogger().setLevel(logging.WARNING)
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
    for record in records:
        # Append random timestamp to each record
        record_with_timestamp = (*record, generate_random_timestamp())
        postgres_hook.run(
            """INSERT INTO sales VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
            parameters=record_with_timestamp
        )


if __name__ == "__main__":
    source = extract_mysql()
    load_postgres(source)
