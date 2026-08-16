"""
Author: Germain
Descripition: Create Postgres table aggregating Nintendo game sales by year.
Parameters: None
Return Type: None

Provides a helper function that builds `agg_nintendo_year_sales` selecting rows
from `sales` where `publisher` is 'Nintendo' and producing yearly aggregates.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook


def staging_agg_nintendo_year_sales():
    """
    Author: Germain
    Descripition: Create or replace `agg_nintendo_year_sales` table in Postgres.
    Parameters: None
    Return Type: None

    Executes SQL against the `postgres_connection` to recreate the aggregation
    table for Nintendo-published games, computing yearly Global_Sales totals.
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
    postgres_hook.run("""
        DROP TABLE IF EXISTS agg_nintendo_year_sales;
        CREATE TABLE agg_nintendo_year_sales AS
        SELECT name_game
                        , year_game
                        , Global_Sales
                        , sum(Global_Sales) OVER (PARTITION BY year_game) AS year_Global_Sales
                        FROM sales
                        WHERE publisher = 'Nintendo'
                        ORDER BY year_game DESC
                """)


if __name__ == "__main__":
    staging_agg_nintendo_year_sales()
