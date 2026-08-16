"""
Author: Germain
Descripition: Create Postgres table aggregating Electronic Arts game sales by year.
Parameters: None
Return Type: None

Contains a single function to create `agg_ea_year_sales` by selecting and
aggregating rows from the `sales` table where `publisher` is 'Electronic Arts'.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook


def staging_agg_ea_year_sales():
    """
    Author: Germain
    Descripition: Create or replace `agg_ea_year_sales` table in Postgres.
    Parameters: None
    Return Type: None

    Uses the `postgres_connection` hook to execute SQL that drops and recreates
    the aggregation table for Electronic Arts games, computing yearly totals.
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
    postgres_hook.run("""
        DROP TABLE IF EXISTS agg_ea_year_sales;
        CREATE TABLE agg_ea_year_sales AS
        SELECT name_game
                        , year_game
                        , Global_Sales
                        , sum(Global_Sales) OVER (PARTITION BY year_game) AS year_Global_Sales
                        FROM sales
                        WHERE publisher = 'Electronic Arts'
                        ORDER BY year_game DESC
                """)


if __name__ == "__main__":
    staging_agg_ea_year_sales()
