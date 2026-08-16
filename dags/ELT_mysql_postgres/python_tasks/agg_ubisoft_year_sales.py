"""
Author: Germain
Descripition: Module to create a Postgres table aggregating Ubisoft game sales by year.
Parameters: None
Return Type: None

This module contains a single task function that connects to Postgres using
Airflow's PostgresHook and creates a table `agg_ubisoft_year_sales` with
aggregated sales per year for games published by Ubisoft.
"""

from airflow.providers.postgres.hooks.postgres import PostgresHook


def staging_agg_ubisoft_year_sales():
    """
    Author: Germain
    Descripition: Create or replace `agg_ubisoft_year_sales` table in Postgres.
    Parameters: None
    Return Type: None

    Connects to the Postgres database using the connection id `postgres_connection`
    and runs SQL to drop if exists then create `agg_ubisoft_year_sales` selecting
    aggregated Global_Sales per `year_game` for publisher 'Ubisoft'.
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connection')
    postgres_hook.run("""
        DROP TABLE IF EXISTS agg_ubisoft_year_sales;
        CREATE TABLE agg_ubisoft_year_sales AS
        SELECT name_game
                        , year_game
                        , Global_Sales
                        , sum(Global_Sales) OVER (PARTITION BY year_game) AS year_Global_Sales 
                        FROM sales
                        WHERE publisher = 'Ubisoft'
                        ORDER BY year_game DESC
                """)


if __name__ == "__main__":
    staging_agg_ubisoft_year_sales()
