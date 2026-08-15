from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'Germain',
    'start_date': datetime(2026, 8, 2),  # The date when the DAG starts
}

# Define the DAG
dag = DAG(
    'ELT_analytics_video_games',             # DAG ID
    default_args=default_args,     # Default arguments for the DAG
    description='Analytics ELT',
    tags=['Data Engineering courses',"Intermediate"],
    catchup= False,              # Do not backfill past runs when DAG is created
    schedule='0 0 * * *'  # Schedule interval (run at midnight every day)
)

# The start point of the DAG, represented by a DummyOperator
start = DummyOperator(
    task_id='start',
    dag=dag
)

clean_destination_postgresql = SQLExecuteQueryOperator(
    task_id='clean_destination_postgresql',
    sql=""" TRUNCATE TABLE sales;""",
    conn_id='postgres_connection',
    dag=dag
)

# The first task, extract and load data from MySQL to Postgres, represented by a BashOperator
extract_load_mysql_to_postgre = BashOperator(
    task_id='extract_mysql',
    bash_command='python /opt/airflow/dags/ELT-mysql-postgres/python_tasks/extract_load_mysql_to_postgre.py',
    dag=dag
)

# The second task, perform an aggregation of EA year sale data, represented by a BashOperator
agg_ea_year_sale = BashOperator(
    task_id='agg_ea_year_sale',
    bash_command='python /opt/airflow/dags/ELT-mysql-postgres/python_tasks/agg_ea_year_sale.py',
    dag=dag
)

# The third task, perform an aggregation of Nintendo year sales data, represented by a BashOperator
agg_nintendo_year_sales = BashOperator(
    task_id='agg_nintendo_year_sales',
    bash_command='python /opt/airflow/dags/ELT-mysql-postgres/python_tasks/agg_nintendo_year_sales.py',
    dag=dag
)

# The fourth task, perform an aggregation of Ubisoft year sales data, represented by a BashOperator
agg_ubisoft_year_sales = BashOperator(
    task_id='agg_ubisoft_year_sales',
    bash_command='python /opt/airflow/dags/ELT-mysql-postgres/python_tasks/agg_ubisoft_year_sales.py',
    dag=dag
)

# The end point of the DAG, represented by a DummyOperator
stop = DummyOperator(
    task_id='stop',
    dag=dag
)

start >> clean_destination_postgresql >> extract_load_mysql_to_postgre >> [agg_ea_year_sale, agg_nintendo_year_sales, agg_ubisoft_year_sales] >> stop