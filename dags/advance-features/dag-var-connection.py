from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

default_args = {
    "owner": "Germain",
    "start_date": datetime(2026, 8, 8)
}

dag = DAG(
    "variables_connections_dag",
    default_args=default_args,
    description="A simple DAG that uses Variables and Connections",
    tags=["Data Engineering courses", "Advanced"],
    catchup=False,
    schedule="@daily"
)

# Define a Variable that can be used across tasks in this DAG
example_variable = Variable.get("variable_test_airflow")

# Define a Connection that represents the database connection to be used
database_connection_id = "postgres_connection"

# Task 1: Print the value of the example_variable
def print_variable():
    print(f"Example Variable Value: {example_variable}")

task_print_variable = PythonOperator(
    task_id="print_variable",
    python_callable=print_variable,
    dag=dag
)

task_execute_query = BashOperator(
    task_id="execute_query",
    bash_command=f"airflow connections get {database_connection_id}",
    dag=dag
)

task_print_variable >> task_execute_query