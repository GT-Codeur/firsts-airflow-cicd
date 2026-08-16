"""
Author: Germain
Descripition: Example DAG demonstrating the use of Airflow Variables and
Connections within tasks.
Parameters: None
Return Type: None

Provides a simple DAG which reads an Airflow Variable and demonstrates using
the `airflow` connections get` command to interact with stored connections.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

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
DATABASE_CONNECTION_ID = "postgres_connection"


def print_variable():
    """
    Author: Germain
    Descripition: Print the value of a pre-configured Airflow Variable.
    Parameters: None
    Return Type: None

    The function retrieves the value of `example_variable` (read at module
    import time) and prints it to standard output. Used by a `PythonOperator`
    inside the DAG for demonstration purposes.
    """
    print(f"Example Variable Value: {example_variable}")


task_print_variable = PythonOperator(
    task_id="print_variable",
    python_callable=print_variable,
    dag=dag
)

task_execute_query = BashOperator(
    task_id="execute_query",
    bash_command=f"airflow connections get {DATABASE_CONNECTION_ID}",
    dag=dag
)

dag_flow = task_print_variable >> task_execute_query
