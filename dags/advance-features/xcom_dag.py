"""
Author: Germain
Descripition: Example DAG demonstrating XCom push and pull between tasks.
Parameters: None
Return Type: None

Defines a simple DAG with two Python tasks: one pushes a string into XCom and
the other pulls and prints that value. Useful for learning how XComs work in
Airflow task-to-task communication.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the DAG.
# These are common configurations that apply to the DAG and its tasks.
default_args = {
    'owner': 'Germain',                          # Owner of the DAG
    'start_date': datetime(2026, 8, 8),          # Start date for the DAG
}

# Instantiate a DAG. This represents a collection of tasks that run on a schedule.
dag = DAG(
    'xcom_example_dag',                          # Unique identifier for the DAG
    default_args=default_args,                   # Apply the default arguments
    description='XCom DAG',   # Description of the DAG's purpose
    tags=['Data Engineering courses',"Advanced"],
    schedule='@daily',                  # How often to run the DAG. '@daily' means once a day.
    # If set to True,
    # Airflow will execute all instances between the DAG's start_date and the current day.
    # Setting to False means skipping missed instances.
    catchup=False
)


def push_xcom_value(**kwargs):
    """
    Author: Germain
    Descripition: Push a sample string value to XCom under `sample_key`.
    Parameters: kwargs (dict) - Airflow context, expects task instance `ti`.
    Return Type: None

    Uses `ti.xcom_push` to push a predefined string so another task can
    retrieve it with `xcom_pull`.
    """
    value_to_push = "This is the pushed value!"
    # Using the xcom_push method to push a value to XCom
    kwargs['ti'].xcom_push(key='sample_key', value=value_to_push)


push_task = PythonOperator(
    task_id='push_task',  # Unique identifier for this task
    python_callable=push_xcom_value, # Python function to be executed by this task
    provide_context=True, # This ensures the function gets the necessary keyword arguments like 'ti'
    dag=dag  # Link this task to the previously defined DAG
)


def pull_xcom_value(**kwargs):
    """
    Author: Germain
    Descripition: Pull a value from XCom pushed by `push_task` and print it.
    Parameters: kwargs (dict) - Airflow context, expects task instance `ti`.
    Return Type: None

    Retrieves the value stored under key `sample_key` from the `push_task`
    execution and prints it to standard output.
    """
    ti = kwargs['ti'] # Extract the task instance from the provided kwargs
    # Use xcom_pull to retrieve the value from XCom
    pulled_value = ti.xcom_pull(task_ids='push_task', key='sample_key')
    print(f"Pulled Value from XCom: {pulled_value}")


pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_xcom_value,
    provide_context=True,
    dag=dag
)

# Setting the order in which the tasks will execute.
# push_task will execute first, followed by pull_task, and finally bash_task.
DAG_FLOW = push_task >> pull_task
