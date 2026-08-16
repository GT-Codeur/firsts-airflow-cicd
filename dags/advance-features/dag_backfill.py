"""
Author: Germain
Descripition: Example DAG to demonstrate Airflow catchup and backfill behavior.
Parameters: None
Return Type: None

This DAG is configured with `catchup=True` and defines a single Python task
that prints a provided message. It's useful for teaching how backfills operate
across a DAG's `start_date` and schedule interval.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default DAG arguments
default_args = {
    'owner': 'Germain',
    'start_date': datetime(2026, 7, 20),  # The date when the DAG starts
}

# Define the DAG
dag = DAG(
    'catchup_and_backfill_dag', # DAG ID
    default_args=default_args, # Default arguments for the DAG
    description='catchup and backfill',
    tags=['Data Engineering courses',"Advanced"],
    catchup = True,
    schedule='30 10 * * *' #  Dag Run = start_date + schedule -> 2026/07/21 at 10:30
)


def print_message(message):
    """
    Author: Germain
    Descripition: Print the provided message to stdout.
    Parameters: message (str) - the message to print
    Return Type: None

    A small helper used by a `PythonOperator` to demonstrate repeated task
    execution during backfill/catchup scenarios.
    """
    print(message)


# Task to print a message for each execution
print_message_task = PythonOperator(
    task_id='print_message',
    python_callable=print_message,
    op_args=['Hello, this is a backfill example!'],
    dag=dag
)
