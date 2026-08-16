"""
Author: Germain
Descripition: Demonstrates grouping tasks using Airflow `TaskGroup`.
Parameters: None
Return Type: None

This DAG shows how to organize related tasks into a `TaskGroup` and set up
dependencies. It contains simple BashOperators for illustrative purposes.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

# Default DAG arguments
default_args = {
    'owner': 'Germain',
    'start_date': datetime(2026, 8, 9),  # The date when the DAG starts
}

# Define the DAG
dag = DAG(
    'taskgroup_dag',             # DAG ID
    default_args=default_args,     # Default arguments for the DAG
    description='taskgroup DAG',
    tags=['Data Engineering courses',"Advanced"],
    # If set to True,
    # Airflow will execute all instances between the DAG's start_date and the current day.
    # Setting to False means skipping missed instances.
    catchup=False,
    schedule='@daily'  # Schedule interval (run at midnight every day)
)


# Define tasks using BashOperator
task1 = BashOperator(
    task_id='task1',                      # Task ID
    bash_command='echo "Task 1"',         # Bash command to be executed
    dag=dag                               # Assign the task to the DAG
)

# Using the TaskGroup context manager.
with TaskGroup("grouped_tasks", dag=dag) as tg:
    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "Task 2"',
        dag=dag
    )

    task3 = BashOperator(
        task_id='task3',
        bash_command='echo "Task 3"',
        dag=dag
    )

    task4 = BashOperator(
        task_id='task4',
        bash_command='echo "Task 4"',
        dag=dag
    )

    GROUP_DAG_FLOW = [task2, task3] >> task4

task5 = BashOperator(
    task_id='Create_txt_file',
    # Create a txt file with text
    bash_command="""
    echo "Ceci est un fichier test" > /opt/airflow/dags/advance-features/sample_bis.txt
    """,
    dag=dag
)

# Define the task dependencies
DAG_FLOW = task1 >> tg >> task5
