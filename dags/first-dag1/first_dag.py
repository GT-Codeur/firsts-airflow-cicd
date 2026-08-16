"""
Author: Germain
Descripition: Simple beginner DAG demonstrating basic task dependencies.
Parameters: None
Return Type: None

Contains several BashOperator tasks that echo short messages and create a
sample text file. Intended as an introductory example for DAG authors.
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Define argument for the DAG
default_args = {
    "owner": "germain",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "start_date": datetime(2026, 7, 29)
}

# Define the DAG
dag = DAG(
    "my_first_DAG",
    default_args=default_args,
    description="Training DAG",
    tags=["Data engineering courses", "Beginner"],
    catchup=False,
    schedule="0 0 * * *"
)

# Define task using BashOperator
task1 = BashOperator(
    task_id="task1",
    bash_command="echo \"Task 1\"",
    dag=dag
)

task2 = BashOperator(
    task_id="task2",
    bash_command="echo \"Task 2\"",
    dag=dag
)

task3 = BashOperator(
    task_id="task3",
    bash_command="echo \"Task 3\"",
    dag=dag
)

task4 = BashOperator(
    task_id="task4",
    bash_command="echo \"Task 4\"",
    dag=dag
)

task5 = BashOperator(
    task_id="Create_txt_file",
    bash_command='echo "Ceci est un fichier test" > /opt/airflow/dags/sample.txt',
    dag=dag
)

# Define the dependencies
dag_flow = task1 >> [task2, task3] >> task4 >> task5
