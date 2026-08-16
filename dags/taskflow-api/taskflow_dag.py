"""
Author: Germain
Descripition: TaskFlow API example showing lightweight task composition.
Parameters: None
Return Type: DAG

Defines a TaskFlow-style DAG composed of Python tasks that pass values
between each other using return values rather than XComs explicitly.
"""

from datetime import datetime
from airflow import DAG
from airflow.decorators import task, dag

# Default arguments for the DAG
default_args = {
    'owner': 'Germain',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'start_date': datetime(2026, 8, 9),
}


@dag(dag_id='taskflow_dag',
     default_args=default_args,
     description='taskflow DAG',
     tags=['Data Engineering courses', "Advanced"],
     catchup=False,
     schedule='0 0 * * *'
     )
def taskflow_dag():
    """Task flow DAG"""

    # First task: simply prints and returns a message
    @task()
    def execute_task1():
        """
        Author: Germain
        Descripition: Print and return a static message for task 1.
        Parameters: None
        Return Type: str

        Returns a short string consumed by downstream tasks.
        """
        print("print - Task 1")
        return "Output of Task 1"

    # Second task: simply prints and returns another message
    @task()
    def execute_task2():
        """
        Author: Germain
        Descripition: Print and return a static message for task 2.
        Parameters: None
        Return Type: str
        """
        print("print - Task 2")
        return "Output of Task 2"

    # Third task: just another print and return
    @task()
    def execute_task3():
        """
        Author: Germain
        Descripition: Print and return a static message for task 3.
        Parameters: None
        Return Type: str
        """
        print("print - Task 3")
        return "Output of Task 3"

    # Fourth task: takes the outputs from task2 and task3, prints them, and returns a message
    @task()
    def execute_task4(input_from_task2, input_from_task3):
        """
        Author: Germain
        Descripition: Consume two inputs from previous tasks and return a message.
        Parameters: input_from_task2 (str), input_from_task3 (str)
        Return Type: str

        Prints the inputs received from `execute_task2` and `execute_task3` and
        returns a short string for downstream consumption.
        """
        print(f"print - Task 4, Inputs: {input_from_task2}, {input_from_task3}")
        return "Output of Task 4"

    # Final task: takes the output from task4 and writes it into a text file
    @task()
    def create_txt_file(input_from_task4):
        """
        Author: Germain
        Descripition: Write the provided input to a local text file.
        Parameters: input_from_task4 (str)
        Return Type: None

        Persists a small artifact `sample_taskflow.txt` containing the
        `input_from_task4` value. File path is under `/opt/airflow/dags/taskflow-api`.
        """
        with open(
            '/opt/airflow/dags/taskflow-api/sample_taskflow.txt',
            'w', encoding='utf-8') as file:
            file.write(f"Ceci est un fichier test. Input: {input_from_task4}")

    # Initiate tasks
    task1_output = execute_task1()
    task2_output = execute_task2()
    task3_output = execute_task3()
    task4_output = execute_task4(task2_output, task3_output)
    create_txt = create_txt_file(task4_output)

    # Define the task dependencies:
    # task1_output runs before task2_output and task3_output
    # Both task2_output and task3_output run before task4_output
    # task4_output runs before create_txt_file
    task1_output >> [task2_output, task3_output] >> task4_output >> create_txt


# Instantiate the DAG
DAG_INSTANCE = taskflow_dag()
