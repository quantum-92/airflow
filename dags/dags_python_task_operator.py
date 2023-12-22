import datetime
import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["python_task_op", "example"],
) as dag:

    @task(task_id = "python_task_1")
    def print_context(some_input):    
        print(some_input)
    
    python_task_1 = print_context('task decorator 실행')
