import datetime
import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=True,
    tags=["python_task_temp", "example"],
) as dag:

    @task(task_id = "python_task")
    def show_templates(*kwargs):    
        print(kwargs)
    
    python_task = show_templates('task decorator 실행')
