import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
import random

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["python_op", "슬통생"]
) as dag:
    def select_fruit():
        fruit = ['BANANA', 'APPLE', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0, 3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = "py_t1",
        python_callable = select_fruit
    )

    py_t2 = PythonOperator(
        task_id = "py_t2",
        python_callable = select_fruit
    )

    py_t1 >> py_t2