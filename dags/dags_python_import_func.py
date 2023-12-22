import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["python_import_op", "슬통생"]
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id = "task_get_sftp",
        python_callable = get_sftp
    )
