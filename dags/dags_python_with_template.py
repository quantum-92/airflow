import datetime
import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 12, 15, tz="Asia/Seoul"),
    catchup=False,
    tags=["python_task_temp", "example"],
) as dag:

    def python_func1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)
        
    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = python_func1,
        op_kwargs = {'start_date': '{{data_interval_start}}',
                     'end_date': '{{data_interval_end}}'}
    )
    
    @task(task_id = "python_t2")
    def python_func2(*kwargs):    
        print(kwargs)
        print('ds:', kwargs['ds'])
        print('ts: ', kwargs['ts'])
        print('ds_interval_start: ', str(kwargs['data_interval_start']))
        print('ds_interval_end: ', str(kwargs['data_interval_end']))
        print('task_instance: ', kwargs['ti'])
        
    python_t1 >> python_func2()
