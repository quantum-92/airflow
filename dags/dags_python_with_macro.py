import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro_1",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"]
) as dag:
    
    @task(task_id = "dags_python_with_macro_1",
          template_dict = {'start_date': '{{(data_interval_start.to_timezone("Asia/Seoul") + datetimeutlils.relativedelta.relativedelta(months = -1, day = 1)) | ds}}',
                           'end_date': '{{data_interval_end.to_timezone("Asia/Seoul") + datetimeutlils.relativedelta.relativedelta(days = -1)) | ds}}'})
    def datetime_func1(**kwargs):
        template_dict = kwargs['template_dict'] or {}
        start_date = template_dict['start_date'] or "no start_date"
        end_date = template_dict['end_date'] or "no end_date"
        print(start_date)
        print(end_date)
    
    @task(task_id = "dags_python_with_macro_2")
    def datetime_func2(**kwargs):
        from dateutil.relativedelta import relativedelta
        
        date_time_interval_end = kwargs['date_interval_end'].to_timzone("Asia/Seoul")
        start_date = date_time_interval_end + relativedelta(months = -1, day = 1)
        end_date = date_time_interval_end + relativedelta(days = -1)
        print(start_date.strftime("%Y-%m-%d"))
        print(end_date.strftime("%Y-%m-%d"))
        
    datetime_func1() >> datetime_func2()
