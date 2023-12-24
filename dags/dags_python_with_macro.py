import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 12, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["example", "example2"]
) as dag:
    
    @task(task_id = "task_using_macro",
          templates_dict = {'start_date': '{{(data_interval_start.in_timezone("Asia/Seoul") + macros.datetimeutlils.relativedelta.relativedelta(months = -1, day = 1)) | ds}}',
                           'end_date': '{{data_interval_end.in_timezone("Asia/Seoul") + macros.datetimeutlils.relativedelta.relativedelta(days = -1)) | ds}}'})
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs['template_dict'] or {}
        start_date = templates_dict['start_date'] or "no start_date"
        end_date = templates_dict['end_date'] or "no end_date"
        print(start_date)
        print(end_date)
    
    @task(task_id = "task_direct_calc")
    def get_datetime_direct(**kwargs):
        from dateutil.relativedelta import relativedelta
        
        data_interval_end = kwargs['data_interval_end'].in_timzone("Asia/Seoul")
        start_date = data_interval_end + relativedelta(months = -1, day = 1)
        end_date = data_interval_end + relativedelta(day = 1) + relativedelta(months = -1)
        print(start_date.strftime("%Y-%m-%d"))
        print(end_date.strftime("%Y-%m-%d"))
        
    get_datetime_macro() >> get_datetime_direct()
