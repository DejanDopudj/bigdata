from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import datetime as dt
from lib.spark import BatchJob
from airflow.decorators import task


@task
def test():
    job = BatchJob()
    job.daily_ingest(f"01-03-2017", f"2017-03-01")

with DAG(
    dag_id='first_sample_dag',
    start_date=datetime(2022, 5, 28),
    schedule_interval= dt.timedelta(minutes=300000)
) as dag:
    start_task = EmptyOperator(
        task_id='start'
)
    print_hello_world = BashOperator(
        task_id='print_hello_world',
        bash_command='echo "HelloWorld!"'
)
    end_task = EmptyOperator(
        task_id='end'
)
start_task >> test() >> end_task