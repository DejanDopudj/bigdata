from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import datetime as dt
from airflow.decorators import task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor

spark_master = "spark://spark:7077"
jar1 = '/usr/local/spark/resources/mongo-spark-connector_2.12-10.2.1-all.jar'
jar2 = '/usr/local/spark/resources/spark-avro_2.12-3.3.0.jar'
jar3 = '/usr/local/spark/resources/spark-sql-kafka-0-10_2.12-3.3.0.jar'

with DAG(
    'fact_dag',
    start_date=datetime(2022, 5, 28),
    schedule_interval= dt.timedelta(minutes=400000)
) as dag:
    wait_for_raw_ingestion = ExternalTaskSensor(
        task_id="wait_for_staging_ingestion",
        retries=2,
        external_dag_id="staging_ingestion_dag",
    )
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
    testtt = SparkSubmitOperator(
        task_id='test',
        conn_id="spark_default",
        application='/user/local/spark/app/points_per_player.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    
    points_per_game = SparkSubmitOperator(
        task_id='points_per_game',
        conn_id="spark_default",
        application='/user/local/spark/app/points_per_game.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    

wait_for_raw_ingestion >> start_task >> print_hello_world >> testtt >> points_per_game >> end_task
