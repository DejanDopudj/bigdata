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
    
    end_task = EmptyOperator(
        task_id='end'
)
    points_per_player = SparkSubmitOperator(
        task_id='points_per_player',
        conn_id="spark_default",
        application='/user/local/spark/app/points_per_player.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    rebounds_per_player = SparkSubmitOperator(
        task_id='rebounds_per_player',
        conn_id="spark_default",
        application='/user/local/spark/app/rebounds_per_player.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    assists_per_player = SparkSubmitOperator(
        task_id='assists_per_player',
        conn_id="spark_default",
        application='/user/local/spark/app/assists_per_player.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    stats_per_game = SparkSubmitOperator(
        task_id='stats_per_game',
        conn_id="spark_default",
        application='/user/local/spark/app/stats_per_game.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    first_points_per_game = SparkSubmitOperator(
        task_id='first_points_per_game',
        conn_id="spark_default",
        application='/user/local/spark/app/first_points_scored.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    percentage_of_points_per_quarter_per_team_per_game = SparkSubmitOperator(
        task_id='percentage_of_points_per_quarter_per_team_per_game',
        conn_id="spark_default",
        application='/user/local/spark/app/percentage_of_points.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    scores = SparkSubmitOperator(
        task_id='scores',
        conn_id="spark_default",
        application='/user/local/spark/app/scores.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    seasonal_points_per_quarter = SparkSubmitOperator(
        task_id='seasonal_points_per_quarter',
        conn_id="spark_default",
        application='/user/local/spark/app/seasonal_per_quarter.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    steals_between_players = SparkSubmitOperator(
        task_id='steals_between_players',
        conn_id="spark_default",
        application='/user/local/spark/app/steals_between_players.py',
        conf={"spark.master":spark_master},
        jars=jar1+","+jar2+","+jar3,
    )
    

wait_for_raw_ingestion >> [start_task >> points_per_player >> rebounds_per_player >> assists_per_player >> stats_per_game >> 
                           first_points_per_game >> percentage_of_points_per_quarter_per_team_per_game >>
                             scores >> seasonal_points_per_quarter >>  steals_between_players >> end_task]
