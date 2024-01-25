from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col,
    regexp_extract,
    when,
    sum,
    concat_ws,
    year,
    month,
    count,
    avg,
)
import csv
from spark import BatchJob
from util import read_from_db, write_to_db, convert_to_date
from airflow.operators.python import get_current_context
import sys

def points_per_player():
    job = BatchJob()
    df = read_from_db(job.spark,"nba_test_core", "points_per_player")
    df.show()
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_fact", "play_by_play")
    dimension_player = read_from_db(job.spark, "nba_test_fact", "dimension_player")
    joined_df = df.join(dimension_player, df['Assister'] == dimension_player['player_id'], 'inner')

    result_df = (
        joined_df.filter(col("Assister").alias("Player") != '')
        .groupBy("Assister", "Date", "Season", "URL", "Player_name")
        .agg(
            sum(when(col("Assister") != '', 1).otherwise(0)).alias("assists")
        )
        .orderBy(col("assists").desc())
        .withColumn("Assister", col("player_name"))
        .drop("player_name")
    )
    result_df.show()
    write_to_db(result_df, "nba_test_core", "assists_per_player3", "overwrite")
    df = read_from_db(job.spark,"nba_test_core", "assists_per_player3")
    df.show()


points_per_player()