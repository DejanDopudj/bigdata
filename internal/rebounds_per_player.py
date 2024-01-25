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
    joined_df = df.join(dimension_player, df['Rebounder'] == dimension_player['player_id'], 'inner')


    result_df = (
        joined_df.filter(col("Rebounder").alias("Player") != '')
        .groupBy("Rebounder", "Date", "Season", "URL", "player_name")
        .agg(
            sum(
                when(col("ReboundType") == "defensive", 1).otherwise(0)
            ).alias("defensive_rebounds"),
            sum(
                when(col("ReboundType") == "offensive", 1).otherwise(0)
            ).alias("offensive_rebounds")
        )
        .orderBy(col("defensive_rebounds").desc(), col("offensive_rebounds").desc())
        .withColumn("Rebounder", col("player_name"))
        .drop("player_name")
    )
    write_to_db(result_df, "nba_test_core", "rebounds_per_player3", "overwrite")
    df = read_from_db(job.spark,"nba_test_core", "rebounds_per_player3")
    df.show()


points_per_player()