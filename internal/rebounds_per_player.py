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
    df = read_from_db(job.spark, "nba_test_staging", "play_by_play")
    result_df = (
        df.filter(col("Rebounder").alias("Player") != '')
        .groupBy("Rebounder", "Date", "HomeTeam", "AwayTeam", "Season", "URL")
        .agg(
            sum(
                when(col("ReboundType") == "defensive", 1).otherwise(0)
            ).alias("defensive_rebounds"),
            sum(
                when(col("ReboundType") == "offensive", 1).otherwise(0)
            ).alias("offensive_rebounds")
        )
        .orderBy(col("defensive_rebounds").desc(), col("offensive_rebounds").desc())
    )
    write_to_db(result_df, "nba_test_core", "rebounds_per_player3")
    df = read_from_db(job.spark,"nba_test_core", "rebounds_per_player3")
    df.show()


points_per_player()