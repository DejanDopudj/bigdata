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
from pyspark.sql.functions import split
import sys

def extract_player_info(column, spark):
    split_col = split(column, " - ")
    return spark.createDataFrame([(split_col.getItem(1), split_col.getItem(0))], ["player_id", "player_name"])


def points_per_player():
    job = BatchJob()
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_staging", "play_by_play")
    df.show()
    shooter_df = df.select("Shooter").distinct().withColumnRenamed("Shooter", "player_name")
    assister_df = df.select("Assister").distinct().withColumnRenamed("Assister", "player_name")
    rebounder_df = df.select("Rebounder").distinct().withColumnRenamed("Rebounder", "player_name")
    # Union the three DataFrames to get unique player names
    result_df = shooter_df.union(assister_df).union(rebounder_df).distinct()
    result_df.show()
    split_col = split(result_df['player_name'], ' - ')
    result_df = result_df.withColumn('player_id', split_col.getItem(1))
    result_df = result_df.withColumn('player_name', split_col.getItem(0))

    # Show the result
    # result_df.show(truncate=False)
    write_to_db(result_df, "nba_test_fact", "dimension_player2", "overwrite")
    df = read_from_db(job.spark,"nba_test_fact", "dimension_player2")
    df.show(truncate=False)

points_per_player()