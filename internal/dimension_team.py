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
    # df.show()
    away_team_df = df.select("AwayTeam").distinct().withColumnRenamed("AwayTeam", "team_name")
    home_team_df = df.select("HomeTeam").distinct().withColumnRenamed("HomeTeam", "team_name")
    # Union the three DataFrames to get unique player names
    result_df = away_team_df.union(home_team_df).distinct()
    result_df.show()

    # Show the result
    # result_df.show(truncate=False)
    write_to_db(result_df, "nba_test_fact", "dimension_team", "overwrite")
    df = read_from_db(job.spark,"nba_test_fact", "dimension_team")
    df.show()

points_per_player()