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
        df.filter(col("Shooter").isNotNull())
        .groupBy("Shooter", "Date", "HomeTeam", "AwayTeam", "Season")
        .agg(
            sum(
                when((col("ShotType") == 3) & (col("ShotOutcome") == "make"), 3)
                .when((col("ShotType") == 2) & (col("ShotOutcome") == "make"), 2)
                .otherwise(0)
            ).alias("Points")
        )
        .orderBy(col("Points").desc())
    )
    write_to_db(result_df, "nba_test_core", "points_per_player")
    df = read_from_db(job.spark,"nba_test_core", "points_per_player")
    df.show()


points_per_player()