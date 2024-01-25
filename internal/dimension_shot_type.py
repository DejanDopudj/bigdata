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


def points_per_player():
    job = BatchJob()
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_staging", "play_by_play")
    df.show()
    result_df = (
        df.select("ShotType")
        .distinct()
        .withColumn("shot_type", col("ShotType"))
        .withColumn("shot_worth", when(col("ShotType").contains("3-pt"), 3).otherwise(2))
        .filter(col("ShotType") != '')
        .drop("ShotType")
    )

    # Show the result
    # result_df.show(truncate=False)
    write_to_db(result_df, "nba_test_fact", "dimension_shot_type", "overwrite")
    df = read_from_db(job.spark,"nba_test_fact", "dimension_shot_type")
    df.show()

points_per_player()