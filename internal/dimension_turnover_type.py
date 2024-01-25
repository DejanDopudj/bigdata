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
    dimension_turnover_type_df = (
        df.select("TurnoverType")
        .distinct()
        .withColumnRenamed("TurnoverType", "turnover_type_name")
    )

    # Add a unique turnover_type_id column
    result_df = dimension_turnover_type_df.withColumn("turnover_type_id", col("turnover_type_name"))

    # Show the result
    # result_df.show(truncate=False)
    write_to_db(result_df, "nba_test_fact", "dimension_turnover_type", "overwrite")
    df = read_from_db(job.spark,"nba_test_fact", "dimension_turnover_type")
    df.show(truncate=False)

points_per_player()