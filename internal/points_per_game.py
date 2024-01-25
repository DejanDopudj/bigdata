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

def points_per_game():
    job = BatchJob()
    df = read_from_db(job.spark,"nba_test_core", "points_per_player")
    df.show()
    result_df = (
        df.groupBy("Shooter", "Season")
        .agg(avg("Points").alias("AveragePointsPerGame"))
        .orderBy(col("AveragePointsPerGame").desc())
    )
    write_to_db(result_df, "nba_test_core", "points_per_game")
    result_df.show()


points_per_game()