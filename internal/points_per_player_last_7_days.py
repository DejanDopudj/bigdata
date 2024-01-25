from pyspark.sql import SparkSession
from pyspark.sql import Row,Window
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
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_core", "points_per_player4")
    
    window_spec = Window().partitionBy("Shooter").orderBy(col("Date").desc()).rowsBetween(-6, 0)

    # Add a new column for the sum of points in the last 7 days
    result_df = df.withColumn("Points_Last_7_Days", sum("Points").over(window_spec)).select("Shooter", "URL", "Points_Last_7_Days")

    # Show the result
    result_df.show()

points_per_player()