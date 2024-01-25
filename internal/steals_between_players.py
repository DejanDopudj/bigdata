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
    df = read_from_db(job.spark, "nba_test_staging", "play_by_play")
    result_df = df.select("TurnoverPlayer", "TurnoverCauser", "TurnoverCause")\
        .filter(col("TurnoverCause") == "steal")\
        .groupBy("TurnoverPlayer", "TurnoverCauser")\
        .agg(count("TurnoverCause").alias("count"))
    write_to_db(result_df, "nba_test_core", "steals_between_players")
    result_df.show()


points_per_game()