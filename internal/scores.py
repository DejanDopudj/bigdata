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
    end_game_df = df.filter(col("AwayPlay") == "End of Game").select(
        "URL", "Date", "Season", "HomeTeam", "AwayTeam", "HomeScore", "AwayScore"
    )
    write_to_db(end_game_df, "nba_test_core", "scores")
    end_game_df.show()


points_per_game()