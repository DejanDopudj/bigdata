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
    row_number,
    asc,
    desc
)
from pyspark.sql.window import Window
import csv
from spark import BatchJob
from util import read_from_db, write_to_db, convert_to_date
from airflow.operators.python import get_current_context
import sys

def first_points_per_game():
    job = BatchJob()
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_staging", "play_by_play")
    made_shots_CTE = (
        df.select(col("Shooter"), col("ShotType"), col("URL"), col("HomeScore"), col("AwayScore"))
        .filter((col("Shooter") != '') & (col("ShotOutcome") == "make"))
    )
    ranked_players_CTE = ( # u jos bar dva upita
         made_shots_CTE.select(col("Shooter"), col("ShotType"), row_number().over(Window.partitionBy("URL").orderBy(["HomeScore", "AwayScore"])).alias("Rank"))
         .orderBy(col("Rank").asc())
    )
    last_df = (
         ranked_players_CTE.select(col("Shooter"), col("ShotType")).filter(col("Rank") == 1)
    )

    last_df.show()
    write_to_db(last_df, "nba_test_core", "first_points_per_game")
    df = read_from_db(job.spark,"nba_test_core", "first_points_per_game")
    df.show()


first_points_per_game()#