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
    points_df = read_from_db(job.spark,"nba_test_core", "points_per_player3")
    rebounds_df = read_from_db(job.spark,"nba_test_core", "rebounds_per_player3")
    assists_df = read_from_db(job.spark,"nba_test_core", "assists_per_player3")
    joined_df = points_df.alias("p").join(rebounds_df, (points_df["Shooter"] == rebounds_df["Rebounder"]) & (points_df["URL"] == rebounds_df["URL"]), "inner") \
                        .join(assists_df, (points_df["Shooter"] == assists_df["Assister"]) & (points_df["URL"] == assists_df["URL"]), "inner")
    result_df = (
        joined_df.groupBy("p.Shooter", "p.Date", "p.HomeTeam", "p.AwayTeam", "p.Season")
        .agg(
            avg("p.Points").alias("avg_points"),
            avg("defensive_rebounds").alias("avg_defensive_rebounds"),
            avg("offensive_rebounds").alias("avg_offensive_rebounds"),
            avg("assists").alias("avg_assists")
        )
    )
    final_df = result_df.select(col("Shooter").alias("Player"), col("Date"), col("HomeTeam"), col("AwayTeam"), col("Season"), col("avg_points"), col("avg_defensive_rebounds")
                                , col("avg_offensive_rebounds"), col("avg_assists"))
    write_to_db(final_df, "nba_test_core", "stats_per_game")
    result_df.show()


points_per_game()