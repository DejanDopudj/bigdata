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
    
    points_column = when((col("ShotType") == 3) & (col("ShotOutcome") == "make"), 3) \
    .when((col("ShotType") == 2) & (col("ShotOutcome") == "make"), 2) \
    .when((col("FreeThrowOutcome") == "make"), 1)\
    .otherwise(0)

    score_team_column = when(col("HomePlay") != '', col("HomeTeam")) \
    .when(col("AwayPlay") != '', col("AwayTeam"))

    result_df = (
        df.select("URL", "GameType", "Quarter", score_team_column.alias("Team"), points_column.alias("Points"), "Season")
        .groupBy("URL", "GameType", "Quarter", "Team")
        .agg(sum("Points").alias("TotalPointsPerQuarter"))
        .orderBy("URL", "GameType", "Quarter", "Team")
    )

    end_game_info = (
        df.filter(col("AwayPlay") == "End of Game")
        .select("URL", "AwayTeam", "AwayScore", "HomeTeam", "HomeScore", "Season")
    )
    result_df_with_end_game_info = result_df.join(
        end_game_info,
        on=["URL"],
        how="inner"
    )

    result_df_with_percentages = result_df_with_end_game_info.withColumn(
        "Percentage",
        col("TotalPointsPerQuarter") / when(col("Team") == col("AwayTeam"), col("AwayScore"))
                                        .otherwise(col("HomeScore")) * 100
    )
    result_df_with_percentages.show()
    write_to_db(result_df_with_percentages, "nba_test_core", "team_points_per_quarter")
    df = read_from_db(job.spark,"nba_test_core", "team_points_per_quarter")
    df.show()


first_points_per_game()#