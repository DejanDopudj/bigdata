
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
    avg,monotonically_increasing_id
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
    dimension_team_df = read_from_db(job.spark, "nba_test_fact", "dimension_team")
    dimension_shot_type_df = read_from_db(job.spark, "nba_test_fact", "dimension_shot_type")
    dimension_player_df = read_from_db(job.spark, "nba_test_fact", "dimension_player2")
    dimension_turnover_type_df = read_from_db(job.spark, "nba_test_fact", "dimension_turnover_type")

    team_home_dim = dimension_team_df.withColumnRenamed("team_name", "HomeTeam_dim").withColumnRenamed("_id", "HomeTeam_id")
    team_away_dim = dimension_team_df.withColumnRenamed("team_name", "AwayTeam_dim").withColumnRenamed("_id", "AwayTeam_id")
    shot_type_dim = dimension_shot_type_df.withColumnRenamed("shot_type", "ShotType_dim").withColumnRenamed("_id", "ShotType_id")
    turnover_type_dim = dimension_turnover_type_df.withColumnRenamed("turnover_type_name", "TurnoverType_dim").withColumnRenamed("turnover_type_id", "TurnoverType_id")



    df_alias = df.alias("df")


    fact_play_df = df_alias.join(team_home_dim, col("df.HomeTeam") == col("HomeTeam_dim"), "left_outer") \
        .drop("HomeTeam_dim") \
        .withColumnRenamed("HomeTeam_id", "HomeTeam_id") \
        .join(team_away_dim, col("df.AwayTeam") == col("AwayTeam_dim"), "left_outer") \
        .drop("AwayTeam_dim") \
        .withColumnRenamed("AwayTeam_id", "AwayTeam_id")\
        .join(shot_type_dim, col("df.ShotType") == col("ShotType_dim"), "left_outer") \
        .drop("ShotType_dim") \
        .withColumnRenamed("ShotType_id", "ShotType_id")\
        .join(turnover_type_dim, col("df.TurnoverType") == col("TurnoverType_dim"), "left_outer") \
        .drop("TurnoverType_dim") \
        .withColumnRenamed("TurnoverType_id", "TurnoverType_id")

    fact_play_df = fact_play_df.withColumn("secLeftInt", col("df.SecLeft").cast("int"))

    fact_play_df = fact_play_df.orderBy([col("URL").asc(), col("quarter").asc(), col("secLeftInt").desc()]).withColumn("play_id", monotonically_increasing_id())

    fact_play_df.show()



    fact_play_df = fact_play_df.select(
        "play_id", "df.URL", "df.GameType", "df.Location", "df.Date",
        "df.WinningTeam", "df.Quarter", "df.SecLeft", 
        "AwayTeam_id", "df.AwayPlay", "df.AwayScore", 
        "HomeTeam_id", "df.HomePlay", "df.HomeScore", 
        "Shooter", "Assister", "Rebounder", "ReboundType",  
        "ShotType_id", "df.ShotOutcome", "df.ShotDist", 
        "TurnoverType_id", "df.TurnoverPlayer", "df.TurnoverCause", "df.FreeThrowOutcome", "df.FreeThrowShooter",
        "df.TurnoverCauser", "df.JumpballAwayPlayer", "df.JumpballHomePlayer", "df.JumpballPoss", "df.Season"
    ).filter((col("URL")=="/boxscores/201703010ORL.html"))
    
    split_col = split(fact_play_df['Shooter'], ' - ')
    fact_play_df = fact_play_df.withColumn('Shooter', split_col.getItem(1))
    split_col = split(fact_play_df['Assister'], ' - ')
    fact_play_df = fact_play_df.withColumn('Assister', split_col.getItem(1))
    split_col = split(fact_play_df['Rebounder'], ' - ')
    fact_play_df = fact_play_df.withColumn('Rebounder', split_col.getItem(1))

    fact_play_df = fact_play_df.drop("secLeftInt")

    write_to_db(fact_play_df, "nba_test_fact", "play_by_play", "overwrite")
    df = read_from_db(job.spark,"nba_test_fact", "play_by_play")
    df.show(truncate=False)

points_per_player()



