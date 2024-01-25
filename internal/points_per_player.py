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

def points_per_player():
    job = BatchJob()
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_fact", "play_by_play")
    dimension_player = read_from_db(job.spark, "nba_test_fact", "dimension_player")
    dimension_shot_type = read_from_db(job.spark, "nba_test_fact", "dimension_shot_type")
    
    joined_df = df.join(dimension_player, df['Shooter'] == dimension_player['player_id'], 'inner')


    # Joining the result with dimension_type_id on 'shot_type_id' = 'shot_type_id'
    final_df = joined_df.join(dimension_shot_type, joined_df['ShotType_id'] == dimension_shot_type['_id'], 'inner')

    result_df = (
        final_df.filter(col("Shooter").alias("Player") != 'null')
        .groupBy("Shooter", "Date", "Season", "URL", "player_name")
        .agg(
            sum(
                when((col("shot_worth") == 3) & (col("ShotOutcome") == "make"), 3)
                .when((col("shot_worth") == 2) & (col("ShotOutcome") == "make"), 2)
                .otherwise(0)
            ).alias("Points"),
            sum(
                when((col("shot_worth") == 3) & (col("ShotOutcome") == "make"), 1)
                .otherwise(0)
            ).alias("ThreePointersMade"),
            sum(
                when(col("shot_worth") == 3, 1)
                .otherwise(0)
            ).alias("ThreePointersShot"),
            sum(
                when((col("shot_worth") == 2) & (col("ShotOutcome") == "make"), 1)
                .otherwise(0)
            ).alias("TwoPointersMade"),
            sum(
                when(col("shot_worth") == 2, 1)
                .otherwise(0)
            ).alias("TwoPointersShot"),
        )
        .withColumn("ThreePointPercentage", col("ThreePointersMade") / col("ThreePointersShot"))
        .withColumn("TwoPointPercentage", col("TwoPointersMade") / col("TwoPointersShot"))
        .withColumn("Shooter", col("player_name"))
        .drop("player_name")
        .drop("_id")
        .orderBy(col("Points").desc())
    )
    result_df.show()    
    write_to_db(result_df, "nba_test_core", "points_per_player4", "overwrite")
    df = read_from_db(job.spark,"nba_test_core", "points_per_player4")
    df.show(truncate=False)


points_per_player()