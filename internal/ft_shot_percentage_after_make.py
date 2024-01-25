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
    desc,
    lead
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
    df = read_from_db(job.spark, "nba_test_fact", "play_by_play")

    df = df.withColumn("secLeftInt", col("SecLeft").cast("int"))

    window_spec = Window().partitionBy("FreeThrowShooter").orderBy([col("URL").asc(), col("quarter").asc(), col("secLeftInt").desc()])

    your_dataframe = df.withColumn("Next_FreeThrowOutcome", lead("FreeThrowOutcome").over(window_spec))

    success_df = your_dataframe.filter((col("FreeThrowOutcome") == "make"))


    result_df = success_df.filter(col("FreeThrowShooter")!='null').groupBy("FreeThrowShooter").agg(
        count((col("Next_FreeThrowOutcome") == "make")).alias("SuccessfulSecondFreeThrows"),
        count("*").alias("TotalFirstFreeThrows")
    ).withColumn("Percentage", col("SuccessfulSecondFreeThrows") / col("TotalFirstFreeThrows") * 100)


    result_df.show()
    # write_to_db(result_df, "nba_test_core", "ft_percentage_after_make")
    # df = read_from_db(job.spark,"nba_test_core", "ft_percentage_after_make")
    # df.show()


first_points_per_game()

