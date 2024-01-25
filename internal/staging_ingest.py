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
from util import read_from_db, write_to_db

def staging_ingest(date):
        job = BatchJob()
        job.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_staging")
        print("1")
        print("2")
        raw_data = read_from_db(job.spark, "nba_test_raw", "play_by_play")
        # raw_data.show()
        result_df = (
            raw_data.filter((col("Date") == date))
            .drop("Time")
            .withColumn("ShotType", regexp_extract(col("ShotType"), r"(\d+)", 1))
            .withColumn(
                "Season",
                concat_ws(
                    "-",
                    when(month(col("Date")) >= 10, year(col("Date")))
                    .otherwise(year(col("Date")) - 1)
                    .cast("string"),
                    when(month(col("Date")) >= 10, year(col("Date")) + 1)
                    .otherwise(year(col("Date")))
                    .cast("string"),
                ),
            )
        )
        write_to_db(result_df, "nba_test_staging", "play_by_play")
        staging_data = read_from_db(job.spark, "nba_test_staging", "play_by_play")
        staging_data.show()

     

staging_ingest("2017-03-01")