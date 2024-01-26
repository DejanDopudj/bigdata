from pyspark.sql.functions import (
    col,
)
from spark import BatchJob
from util import read_from_db, write_to_db
from pyspark.sql.functions import split

def extract_player_info(column, spark):
    split_col = split(column, " - ")
    return spark.createDataFrame([(split_col.getItem(1), split_col.getItem(0))], ["player_id", "player_name"])


def points_per_player():
    job = BatchJob()
    # drop_partition(job.spark,"nba_test_core", "points_per_player")
    df = read_from_db(job.spark, "nba_test_staging", "play_by_play")
    df.show()
    dimension_turnover_type_df = (
        df.select("TurnoverType")
        .distinct()
        .withColumnRenamed("TurnoverType", "turnover_type_name")
    )

    result_df = dimension_turnover_type_df.withColumn("turnover_type_id", col("turnover_type_name"))

    write_to_db(result_df, "nba_test_fact", "dimension_turnover_type", "overwrite")
    df = read_from_db(job.spark,"nba_test_fact", "dimension_turnover_type")
    df.show(truncate=False)

points_per_player()