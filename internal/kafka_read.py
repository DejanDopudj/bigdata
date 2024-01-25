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
    from_json
)
import csv
from spark import BatchJob
from util import read_from_db, write_to_db, convert_to_date, write_stream_to_db, read_stream_from_db
from airflow.operators.python import get_current_context
import sys
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("event_date_time", StringType(), True),
    StructField("country_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True)
])

def kafka_read(ds):
    date_str = convert_to_date(ds)
    date="01-03-2017"
    job = BatchJob()
    df = job.spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "kafka:9092") \
      .option("subscribe", "test") \
      .option("startingOffsets", "earliest") \
      .load()\
      .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))\
      .select(col("parsed_value.*"))

    query = df.selectExpr("*") \
        .writeStream \
        .format("console") \
        .option("checkpointLocation", "hdfs://namenode:9000/checkpoint") \
        .start()

    query.awaitTermination()

    # write_stream_to_db(df,"nba_test","test","hdfs://namenode:9000/checkpoint12345")

    # new_df = read_stream_from_db(job.spark,"nba_test","test")

    # print("AAAAAAAA")
    # query = new_df.selectExpr("*") \
    #     .writeStream \
    #     .format("console") \
    #     .option("checkpointLocation", "hdfs://namenode:9000/checkpoint32345") \
    #     .start()
    # print("AAAAAAAA")

 
execution_date = sys.argv[1]
kafka_read(execution_date)