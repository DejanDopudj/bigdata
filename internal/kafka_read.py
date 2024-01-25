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
    from_json,
)
import csv
from spark import BatchJob
from util import read_from_db, write_to_db, convert_to_date
from airflow.operators.python import get_current_context
import sys
from pyspark.sql.types import StructType, StructField, StringType

spark: SparkSession = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("NBA")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=GMT")
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=GMT")
    .config("spark.sql.session.timeZone", "UTC")
    .config(
        "spark.mongodb.read.connection.uri",
        "mongodb://mongo:27017",
    )
    .config(
        "spark.mongodb.write.connection.uri",
        "mongodb://mongo:27017",
    )
    .config(
        "spark.jars.packages",
        "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
    )
    .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse2")
    .config(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        "jdbc:postgresql://hive-metastore-postgresql/metastore",
    )
    .config(
        "spark.hadoop.javax.jdo.option.ConnectionDriverName",
        "org.postgresql.Driver",
    )
    .config("spark.hadoop.javax.jdo.option.ConnectionUserName", "hive")
    .config("spark.hadoop.javax.jdo.option.ConnectionPassword", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .config("spark.pyspark.python", "python3")
    .enableHiveSupport()
    .getOrCreate()
)

schema = StructType(
    [
        StructField("session_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("event_date_time", StringType(), True),
        StructField("country_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("user_id", StringType(), True),
    ]
)


def foreach_batch(df):
    df.show()
    df.write.format("mongodb").mode("append").option("database", "test").option(
        "collection", "test"
    ).save()


def foreach_batch2(df):
    df.show()


def kafka_read(ds):
    date_str = convert_to_date(ds)
    date = "01-03-2017"
    job = BatchJob()
    schema = StructType(
        [
            StructField("session_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("event_date_time", StringType(), True),
            StructField("country_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("user_id", StringType(), True),
        ]
    )
    df = (
        job.spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "test")
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
        .select(col("parsed_value.*"))
    )

    query = df.writeStream.foreachBatch(foreach_batch).outputMode("append").start()

    query.awaitTermination()

    # df = (
    #     spark.readStream.format("mongodb")
    #     .option("database", "test")
    #     .option("collection", "test")
    #     .schema(schema)
    #     .load()
    # )

    # query2 = df.writeStream.foreachBatch(foreach_batch).outputMode("append").start()

    # query2.awaitTermination()


def read_stream_from_db(spark, database, collection):
    df = (
        spark.readStream.format("mongodb")
        .option("database", database)
        .option("collection", collection)
        .schema(schema)
        .load()
    )
    return df


execution_date = sys.argv[1]
kafka_read(execution_date)