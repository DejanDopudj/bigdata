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
    from_json,window
)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when, unix_timestamp
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
        "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.HDFSLogStore",
        )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
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


def foreach_batch(df, epoch_id):
    df.show()
    # df.write.format("mongodb").mode("append").option("database", "test").option(
    #     "collection", "test"
    # ).save()
    # df.write.format("mongodb").mode("append").option("database", "test2").option(
    #     "collection", "test2"
    # ).save()
    df.write.format("mongodb").mode("append").option("database", "test3").option(
        "collection", "test3"
    ).save()
    deduplicated = deduplicate(df)
    deduplicated.show()

def deduplicate(df):
    return df.distinct()

def check_player(df):
    streamDF = streamDF.select("player_id", "name")
    df = read_from_db(spark, "nba_test_core", "points_per_player4")
    window_spec = Window().partitionBy("Shooter").orderBy(col("Date").desc()).rowsBetween(-6, 0)
    result_df = df.withColumn("Points_Last_7_Days", sum("Points").over(window_spec)).select("Shooter", "URL", "Points_Last_7_Days")
    df = df.join(result_df, col("Shooter") == col("player_id"), "inner").select()
    df.show(truncate=False)
    df.write.format("mongodb").mode("append").option("database", "transformations").option(
        "collection", "player_products"
    ).save()


def session_length(df):
    df_sessions = df.select("session_id", "event_date_time", "name")

    windowSpec = Window().partitionBy("session_id").orderBy("event_date_time")

    df_sessions = df_sessions.withColumn("session_length", 
        when(col("name") == "session_started", 0)
        .otherwise(unix_timestamp("event_date_time") - unix_timestamp(lag("event_date_time").over(windowSpec)))
    )
    df.write.format("mongodb").mode("append").option("database", "transformations").option(
        "collection", "session_length"
    ).save()


def events_clicked(df):
    result_df = df.filter((col("name") == "bought_product") | (col("name") == "clicked_on_product")) \
        .groupBy(window("event_date_time", "1 minute"), "name") \
        .count()
    
    bought_count = result_df.filter(col("name") == "bought_product").select("event_count").first()[0]
    clicked_count = result_df.filter(col("name") == "clicked_on_product").select("event_count").first()[0]

    if bought_count > clicked_count:
        print("Warning: The count of bought_product is greater than clicked_on_product within the last minute.")



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
            StructField("player_name", StringType(), True),
            StructField("cost", StringType(), True),
            StructField("user_id", StringType(), True),
        ]
    )
    df = (
        job.spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "kafka-topic")
        # .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
        .select(col("parsed_value.*"))
    )
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS n_raw")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS n_staging")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS n_fact")

    df.writeStream.format("delta").outputMode("append").trigger(
            availableNow=True
        ).option("checkpointLocation", "hdfs://namenode:9000/user/hive/warehouse2/n_raw").toTable(
            "n_raw.streaming"
        ).awaitTermination()
    print("Written")


    # df = (
    #     spark.readStream.format("mongodb")
    #     .option("database", "test")
    #     .option("collection", "test")
    #     .schema(schema)
    #     .load()
    # )

    # query2 = df.writeStream.option("checkpointLocation", "staging").foreachBatch(foreach_batch2).outputMode("append").start()

    # # query2 = df.writeStream.foreachBatch(foreach_batch2).outputMode("append").start()

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