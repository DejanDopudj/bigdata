from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col,
)


class BatchJob:
    def __init__(self):
        self.spark = SparkSession.builder \
        .appName("NBA") \
        .config("spark.master", "spark://spark-master:7077") \
        .config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-avro_2.12:3.3.0",
        ) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        ) \
        .config("spark.sql.warehouse.dir", "./spark-warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.pyspark.python", "python3") \
        .enableHiveSupport() \
        .getOrCreate()

        
    def run(self):
        # self.example_read()
        self.staging_ingest('01-01-2017')
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS primerBaze2")
        # df = self.spark.read.format("csv").option("mode", "PERMISSIVE").load("./data/2019-20_pbp.csv")
        # df = self.spark.read.csv("./data/2019-20_pbp.csv", header=True, inferSchema=True)
        # df.write.format("delta").option("overwriteSchema", "true").saveAsTable("primerBaze2.test2")
        # self.transform1()

    def transform1(self):
        df: DataFrame = self.spark.read.format("delta").table("primerBaze2.test2")
        res = (
            df.filter(col("Rebounder").isNotNull())
            .groupBy("Rebounder", "ReboundType")
            .count()
        )
        res.show()

    def daily_ingest(self, date):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_raw")
        print("1")
        df = self.spark.read.csv(f"./data/{date}-pbp.csv", header=True, inferSchema=True)
        print("2")
        df.write.format("delta").mode("overwrite") \
        .partitionBy("Date") \
        .saveAsTable("nba_raw.play_by_play")
        print("3")

        
    def staging_ingest(self, date):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_staging")
        print("1")
        print("2")
        result_df = (
            self.spark.read.format("delta").table("nba_raw.play_by_play")
            .filter((col("Date") == date))
            .drop("Time", "AwayPlay","HomePlay")
        )
        result_df.write.format("delta").mode("overwrite") \
        .partitionBy("Date") \
        .saveAsTable("nba_staging.play_by_play")
        print("4")
        self.example_read()

    def example_read(self):
        df: DataFrame = self.spark.read.format("delta").table("nba_staging.play_by_play")
        res = (
            df.filter(col("Rebounder").isNotNull())
            .groupBy("Rebounder", "ReboundType")
            .count()
        )
        res.show()

    

job = BatchJob()
job.run()