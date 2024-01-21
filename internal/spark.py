from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col,
)
import os, csv


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
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS primerBaze2")
        folder_list = [folder for folder in os.listdir('./data')]
        print(folder_list)
        csv_reader = csv.reader("./data/2019-20_pbp.csv")
        i = 0
        df = self.spark.read.format("csv").option("mode", "PERMISSIVE").load("./data/2019-20_pbp.csv")
        df = self.spark.read.csv("./data/2019-20_pbp.csv", header=True, inferSchema=True)
        df.write.format("delta").option("overwriteSchema", "true").saveAsTable("primerBaze2.test2")
        # df.show(truncate=False, vertical=True)
        self.transform1()

    def write_parquet(self, df, path):
        df.write.parquet(path)

    def read_parquet(self, path):
        df = self.spark.read.parquet(path)
        return df

    def transform1(self):
        df: DataFrame = self.spark.read.format("delta").table("primerBaze2.test2")
        res = (
            df.filter(col("Rebounder").isNotNull())
            .groupBy("Rebounder", "ReboundType")
            .count()
        )
        res.show()

job = BatchJob()
job.run()