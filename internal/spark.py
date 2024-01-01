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
        .getOrCreate()
        
    def run(self):
        folder_list = [folder for folder in os.listdir('./data')]
        print(folder_list)
        csv_reader = csv.reader("./data/2019-20_pbp.csv")
        i = 0
        df = self.spark.read.csv("./data/2019-20_pbp.csv", header=True, inferSchema=True)
        self.write_parquet(df, "./processed/test/data/2019-20_pbp.parquet")
        read_df = self.read_parquet("./processed/test/data/2019-20_pbp.parquet")
        read_df.show(truncate=False, vertical=True)
        self.transform1()

    def write_parquet(self, df, path):
        df.write.parquet(path)

    def read_parquet(self, path):
        df = self.spark.read.parquet(path)
        return df

    def transform1(self):
        df = self.read_parquet("./processed/test/data/2019-20_pbp.parquet")
        res = (
            df.filter(col("Rebounder").isNotNull())
            .groupBy("Rebounder", "ReboundType")
            .count()
        )
        res.show()
        self.write_parquet(res, "./processed/test/data/2019-20_rebounds.parquet")

job = BatchJob()
job.run()