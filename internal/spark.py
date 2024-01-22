from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, regexp_extract, when, sum, concat_ws, year, month, count, avg
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
        # self.points_per_player()
        # self.spark.sql(f"DROP TABLE IF EXISTS nba_raw.play_by_play")
        # self.spark.sql(f"DROP TABLE IF EXISTS nba_staging.play_by_play")
        # self.spark.sql(f"DROP TABLE IF EXISTS nba_core.play_by_play")

        # for i in range (1,6):
        # self.daily_ingest(f"01-02-2017", f"2017-02-01")
        # self.daily_ingest(f"01-06-2017", f"2017-06-01")
        # self.staging_ingest(f"2017-04-01")
        # print("PPP")
        # self.calculate()
        self.points_per_player()
        self.points_per_game()
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS primerBaze2")
        # df = self.spark.read.format("csv").option("mode", "PERMISSIVE").load("./data/2019-20_pbp.csv")
        # df = self.spark.read.csv("./data/2019-20_pbp.csv", header=True, inferSchema=True)
        # df.write.format("delta").option("overwriteSchema", "true").saveAsTable("primerBaze2.test2")
        # self.transform1()
    
    def drop_partition(self, database_name, table_name, partition_column, partition_value):
        if self.spark.sql(f"SHOW TABLES IN {database_name} LIKE '{table_name}'").count() > 0:
        # Delta table exists, perform the DELETE operation to drop the specified partition
            self.spark.sql(f"DELETE FROM {database_name}.{table_name} WHERE {partition_column} = '{partition_value}'")
            print(f"Partition {partition_value} dropped from {table_name}")
        else:
            print(f"Delta table {table_name} does not exist")

    def transform1(self):
        df: DataFrame = self.spark.read.format("delta").table("primerBaze2.test2")
        res = (
            df.filter(col("Rebounder").isNotNull())
            .groupBy("Rebounder", "ReboundType")
            .count()
        )
        res.show()

    def daily_ingest(self, date, date2):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_raw")
        self.drop_partition("nba_raw", "play_by_play", "Date", date2)
        print("1")
        df = self.spark.read.csv(f"./data/{date}-pbp.csv", header=True, inferSchema=True)
        print("2")
        df.write.format("delta") \
        .saveAsTable("nba_raw.play_by_play", partitionBy="Date", mode="append")
        print("3")
        self.staging_ingest(date2)

        
    def staging_ingest(self, date):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_staging")
        print("1")
        print("2")
        result_df = (
            self.spark.read.format("delta").table("nba_raw.play_by_play")
            .filter((col("Date") == date))
            .drop("Time", "AwayPlay","HomePlay")
            .withColumn("ShotType", regexp_extract(col("ShotType"), r'(\d+)', 1))
            .withColumn("Season", concat_ws('-', 
                                    when(month(col("Date")) >= 10, year(col("Date"))).otherwise(year(col("Date")) - 1).cast("string"),
                                    when(month(col("Date")) >= 10, year(col("Date")) + 1).otherwise(year(col("Date"))).cast("string"),
                                )) \
        )
        result_df.write.format("delta") \
        .saveAsTable("nba_staging.play_by_play", partitionBy="Date", mode="append")
        print("4")
        # self.example_read()
        # self.points_per_player()

    def example_read(self):
        df: DataFrame = self.spark.read.format("delta").table("nba_staging.play_by_play")
        res = (
            df.filter(col("Rebounder").isNotNull())
            .groupBy("Rebounder", "ReboundType")
            .count()
        )
        res.show()

    def calculate(self):
        df: DataFrame = self.spark.read.format("delta").table("nba_raw.play_by_play")
        result_df = df.groupBy("Date").agg(count("*").alias("RecordCount"))
        result_df.select("Date", "RecordCount").show()


    def points_per_player(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_core")
        df: DataFrame = self.spark.read.format("delta").table("nba_staging.play_by_play")
        result_df = df.filter(col("Shooter").isNotNull()) \
            .groupBy("Shooter", "Date", "HomeTeam", "AwayTeam", "Season") \
            .agg(
                sum(when((col("ShotType") == 3) & (col("ShotOutcome") == "make"), 3)
                    .when((col("ShotType") == 2) & (col("ShotOutcome") == "make"), 2)
                    .otherwise(0)
                    ).alias("Points")
            ) \
            .orderBy(col("Points").desc())
        result_df.write.format("delta").option("overwriteSchema", "true") \
        .partitionBy("Season", "Date") \
        .saveAsTable("nba_core.play_by_play", mode="overwrite")
        result_df.show()

    def points_per_game(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_core")
        df: DataFrame = self.spark.read.format("delta").table("nba_core.play_by_play")
        average_stats_df = df.groupBy("Shooter", "Season") \
        .agg(
            avg("Points").alias("AveragePointsPerGame")
        ) \
        .orderBy(col("AveragePointsPerGame").desc())
        average_stats_df.show()

job = BatchJob()
job.run()