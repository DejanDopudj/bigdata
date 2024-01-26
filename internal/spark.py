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


class BatchJob:
    def __init__(self):
        self.spark: SparkSession = (
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
            .config(
                "spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse2"
            )
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

    def read_mongodb(self, database, collection):
        return (
            self.spark.read.format("mongodb")
            .option("database", database)
            .option("collection", collection)
            .load()
        )

    def write_mongodb(self, df, database, collection):
        #         dataFrame = self.spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
        #    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
        df.write.format("mongodb").mode("append").option("database", database).option(
            "collection", collection
        ).save()

    # def run(self):
        # self.example_read()
        # self.points_per_player()
        # self.spark.sql(f"DROP TABLE IF EXISTS nba_raw.play_by_play")
        # self.spark.sql(f"DROP TABLE IF EXISTS nba_staging.play_by_play")
        # self.spark.sql(f"DROP TABLE IF EXISTS nba_core.play_by_play")

        # for i in range (1,6):
        # self.daily_ingest(f"01-02-2017", f"2017-02-01")
        # self.daily_ingest(f"01-06-2017", f"2017-06-01")
        # self.staging_ingest(f"2017-04-01")
        # print("PPPD")
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_raw")
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_raw")
        # print("dropping partition")
        # x = self.spark.sql(f"SHOW TABLES IN nba_raw LIKE 'test2'").select()
        # print(x)
        # print("test")
        # # self.drop_partition("nba_raw", "play_by_play", "Date", "2017-02-01")

        # self.calculate()
        # self.points_per_player()
        # self.points_per_game()
        # self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS primerBaze2")
        # df = self.spark.read.format("csv").option("mode", "PERMISSIVE").load("./data/2019-20_pbp.csv")
        # df = self.spark.read.csv("./data/2019-20_pbp.csv", header=True, inferSchema=True)
        # df.write.format("delta").option("overwriteSchema", "true").saveAsTable("primerBaze2.test2")
        # self.transform1()

    def drop_partition(
        self, database_name, table_name, partition_column, partition_value
    ):
        # ne valja
        if (
            self.spark.sql(
                f"SHOW TABLES IN {database_name} LIKE '{table_name}'"
            ).select()
            > 0
        ):
            # Delta table exists, perform the DELETE operation to drop the specified partition
            self.spark.sql(
                f"DELETE FROM {database_name}.{table_name} WHERE {partition_column} = '{partition_value}'"
            )
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

    def staging_ingest(self, date):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_staging")
        print("1")
        print("2")
        result_df = (
            self.spark.read.format("delta")
            .table("nba_raw.play_by_play")
            .filter((col("Date") == date))
            .drop("Time", "AwayPlay", "HomePlay")
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
        result_df.write.format("delta").saveAsTable(
            "nba_staging.play_by_play", partitionBy="Date", mode="append"
        )
        print("4")
        # self.example_read()
        # self.points_per_player()

    def example_read(self):
        df: DataFrame = self.spark.read.format("delta").table(
            "nba_staging.play_by_play"
        )
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
        df: DataFrame = self.spark.read.format("delta").table(
            "nba_staging.play_by_play"
        )
        result_df = (
            df.filter(col("Shooter") != '')
            .groupBy("Shooter", "Date", "HomeTeam", "AwayTeam", "Season")
            .agg(
                sum(
                    when((col("ShotType") == 3) & (col("ShotOutcome") == "make"), 3)
                    .when((col("ShotType") == 2) & (col("ShotOutcome") == "make"), 2)
                    .otherwise(0)
                ).alias("Points")
            )
            .orderBy(col("Points").desc())
        )
        result_df.write.format("delta").option("overwriteSchema", "true").partitionBy(
            "Season", "Date"
        ).saveAsTable("nba_core.play_by_play", mode="overwrite")
        result_df.show()

    def points_per_game(self):
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS nba_core")
        df: DataFrame = self.spark.read.format("delta").table("nba_core.play_by_play")
        average_stats_df = (
            df.groupBy("Shooter", "Season")
            .agg(avg("Points").alias("AveragePointsPerGame"))
            .orderBy(col("AveragePointsPerGame").desc())
        )
        average_stats_df.show()


# job = BatchJob()
# job.run()
