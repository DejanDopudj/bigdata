from pyspark.sql.functions import col
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    window,
    unix_timestamp
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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

def read_stream(table: str, spark):
    df = spark.readStream.format("delta").table(table)
    df2 = df

    ret = df2.join(df, df["session_id"] == df2["session_id"], "inner")

    return ret

def write_stream(
    df, table: str
) -> None:
    df.writeStream.format("delta").outputMode("append").trigger(
        availableNow=True
    ).option("checkpointLocation", f"hdfs://namenode:9000/user/hive/warehouse2/{table}").toTable(
        "session_data"
    ).awaitTermination()

def show(df):
    query = (
        df.writeStream.format("console")
        .outputMode("append").trigger(
        availableNow=True)
        .option("truncate", False)
        .start()
    )
    
    query.awaitTermination()

df = read_stream("raw.streaming_v2", spark)
# write_stream(df)
show(df)
