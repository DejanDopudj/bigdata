
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType

def read_from_db(spark, database, collection):
    return (
        spark.read.format("mongodb")
        .option("database", database)
        .option("collection", collection)
        .load()
    )

def write_to_db(df, database, collection):
    df.write.format("mongodb").mode("append").option("database", database).option(
        "collection", collection
    ).partitionBy(
            "Season", "Date"
        ).save()
    
def write_stream_to_db(df, database, collection, checkpoint):
    query = df.writeStream.format("mongodb").outputMode("append").option("database", database).option(
        "collection", collection
).option("checkpointLocation", f"{checkpoint}").trigger(once=True).start()
    query.awaitTermination()


schema = StructType([
    StructField("session_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("event_date_time", StringType(), True),
    StructField("country_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("user_id", StringType(), True)
])

def read_stream_from_db(spark, database, collection):
    df = spark.readStream.format("mongodb").option("database", database).option(
        "collection", collection
    ).schema(schema).load()
    return df

def convert_to_date(ds):
    original_datetime = datetime.fromisoformat(ds.rstrip("Z"))
    date = original_datetime.strftime("%Y-%m-%d")
    return date