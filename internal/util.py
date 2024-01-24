
from datetime import datetime

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
    ).save()

def convert_to_date(ds):
    original_datetime = datetime.fromisoformat(ds.rstrip("Z"))
    date = original_datetime.strftime("%Y-%m-%d")
    return date