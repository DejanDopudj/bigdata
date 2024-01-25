from util import read_from_db, write_to_db, convert_to_date, write_stream_to_db, read_stream_from_db
from spark import BatchJob

def show(df):
    query = (
        df.writeStream.format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    query.awaitTermination()

job = BatchJob()
# df = read_stream_from_db(job.spark, "nba_test_stream_raw", "traffic")
# write_stream_to_db(df, "nba_test_stream_raw", "traffic", "staging")
df = read_stream_from_db(job.spark, "nba_test_stream_raw", "traffic")
# show(df)

