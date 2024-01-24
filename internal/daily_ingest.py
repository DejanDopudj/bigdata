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
from spark import BatchJob
from util import read_from_db, write_to_db, convert_to_date
from airflow.operators.python import get_current_context
import sys


def daily_ingest(ds):
    date_str = convert_to_date(ds)
    date="01-03-2017"
    job = BatchJob()
    # self.drop_partition("nba_raw", "play_by_play", "Date", date2)
    # path = f"hdfs://namenode:9000/data-warehouse/{date}-pbp.csv"
    path = f"/user/local/spark/app/data/{date}-pbp.csv"
    with open(path, newline="\n") as csvfile:
        spamreader = csv.reader(csvfile, delimiter=",", quotechar="|")
        i = 0
        rows = []
        for row in spamreader:
            rows.append(row)
            i += 1
            if i == 15:
                break
    columns = rows[0]
    data_rows = [Row(**dict(zip(columns, values))) for values in rows[1:]]
    df = job.spark.createDataFrame(data_rows)
    write_to_db(df, "nba_test_raw", "play_by_play")
    df2 = read_from_db(job.spark, "nba_test_raw", "play_by_play")
    df2.show()

execution_date = sys.argv[1]
daily_ingest(execution_date)