class BatchJob:
    def __init__(self, spark):
        self.spark = spark
    
    def ucitavam_csv(season):
        df = self.spark.read.option("header", True).csv(f"./data/batch/{season}_pbp.csv")
        df.show() 