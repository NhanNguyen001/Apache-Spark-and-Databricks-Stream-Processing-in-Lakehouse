# Databricks notebook source
class batchWC():
    def __init__(self):
        self.base_data_dir = "/FileStore/data_spark_streaming_scholarnest"

    def getRawData(self):
        from pyspark.sql.functions import explode, split
        lines = (spark.read
                    .format("text")
                    .option("lineSep", ".")
                    .load(f"{self.base_data_dir}/data/text")
                )
        return lines.select(explode(split(lines.value, " ")).alias("word"))
    

# COMMAND ----------


