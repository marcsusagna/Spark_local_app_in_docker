import os
import os.path

from pyspark.sql import functions as F, Window
from pyspark.sql import types as T
from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .master("local")
    .appName("My processing app")
    .getOrCreate()
)


my_data = [
    {"Name": "Name1", "City": "City1"},
    {"Name": "Name2", "City": "City1"},
    {"Name": "Name3", "City": "City1"},
    {"Name": "Name4", "City": "City1"},
    {"Name": "Name5", "City": "City2"},
    {"Name": "Name6", "City": "City2"},
    {"Name": "Name7", "City": "City2"},
    {"Name": "Name9", "City": "City3"}
]

schema = T.StructType([
    T.StructField("Name", T.StringType()),
    T.StructField("City", T.StringType()),
])

my_df = spark.createDataFrame(my_data, schema)

print("Spark: Hello World")

output_dir = "/spark_app/output"

my_df.write.mode("overwrite").csv(output_dir)