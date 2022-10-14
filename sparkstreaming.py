from json import load
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import udf
from pyspark.sql.functions import col, pandas_udf, split
from pyspark.sql import SparkSession

topic = "twitterstream"

#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 sparkstreaming.py

spark = SparkSession \
    .builder \
    .appName("stream") \
    .getOrCreate()
    #config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "False") \
    .load()

#df.printSchema()

# df.show()
query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "/opt/homebrew/Cellar/apache-spark/3.3.0/libexec/python/test_support/sql/streaming") \
    .start()

query.awaitTermination()