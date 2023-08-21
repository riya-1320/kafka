# Connect to Kafka Stream / File Stream and read data as JSON
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder.appName("producer").config("spark.driver.bindAddress", "127.0.0.1").master(
    "local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

dataFrame = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "jsonTopic") \
    .option("startingOffsets", "latest") \
    .load()

stringDf = dataFrame.selectExpr("CAST(value as STRING)")

sample_schema = (
    StructType()
    .add("Name", StringType())
    .add("age", IntegerType())
)

jsonDf = stringDf.select(from_json(col("value"), sample_schema).alias("data")).select("data.*")

jsonDf.writeStream \
    .format("console") \
    .outputMode("append")\
    .start() \
    .awaitTermination()
