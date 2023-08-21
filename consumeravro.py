from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.avro.functions import from_avro

spark = SparkSession.builder.appName("producer").config("spark.driver.bindAddress", "127.0.0.1").master(
    "local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "avroTopic2") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

schema_path = "/home/xs354-riygup/TASKS/kafka/data/data.avsc"

with open(schema_path, "r") as schema_file:
    jsonFormatSchema = schema_file.read()

personDF = df.select(from_avro(col("value"), jsonFormatSchema).alias("Person")).select("Person.*") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
