# Connect to Kafka Stream / File Stream and read data as AVRO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.avro.functions import to_avro
from pyspark.sql.types import StringType, StructType

spark = SparkSession.builder.appName("producer").config("spark.driver.bindAddress", "127.0.0.1").master(
    "local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

dataFrame = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "avroTopic1") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

schema = (
    StructType()
    .add("Name", StringType())
    .add("favColor", StringType())
)

personDF = dataFrame.selectExpr("CAST(value AS STRING)")
final_df = personDF.select(from_json(col("value"), schema).alias("data"))

query = final_df.select(to_avro(struct("data.*")).alias("value")) \
    .writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "avroTopic2") \
    .option("checkpointLocation", "/home/xs354-riygup/TASKS/kafka/checkPoint") \
    .start()

query.awaitTermination()
