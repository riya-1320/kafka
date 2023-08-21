from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window
from pyspark.sql.types import StructType, StringType, TimestampType

spark = SparkSession \
         .builder \
         .appName("producer") \
         .config("spark.driver.bindAddress", "127.0.0.1") \
         .master("local[*]") \
         .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("name", StringType()) \
    .add("timestamp", TimestampType())

dataFrame = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed_df = dataFrame \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Tumbling window example (window size = 10 seconds)
tumbling_window_duration = "10 seconds"

tumbling_window_df = parsed_df \
   .groupBy(window("timestamp", tumbling_window_duration)) \
   .count()

# Sliding window example (window size = 10 seconds, slide duration = 5 seconds)
# sliding_window_duration = "10 seconds"
# sliding_window_slide_duration = "5 seconds"
#
# sliding_window_df = parsed_df \
#    .withWatermark("timestamp", "1 minute") \
#    .groupBy(window("timestamp", sliding_window_duration, sliding_window_slide_duration)) \
#    .count()

# Session window example (gap duration = 10 seconds)
# session_window_gap_duration = "10 seconds"
#
# session_window_df = parsed_df \
#     .withWatermark("timestamp", "1 minute") \
#     .groupBy(window("timestamp", session_window_gap_duration)) \
#     .count()

# Start the streaming queries to display the results
tumbling_window_query = tumbling_window_df \
   .writeStream \
   .outputMode("complete") \
   .format("console") \
   .start(truncate=False)

# sliding_window_query = sliding_window_df \
#    .writeStream \
#    .outputMode("complete") \
#    .format("console") \
#    .start(truncate=False)

# session_window_query = session_window_df \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start(truncate=False)

# Wait for the termination of the queries
tumbling_window_query.awaitTermination()
# sliding_window_query.awaitTermination()
# session_window_query.awaitTermination()
