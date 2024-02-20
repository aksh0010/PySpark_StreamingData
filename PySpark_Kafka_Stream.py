from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("KafkaExample") \
    .getOrCreate()

# Define schema for the incoming Kafka messages
schema = StructType() \
    .add("key", StringType()) \
    .add("value", StringType())

# Read from Kafka topic
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka_broker:port") \
    .option("subscribe", "topic_name") \
    .load()

# Convert binary value column to string and parse JSON
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Perform transformations, aggregations, etc.

# Write processed data to output sink (e.g., console)
# Write processed data to Hive table
query = parsed_df \
    .writeStream \
    .outputMode("append") \
    .format("hive") \
    .option("path", "/user/hive/warehouse/your_table_name") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

query.awaitTermination()

