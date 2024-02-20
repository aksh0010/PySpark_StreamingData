from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PubSubExample") \
    .getOrCreate()

# Read from Pub/Sub topic
pubsub_df = spark \
    .readStream \
    .format("pubsub") \
    .option("subscription", "projects/project_id/subscriptions/subscription_name") \
    .load()

# Perform transformations, aggregations, etc.

# Write processed data to Hive table
query = pubsub_df \
    .writeStream \
    .outputMode("append") \
    .format("hive") \
    .option("path", "/user/hive/warehouse/your_table_name") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

query.awaitTermination()
