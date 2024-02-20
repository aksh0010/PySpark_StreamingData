from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SocketExample") \
    .getOrCreate()

# Read from socket
socket_df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Perform transformations, aggregations, etc.

# Write processed data to Hive table
query = socket_df \
    .writeStream \
    .outputMode("append") \
    .format("hive") \
    .option("path", "/user/hive/warehouse/your_table_name") \
    .option("checkpointLocation", "/path/to/checkpoint/dir") \
    .start()

query.awaitTermination()
