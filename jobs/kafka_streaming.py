from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# -----------------------------
# Schema
# -----------------------------
coin_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("producer_time", StringType(), True),
    StructField("producer_id", StringType(), True),
    StructField("id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("current_price", DoubleType(), True),
    StructField("market_cap", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("high_24h", DoubleType(), True),
    StructField("low_24h", DoubleType(), True),
    StructField("last_updated", StringType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaCryptoConsumer") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

# -----------------------------
# Kafka stream
# -----------------------------
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-1:9092,kafka-2:9092,kafka-3:9092") \
    .option("subscribe", "crypto-prices") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-crypto-consumer") \
    .load()

# -----------------------------
# Convert binary → string
# -----------------------------
json_df = raw_df.select(
    col("value").cast("string").alias("json"),
    col("timestamp").alias("kafka_timestamp"),
    col("partition"),
    col("offset")
)

# -----------------------------
# Parse JSON
# -----------------------------
parsed_df = json_df.select(
    from_json(col("json"), coin_schema).alias("coin"),
    "kafka_timestamp",
    "partition",
    "offset"
)

# -----------------------------
# Flatten + enrich
# -----------------------------
flattened_df = parsed_df.select(
    "coin.*",
    "kafka_timestamp",
    "partition",
    "offset"
).withColumn(
    "event_time",
    to_timestamp("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
).withColumn(
    "producer_time",
    to_timestamp("producer_time", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
).withColumn(
    "event_date",
    to_date("event_time")
)


# -----------------------------
# OPTIONAL: basic dedup (lightweight)
# -----------------------------
# prevents exact duplicates within micro-batch
flattened_df = flattened_df.dropDuplicates(["event_id"])

# -----------------------------
# Write stream (partitioned!)
# -----------------------------
query = flattened_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .partitionBy("event_date") \
    .trigger(processingTime="30 seconds") \
    .option("path", "s3a://crypto-data/raw/") \
    .option("checkpointLocation", "s3a://crypto-data/checkpoints/kafka_to_minio/") \
    .start()

# query = flattened_df.writeStream \
#     .format("parquet") \
#     .outputMode("append") \
#     .partitionBy("event_date") \
#     .option("path", "/tmp/crypto-data/raw") \
#     .option("checkpointLocation", "/tmp/checkpoints/kafka_to_local") \
#     .start()

print(query.status)
print(query.isActive)

query.awaitTermination()