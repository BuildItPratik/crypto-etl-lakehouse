import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
from datetime import datetime
import psycopg2
import traceback

# -----------------------------
# SPARK SESSION
# -----------------------------
spark = SparkSession.builder \
    .appName("CryptoMetricsCalculator") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

jdbc_url = "jdbc:postgresql://postgres:5432/crypto"

db_properties = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}

def log(msg):
    print(f"[{datetime.utcnow()}] {msg}", flush=True)

def get_connection():
    return psycopg2.connect(
        host="postgres",
        port=5432,
        dbname="crypto",
        user="admin",
        password="admin"
    )

try:
    log("=== STARTING JOB ===")

    run_id = int(time.time())
    processing_time_value = datetime.utcnow()

    # -----------------------------
    # READ
    # -----------------------------
    df = spark.read.parquet("s3a://crypto-data/raw/") \
        .filter(col("event_time") >= current_timestamp() - expr("INTERVAL 20 MINUTES"))

    if df.rdd.isEmpty():
        log("No data. Exiting.")
        exit()

    log(f"Rows after read: {df.count()}")

    # -----------------------------
    # CLEAN
    # -----------------------------
    df = df.select(
        "event_id",
        "event_time",
        "producer_time",
        "producer_id",
        "id",
        "symbol",
        col("current_price").cast(DoubleType()).alias("price")
    ).dropDuplicates(["id", "event_time"])

    log(f"Rows after dedup: {df.count()}")

    # -----------------------------
    # METADATA
    # -----------------------------
    df = df.withColumn("processing_time", lit(processing_time_value)) \
        .withColumn("run_id", lit(run_id)) \
        .withColumn("lateness_sec",
                    unix_timestamp("processing_time") - unix_timestamp("event_time")) \
        .withColumn("ingestion_delay_sec",
                    unix_timestamp("processing_time") - unix_timestamp("producer_time")) \
        .withColumn(
            "data_status",
            when(col("lateness_sec") <= 420, "fresh")
            .when(col("lateness_sec") <= 1020, "late")
            .otherwise("too_late")
        )

    df = df.filter(col("data_status") != "too_late")
    log(f"Rows after filtering: {df.count()}")

    # -----------------------------
    # METRICS
    # -----------------------------
    window = Window.partitionBy("id").orderBy("event_time")

    df = df \
        .withColumn("price_1min_ago", lag("price", 2).over(window)) \
        .withColumn("price_5min_ago", lag("price", 10).over(window)) \
        .withColumn("change_1min",
                    round((col("price") - col("price_1min_ago")) / col("price_1min_ago") * 100, 2)) \
        .withColumn("change_5min",
                    round((col("price") - col("price_5min_ago")) / col("price_5min_ago") * 100, 2)) \
        .withColumn("sma", avg("price").over(window.rowsBetween(-4, 0))) \
        .withColumn("volatility", stddev("price").over(window.rowsBetween(-4, 0)))

    # -----------------------------
    # FINAL DEDUP
    # -----------------------------
    df = df.withColumn(
        "row_num",
        row_number().over(
            Window.partitionBy("id", "event_time")
            .orderBy(col("processing_time").desc())
        )
    ).filter(col("row_num") == 1).drop("row_num")

    # -----------------------------
    # LATEST
    # -----------------------------
    desc_window = Window.partitionBy("id").orderBy(col("event_time").desc())

    df = df.withColumn("rank_desc", row_number().over(desc_window))
    latest_per_coin = df.filter(col("rank_desc") == 1).drop("rank_desc")
    df = df.withColumn("is_latest", col("rank_desc") == 1).drop("rank_desc")

    # -----------------------------
    # SPLIT TABLES
    # -----------------------------
    events_df = df.select(
        "event_id", "event_time", "producer_time", "producer_id",
        "id", "symbol", "price",
        col("processing_time").alias("ingestion_time")
    )

    metadata_df = df.select(
        "event_id", "processing_time", "run_id",
        "lateness_sec", "ingestion_delay_sec", "data_status"
    )

    metrics_df = df.select(
        "id", "event_time", "price",
        "price_1min_ago", "price_5min_ago",
        "change_1min", "change_5min",
        "sma", "volatility"
    )

    latest_df = latest_per_coin.select(
        "id", "symbol", "price",
        "change_1min", "change_5min",
        "sma", "volatility",
        "event_time"
    )

    gain_df = latest_per_coin \
        .filter(col("change_5min").isNotNull()) \
        .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").desc()))) \
        .filter(col("rank") <= 5) \
        .select(
            lit(processing_time_value).alias("processing_time"),
            "rank", "id", "symbol", "change_5min"
        )

    loss_df = latest_per_coin \
        .filter(col("change_5min").isNotNull()) \
        .withColumn("rank", row_number().over(Window.orderBy(col("change_5min").asc()))) \
        .filter(col("rank") <= 5) \
        .select(
            lit(processing_time_value).alias("processing_time"),
            "rank", "id", "symbol", "change_5min"
        )

    # -----------------------------
    # EVENTS UPSERT
    # -----------------------------
    log(f"Writing events: {events_df.count()} rows")
    events_df.write.mode("append").jdbc(jdbc_url, "crypto_events_staging", properties=db_properties)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO crypto_events
        SELECT DISTINCT * FROM crypto_events_staging
        ON CONFLICT (id, event_time) DO NOTHING;
    """)
    cur.execute("DELETE FROM crypto_events_staging;")
    conn.commit()
    cur.close()
    conn.close()

    # -----------------------------
    # METADATA UPSERT
    # -----------------------------
    log(f"Writing metadata: {metadata_df.count()} rows")
    metadata_df.write.mode("append").jdbc(jdbc_url, "crypto_event_metadata_staging", properties=db_properties)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO crypto_event_metadata
        SELECT DISTINCT * FROM crypto_event_metadata_staging
        ON CONFLICT (event_id) DO NOTHING;
    """)
    cur.execute("DELETE FROM crypto_event_metadata_staging;")
    conn.commit()
    cur.close()
    conn.close()

    # -----------------------------
    # METRICS UPSERT
    # -----------------------------
    log(f"Writing metrics: {metrics_df.count()} rows")
    metrics_df.write.mode("append").jdbc(jdbc_url, "crypto_metrics_staging", properties=db_properties)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO crypto_metrics
        SELECT DISTINCT * FROM crypto_metrics_staging
        ON CONFLICT (id, event_time) DO NOTHING;
    """)
    cur.execute("DELETE FROM crypto_metrics_staging;")
    conn.commit()
    cur.close()
    conn.close()

    # -----------------------------
    # SNAPSHOT + AGGREGATES
    # -----------------------------
    log(f"Writing latest: {latest_df.count()} rows")
    latest_df.write.mode("overwrite").jdbc(jdbc_url, "crypto_latest", properties=db_properties)

    log(f"Writing gainers: {gain_df.count()} rows")
    gain_df.write.mode("append").jdbc(jdbc_url, "top_5_gainers", properties=db_properties)

    log(f"Writing losers: {loss_df.count()} rows")
    loss_df.write.mode("append").jdbc(jdbc_url, "top_5_losers", properties=db_properties)

    log("=== JOB SUCCESS ===")

except Exception as e:
    log(f"ERROR: {e}")
    traceback.print_exc()
    raise

finally:
    spark.stop()