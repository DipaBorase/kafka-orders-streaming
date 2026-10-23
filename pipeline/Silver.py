
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BRONZE_PATH = "s3a://orders/bronze/orders"
SILVER_PATH = "s3a://orders/silver/orders"
CHECKPOINT_PATH = "/tmp/kafka-orders-checkpoints/silver"

BRONZE_SCHEMA = StructType([
    StructField("kafka_key", StringType(), True),
    StructField("raw_event", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("partition", IntegerType(), True),
    StructField("offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
])

ORDER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("event_time", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaOrdersSilver")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    bronze = (
        spark.readStream
        .schema(BRONZE_SCHEMA)
        .format("json")
        .load(BRONZE_PATH)
    )

    parsed = (
        bronze
        .withColumn("event", from_json(col("raw_event"), ORDER_SCHEMA))
        .select(
            "kafka_key",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "ingested_at",
            "event.*",
        )
        .withColumn("event_time", to_timestamp(col("event_time")))
    )

    valid_orders = parsed.filter(
        col("event_id").isNotNull()
        & col("order_id").isNotNull()
        & col("category").isNotNull()
        & (col("quantity") > 0)
        & (col("unit_price") > 0)
        & col("event_time").isNotNull()
    )

    query = (
        valid_orders.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("Silver streaming query started. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
