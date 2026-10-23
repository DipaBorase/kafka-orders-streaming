
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    round as spark_round,
    sum as spark_sum,
)

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SILVER_PATH = "s3a://orders/silver/orders"
GOLD_PATH = "s3a://orders/gold/category_sales"
CHECKPOINT_PATH = "/tmp/kafka-orders-checkpoints/gold"


def write_gold_batch(batch_df, batch_id):
    (
        batch_df
        .withColumn("updated_at", current_timestamp())
        .write
        .mode("overwrite")
        .parquet(GOLD_PATH)
    )
    print(f"Gold aggregation updated for batch {batch_id}")


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaOrdersGold")
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

    silver_schema = (
        spark.read
        .format("parquet")
        .load(SILVER_PATH)
        .schema
    )

    silver = (
        spark.readStream
        .schema(silver_schema)
        .format("parquet")
        .load(SILVER_PATH)
    )

    gold = (
        silver
        .groupBy("category")
        .agg(
            count("*").alias("total_orders"),
            spark_sum("quantity").alias("total_units_sold"),
            spark_round(
                spark_sum(col("quantity") * col("unit_price")), 2
            ).alias("total_sales"),
        )
    )

    query = (
        gold.writeStream
        .outputMode("complete")
        .foreachBatch(write_gold_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    print("Gold streaming query started. Press Ctrl+C to stop.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
