from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

spark = SparkSession.builder \
    .appName("SilverLayer") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

silver_df = spark.readStream \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar/bronze/") \
    .withColumn("event_time", F.from_unixtime(F.col("event_time") / 1000).cast("timestamp")) \
    .withColumn("trade_time", F.from_unixtime(F.col("trade_time") / 1000).cast("timestamp")) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("quantity", F.col("quantity").cast("double")) 

vwap_df = silver_df \
    .withWatermark("trade_time", "1 minute") \
    .groupBy(
        F.window("trade_time", "1 minute"),
        "symbol"
    ) \
    .agg(
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.sum("quantity").alias("total_quantity"),
        F.count("trade_id").alias("trade_count"),
        F.first("price").alias("first_price"),
        F.last("price").alias("last_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price")
    ) \
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "vwap",
        "total_quantity",
        "trade_count"
    )

# Separate VWAP results for BTCUSDT and ETHUSDT
vwap_btc_df = vwap_df.filter(F.col("symbol") == "BTCUSDT")
vwap_eth_df = vwap_df.filter(F.col("symbol") == "ETHUSDT")