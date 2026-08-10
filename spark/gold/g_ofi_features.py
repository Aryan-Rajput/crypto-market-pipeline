from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

spark = SparkSession.builder \
    .appName("GoldOFIFeatures") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.caseSensitive", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

bronze_df = spark.readStream \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar/bronze-ticks/") \
    .withColumn("trade_time", F.from_unixtime(F.col("trade_time") / 1000).cast("timestamp"))

ofi_df = bronze_df \
    .withWatermark("trade_time", "1 minute") \
    .groupBy(
         F.window("trade_time", "1 minute"),
         "symbol"
    ) \
    .agg(
        (F.sum(F.when(F.col("is_market_maker") == False, F.col("quantity")))).alias("buy_volume"), 
        (F.sum(F.when(F.col("is_market_maker") == True, F.col("quantity")))).alias("sell_volume"),
     ) \
    .select(
         F.col("window.start").alias("window_start"),
         F.col("window.end").alias("window_end"),
         "symbol",
         "buy_volume",
         "sell_volume",
         (F.col("buy_volume") + F.col("sell_volume")).alias("total_volume"),
         (F.col("buy_volume") - F.col("sell_volume")).alias("ofi"),
         ((F.col("buy_volume")-F.col("sell_volume"))/ (F.col("buy_volume") + F.col("sell_volume"))).alias("ofi_norm")
    )

ofi_querry = ofi_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://crypto-pipeline-ar/gold/ofi-features/_checkpoints/") \
    .option("path", "s3a://crypto-pipeline-ar/gold/ofi-features/") \
    .start()

ofi_querry.awaitTermination()