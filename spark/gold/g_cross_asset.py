from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

from spark.utils.spark_session import get_spark_session

spark = get_spark_session("GoldCrossAsset")

spark.sparkContext.setLogLevel("ERROR")


df = spark.read \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar/gold/ofi-features/") \
    .filter(F.col("window_end") >= (F.current_timestamp() - F.expr("INTERVAL 5 MINUTES")))

btc_df = df.filter(F.col("symbol") == "BTCUSDT") \
    .select(
        "window_start", "window_end",
        F.col("ofi_norm").alias("btc_ofi_norm")
    )

eth_df = df.filter(F.col("symbol") == "ETHUSDT") \
    .select(
        "window_start", "window_end",
        F.col("ofi_norm").alias("eth_ofi_norm")
    )

output_df = btc_df.join(eth_df, on="window_start", how="inner") \
    .select(
        btc_df["window_start"],
        btc_df["window_end"],
        "btc_ofi_norm",
        "eth_ofi_norm",
        (F.col("btc_ofi_norm") - F.col("eth_ofi_norm")).alias("divergence")
    )
