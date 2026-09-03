from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable
import os

from spark.utils.spark_session import get_spark_session

spark = get_spark_session("GoldCrossAsset")

spark.sparkContext.setLogLevel("ERROR")


df = spark.read \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar-v3/gold/ofi-features/") \
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


gold_path = "s3a://crypto-pipeline-ar-v3/gold/cross-asset-signal/"

if DeltaTable.isDeltaTable(spark, gold_path):
    gold_table = DeltaTable.forPath(spark, gold_path)
    gold_table.alias("target").merge(
        output_df.alias("source"),
        "target.window_start = source.window_start"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    output_df.write.format("delta").option("path", gold_path).save()

print("Cross-asset signal merged successfully.")