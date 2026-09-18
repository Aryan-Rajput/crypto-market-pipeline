from pyspark.sql import functions as F
from delta.tables import DeltaTable

from spark.utils.spark_session import get_spark_session

spark = get_spark_session("GoldCrossAsset")

spark.sparkContext.setLogLevel("ERROR")

gold_path = "s3a://crypto-pipeline-ar-v3/gold/cross-asset-signal/"

# reading the new data according to the last window in the gold table 
# if the gold table does not exist we will read all the data from the source
# and then merge it with the gold table
try:
    signal_table = DeltaTable.forPath(spark, gold_path)
    last_window = signal_table.toDF().agg(F.max("window_start")).collect()[0][0]
except:
    last_window = None

df = spark.read \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar-v3/gold/ofi-features/")

if last_window:
    df = df.filter(F.col("window_start") > last_window)

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