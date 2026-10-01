from pyspark.sql import functions as F
from spark.utils.spark_session import get_spark_session
from delta.tables import DeltaTable
spark = get_spark_session("SilverOHLCVFeatures")
spark.sparkContext.setLogLevel("ERROR")


SILVER_PATH = "s3a://crypto-pipeline-ar-v3/silver-features/"
BRONZE_PATH = "s3a://crypto-pipeline-ar-v3/bronze-ticks/"
# Same path as before rm-ed the old chekpoints from dir 
CHECKPOINT = "s3a://crypto-pipeline-ar-v3/silver-features/_checkpoints/"

# True  = backfill: read bronze in chunks, then STOP when caught up.
#         Safe to stop and re-run; the checkpoint remembers progress.
BACKFILL = True


reader = spark.readStream.format("delta")
if BACKFILL:
    # limiting each microbathc so it wont go over the limit 
    reader = reader.option("maxFilesPerTrigger", 500)

silver_df = (
    reader.load(BRONZE_PATH)
    .withColumn("trade_ms", F.col("trade_time").cast("long"))
    .withColumn("trade_id_num", F.col("trade_id").cast("long")) 
    .withColumn("event_time", F.from_unixtime(F.col("event_time") / 1000).cast("timestamp"))
    .withColumn("trade_time", F.from_unixtime(F.col("trade_time") / 1000).cast("timestamp"))
    .withColumn("price", F.col("price").cast("double"))
    .withColumn("quantity", F.col("quantity").cast("double"))
)

# Ordering key --> earliest/latest trade by (millisecond time, trade id)
order_key = F.struct("trade_ms", "trade_id_num")

vwap_df = (
    silver_df
    .withWatermark("trade_time", "1 minute")
    .groupBy(F.window("trade_time", "1 minute"), "symbol")
    .agg(
        (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
        F.sum("quantity").alias("total_quantity"),
        F.count("trade_id").alias("trade_count"),
        F.min_by("price", order_key).alias("first_price"),   # was F.first (order-unsafe)
        F.max_by("price", order_key).alias("last_price"),    # was F.last  (order-unsafe)
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "symbol",
        "vwap",
        "total_quantity",
        "trade_count",
        "first_price",
        "last_price",
        "max_price",
        "min_price"
    )
)


def upsert_to_silver(micro_batch_df, batch_id):
    # Idempotent write: update existing (symbol, window_start) rows, insert new ones.
    # Spark requires exactly these two arguments even though batch_id is unused.
    if micro_batch_df.isEmpty():
        return
    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        (
            DeltaTable.forPath(spark, SILVER_PATH).alias("t")
            .merge(
                micro_batch_df.alias("s"),
                "t.symbol = s.symbol AND t.window_start = s.window_start",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # first-ever write: nothing to merge against yet
        micro_batch_df.write.format("delta").mode("overwrite").save(SILVER_PATH)


trigger_args = {"availableNow": True} if BACKFILL else {"processingTime": "30 seconds"}

silver_query = (
    vwap_df.writeStream
    .foreachBatch(upsert_to_silver)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(**trigger_args)
    .start()
)

silver_query.awaitTermination()