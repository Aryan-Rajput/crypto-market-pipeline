from pyspark.sql import functions as F
from spark.utils.spark_session import get_spark_session
from delta.tables import DeltaTable


spark = get_spark_session("SilverLayer")
spark.sparkContext.setLogLevel('ERROR')

silver_path = "s3a://crypto-pipeline-ar-v3/silver-features/"

silver_df = spark.readStream \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar-v3/bronze-ticks/") \
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
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price")
    ) \
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

def upsert_to_silver(micro_batch_df, batch_id):
    # Called once per micro-batch instead of a plain append write
    # Runs a Delta MERGE -- matches existing rows on (symbol,window_start)
    # - If a match exists: update it --> this one uses <whenMatchedUpdateAll>
    # - If no match exists: insert it as a new row --> uses <whenNotMatchedInsertAll>
    # this is what makes re-processing old data safe -- as in no duplicates
    # no matter how many times the same window gets recomputed
    
    if not DeltaTable.isDeltaTable(spark, silver_path):
        # first ever write -- table doesnt exist yet nothing to merge
        # against so just write it out directly this one time
        micro_batch_df.write.format("delta").mode("overwrite").option(
            "path", silver_path
        ).save()
        return
 
    target = DeltaTable.forPath(spark, silver_path)
    target.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.symbol = source.symbol AND target.window_start = source.window_start"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
 

silver_query = vwap_df.writeStream \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", "s3a://crypto-pipeline-ar-v3/silver-features/_checkpoints/") \
    .trigger(processingTime='30 seconds') \
    .start()
 
silver_query.awaitTermination()