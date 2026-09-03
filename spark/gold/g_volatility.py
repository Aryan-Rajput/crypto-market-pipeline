from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from utils.spark_session import get_spark_session

spark = get_spark_session("GoldVolatilityFeatures")
spark.sparkContext.setLogLevel("ERROR")

#CHANGED: read only the last 10 minutes, not the whole table
df = spark.read \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar-v3/silver-features/") \
    .filter(F.col("window_end") >= (F.current_timestamp() - F.expr("INTERVAL 10 MINUTES")))

#UNCHANGED: your existing rolling window logic
window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("window_end") \
    .rowsBetween(-4, 0)

vol_df = df \
    .withColumn("log_return", F.log(F.col("last_price") / F.col("first_price"))) \
    .withColumn("momentum", F.col("last_price") - F.col("first_price")) \
    .withColumn("momentum_pct", (F.col("last_price") - F.col("first_price")) / F.col("first_price") * 100) \
    .withColumn("volatility", F.stddev("log_return").over(window_spec)) \
    .withColumn("price_range", F.col("max_price") - F.col("min_price")) \
    .withColumn("direction",
        F.when(F.col("momentum") > 0, "UP")
         .when(F.col("momentum") < 0, "DOWN")
         .otherwise("FLAT"))

#NEW: trim down to only the last 5 minutes before writing
output_df = vol_df.filter(
    F.col("window_end") >= (F.current_timestamp() - F.expr("INTERVAL 5 MINUTES"))
)
#NEW: write to Delta Lake with MERGE INTO
gold_path = "s3a://crypto-pipeline-ar-v3/gold/volatility-features/"

if DeltaTable.isDeltaTable(spark, gold_path):
    gold_table = DeltaTable.forPath(spark, gold_path)
    gold_table.alias("target").merge(
        output_df.alias("source"),
        "target.symbol = source.symbol AND target.window_start = source.window_start"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    #first-ever run — table doesn't exist yet, just create it normally
    output_df.write.format("delta").option("path", gold_path).save()

print("Volatility features merged successfully.")