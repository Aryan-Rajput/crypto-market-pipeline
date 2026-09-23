from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from spark.utils.spark_session import get_spark_session

spark = get_spark_session("GoldVolatilityFeatures")
spark.sparkContext.setLogLevel("ERROR")

silver_path = "s3a://crypto-pipeline-ar-v3/silver-features/"
vol_path = "s3a://crypto-pipeline-ar-v3/gold/volatility-features/"

# buffer time in minutes to account for prev 4 rows are taken in.
BF = 10

# this logic finds a single <last processed timestamp> across both btcusdt and ethusdt because 
# agg(F.max(...)) is calculated without grouping by symbol that single timestamp is then used
# to filter both symbols during the next run
#
# 
# btc and eth data are not always writen to Silver at exactly the same time differences in trade
# activity, Spark micro-batch scheduling, and processing delays can cause slight 
# timing variations betwen symbols.
#
# for example, btcs latest record may have a window_start of 10:00 while eths latest processed record is only at 9:57.
#
# the old logic takes the global maximum timestamp (10:00 from btc) and stores it as <last_window>. 
# during the next run, both symbols are filtered using <window_start > 10:00>.
#
# the issue is that eth may still have valid, unprocessed records betwen 9:57 and 10:00. 
# since the checkpoint already asumes everything up to 10:00 has ben procesed
# those eth records are skipped permanently.
#
# once skipped, this script can never recover those records. future runs kep moving the checkpoint
# forward, causing the gap betwen eth's actual last procesed row and its next procesed row to grow.
# this is likely the reasonwe have observed 100+ minute gaps in the dataset
#
# It also explains why similar gap sizes appeared in both symbols during our analysis
# because the checkpoint was shared, whichever symbol happened to be slightly behind at a given
# moment was at risk of losing data.
#
# the fix --
# maintain a separate <last procesed timestamp> for each symbol this will ensure that the 
# btc and eth track their progres independently which will prevent one symbols checkpoint from
# afecting or hiding data from the other
try:
    vol_table = DeltaTable.forPath(spark, vol_path)
    last_windows_df = vol_table.toDF() \
        .groupBy("symbol") \
        .agg(F.max("window_start").alias("last_window"))
    # last_windows_df now looks something like:
    #   symbol     | last_window
    #   BTCUSDT    | 2026-09-23 10:00:00
    #   ETHUSDT    | 2026-09-23 09:57:00
    # -- each symbol's OWN progress, not a shared blended value.
    has_existing_data = True
except:
    last_windows_df = None
    has_existing_data = False

df = spark.read.format("delta").load(silver_path)

if has_existing_data:
    df = df.join(last_windows_df, on="symbol", how="left")
    df = df.withColumn(
        "buffered_start",
        F.col("last_window") - F.expr(f"INTERVAL {BF} MINUTES")
    )
    df = df.filter(
        # if a symbol has no checkpoint yet (shouldn't normally happen once has_existing_data is True
        # but kept as a safety net) don't filter it at all Otherwise, keep rows from 
        # (checkpoint - buffer) onward
        F.col("last_window").isNull() | (F.col("window_start") > F.col("buffered_start"))
    ).drop("last_window", "buffered_start")
# else: has_existing_data is False meaning this is a totally fresh table. We just use the full Silver dataset with no 
# filtering at all -- there's nothing to buffer against yet.

window_spec = Window.partitionBy("symbol").orderBy("window_end").rowsBetween(-4, 0)
lag_window = Window.partitionBy("symbol").orderBy("window_end")

vol_df = df \
    .withColumn("log_return", F.log(F.col("last_price") / F.col("first_price"))) \
    .withColumn("momentum", F.col("last_price") - F.col("first_price")) \
    .withColumn("momentum_pct", (F.col("last_price") - F.col("first_price")) / F.col("first_price") * 100) \
    .withColumn("volatility_raw", F.stddev("log_return").over(window_spec)) \
    .withColumn("window_start_4_back", F.lag("window_start", 4).over(lag_window)) \
    .withColumn("span_minutes",
                (F.col("window_start").cast("long") - F.col("window_start_4_back").cast("long")) / 60) \
    .withColumn(
        "volatility",
        F.when(F.col("span_minutes") <= 5.5, F.col("volatility_raw")).otherwise(F.lit(None))
    ) \
    .withColumn("price_range", F.col("max_price") - F.col("min_price")) \
    .withColumn("direction",
                F.when(F.col("momentum") > 0, "UP")
                 .when(F.col("momentum") < 0, "DOWN")
                 .otherwise("FLAT")) \
    .drop("volatility_raw", "window_start_4_back", "span_minutes")


# we read extra rows from silver further back from the checkpoint so the rolling window got actual data
# to work with so refiltering the redundant rows is necessary to avoid writing duplicates to the gold table
if has_existing_data:
    vol_df = vol_df.join(last_windows_df, on="symbol", how="left")
    vol_df = vol_df.filter(
        F.col("last_window").isNull() | (F.col("window_start") > F.col("last_window"))
    ).drop("last_window")


 # same as before, we only want to write new rows that are beyond the last processed timestamp for each symbol
if has_existing_data and DeltaTable.isDeltaTable(spark, vol_path):
    vol_table = DeltaTable.forPath(spark, vol_path)
    vol_table.alias("target").merge(
        vol_df.alias("source"),
        "target.window_start = source.window_start AND target.symbol = source.symbol"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    vol_df.write.format("delta").mode("overwrite").option("path", vol_path).save()

print("Volatility features written successfully.")