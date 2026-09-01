from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

from spark.utils.spark_session import get_spark_session

load_dotenv()

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

spark = get_spark_session("GoldVolatilityFeatures")

df = spark.read \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar/silver-features/") 

window_spec = Window \
    .partitionBy("symbol") \
    .orderBy("window_end") \
    .rowsBetween(-4, 0)

vol_df = df \
    .withColumn("log_return",
        F.log(F.col("last_price") / F.col("first_price"))) \
    .withColumn("momentum", 
        F.col("last_price") - F.col("first_price")) \
    .withColumn("momentum_pct",
        (F.col("last_price") - F.col("first_price")) / F.col("first_price") * 100) \
    .withColumn("volatility", 
        F.stddev("log_return").over(window_spec)) \
    .withColumn("price_range", 
        F.col("max_price") - F.col("min_price")) \
    .withColumn("direction",
        F.when(F.col("momentum") > 0, "UP").when(F.col("momentum") < 0, "DOWN").otherwise("FLAT"))

vol_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", "s3a://crypto-pipeline-ar/gold/volatility-features/") \
    .save()

print("Volatility features written to S3 successfully.")