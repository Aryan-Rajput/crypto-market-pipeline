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

raw_silver_df = spark.readStream \
    .format("delta") \
    .load("s3a://crypto-pipeline-ar/bronze/") \
    .withColumn("event_time", F.from_unixtime(F.col("event_time") / 1000).cast("timestamp")) \
    .withColumn("trade_time", F.from_unixtime(F.col("trade_time") / 1000).cast("timestamp")) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("quantity", F.col("quantity").cast("double")) 

    