from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

load_dotenv()

schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("t", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("T", LongType()),
    StructField("m", BooleanType())
])


rp_username = os.getenv('REDPANDA_USERNAME') 
rp_password = os.getenv('REDPANDA_PASSWORD')
rp_topic = os.getenv('REDPANDA_TOPIC')
rp_bootstrap = os.getenv('REDPANDA_BOOTSTRAP')
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

spark = SparkSession.builder \
    .appName("BronzeLayer") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", rp_bootstrap) \
    .option("subscribe", rp_topic) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-256") \
    .option("kafka.sasl.jaas", f"org.apache.kafka.common.security.scram.ScramLoginModule required username='{rp_username}' password='{rp_password}';") \
    .load()

parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_string") \
    .select(F.from_json("json_string", schema).alias("data")) 

flattened_df = parsed_df.select(
    F.col("data.e").alias("event_type"),
    F.col("data.E").alias("event_time"),
    F.col("data.s").alias("symbol"),
    F.col("data.t").alias("trade_id"),
    F.col("data.p").alias("price"),
    F.col("data.q").alias("quantity"),
    F.col("data.T").alias("trade_time"),
    F.col("data.m").alias("is_market_maker")
)
flat_df = flattened_df.withColumn(
    "event_date", 
    F.to_date((F.col("event_time") / 1000).cast("timestamp"))
)

flat_df.writeStream \
    .format("delta") \
    .option("path", "s3a://crypto-pipeline-ar/bronze-ticks/") \
    .option("checkpointLocation", "s3a://crypto-pipeline-ar/bronze-ticks/_checkpoints/") \
    .partitionBy("event_date") \
    .outputMode("append") \
    .start() \
    .awaitTermination()