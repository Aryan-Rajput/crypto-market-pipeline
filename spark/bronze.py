from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, BooleanType

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

spark = SparkSession.builder.appName("BronzeLayer").getOrCreate()

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