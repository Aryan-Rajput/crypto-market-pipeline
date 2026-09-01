from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

def get_spark_session(app_name: str, extra_configs: dict = {}) -> SparkSession:
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')

    Builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.caseSensitive", "true")
        
    for key, value in extra_configs.items():
        Builder = Builder.config(key, value)

    return Builder.getOrCreate()