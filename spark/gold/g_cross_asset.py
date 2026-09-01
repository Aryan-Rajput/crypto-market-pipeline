from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

from spark.utils.spark_session import get_spark_session

spark = get_spark_session("GoldCrossAsset")

spark.sparkContext.setLogLevel("ERROR")
