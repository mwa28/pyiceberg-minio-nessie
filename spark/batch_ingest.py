from fugue import transform
from pyspark import SparkConf
from pyspark.sql import SparkSession

from common.transform import calculate_delta

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Query the data
spark.sql("CREATE DATABASE icebergy")
