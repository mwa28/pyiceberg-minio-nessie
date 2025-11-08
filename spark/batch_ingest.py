from fugue import transform
from pyspark.sql import SparkSession

from common.transform import calculate_delta

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Read the data
df = spark.read.option("multiline", "true").json("s3://iot-bucket/sensor_data.jsonl")

df = transform(df, calculate_delta, schema="*,diff_ts:long")

df.createOrReplaceTempView("sensor_data_with_delta")

spark.sql(
    """
  MERGE INTO iceberg_db.sensor_historical_data AS target  
    USING sensor_data_with_delta AS source
    ON target.controller_id = source.controller_id 
       AND target.timestamp = source.timestamp
       AND target.parameter = source.parameter
    WHEN MATCHED THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
"""
)
