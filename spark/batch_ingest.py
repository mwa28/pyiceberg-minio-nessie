import pyspark
from common.transform import calculate_delta
from fugue import transform
from pyspark.sql import SparkSession

## DEFINE SENSITIVE VARIABLES
CATALOG_URI = "http://nessie:19120/api/v1"  # Nessie Server URI
WAREHOUSE = "s3://warehouse/"  # Minio Address to Write to
STORAGE_URI = "http://minio:9000"  # Minio IP address from docker inspect

# Configure Spark with necessary packages and Iceberg/Nessie settings
conf = (
    pyspark.SparkConf()
    .setAppName("sales_data_app")
    # Include necessary packages
    .set(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8",
    )
    # Enable Iceberg and Nessie extensions
    .set(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    )
    # Configure Nessie catalog
    .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .set("spark.sql.catalog.nessie.uri", CATALOG_URI)
    .set("spark.sql.catalog.nessie.ref", "main")
    .set("spark.sql.catalog.nessie.authentication.type", "NONE")
    .set(
        "spark.sql.catalog.nessie.catalog-impl",
        "org.apache.iceberg.nessie.NessieCatalog",
    )
    # Set Minio as the S3 endpoint for Iceberg storage
    .set("spark.sql.catalog.nessie.s3.endpoint", STORAGE_URI)
    .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
    .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
)

# Start Spark session
spark = SparkSession.builder.config(conf=conf).getOrCreate()
sdf = spark.read.json("s3://iot-bucket/sensor-data.jsonl")

out = transform(
    sdf,
    calculate_delta,
    schema="*,diff_ts:long",
)
# out is a Spark DataFrame
out.show()
