import os

import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType


def handler(event, _):
    # Extract filename from S3 event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    print(f"Processing file: {key}")

    # Read file from MinIO (simulated S3)
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )
    local_path = f"/tmp/{key.split('/')[-1]}"
    s3.download_file(bucket, key, local_path)

    df = pd.read_json(local_path, lines=True)

    # Calculate the difference from previous timestamp
    df["diff_ts"] = df.groupby(["controller_id", "parameter"])["timestamp"].diff()

    catalog = load_catalog(
        "rest",
        **{
            "uri": "http://nessie:19120/iceberg",  # <-- Use the service name "nessie" inside Docker network
            "warehouse": "iceberg-datalake",  # <-- Just the warehouse name
            "s3.endpoint": "http://minio:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        },
    )

    catalog.create_namespace_if_not_exists("iceberg_db")

    schema = Schema(
        NestedField(
            name="controller_id", field_id=1000, field_type=StringType(), required=True
        ),
        NestedField(
            name="timestamp", field_id=1001, field_type=LongType(), required=True
        ),
        NestedField(
            name="parameter", field_id=1002, field_type=StringType(), required=True
        ),
        NestedField(
            name="value", field_id=1003, field_type=DoubleType(), required=True
        ),
        NestedField(name="unit", field_id=1004, field_type=StringType(), required=True),
    )
    catalog.create_table_if_not_exists(
        "iceberg_db.sensor_historical_data",
        schema=schema,
        location="iceberg-datalake/iceberg_db/sensor_historical_data",  # <-- Explicit location
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
        },
    )
    to_append = pa.Table.from_pandas(df, schema=table.schema().as_arrow())

    with table.transaction() as trx:
        trx.append(to_append)

    return {"statusCode": 200, "file": key}
