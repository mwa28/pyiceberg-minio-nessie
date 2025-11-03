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
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )
    local_path = f"/tmp/{key.split('/')[-1]}"
    s3.download_file(bucket, key, local_path)
    df = pd.read_json(local_path, lines=True, convert_dates=False)

    # Calculate the difference from previous timestamp
    df["diff_ts"] = df.groupby(["controller_id", "parameter"])["timestamp"].diff()
    df["diff_ts"] = df["diff_ts"].fillna(df["timestamp"].astype("double"))

    # Load Nessie catalog with PyArrowFileIO
    catalog = load_catalog(
        "nessie",
        **{
            "uri": "http://nessie:19120/iceberg",
            # "warehouse": "s3://iceberg-datalake/",
            # "io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            # "pyarrow.s3.endpoint_override": os.getenv("MINIO_ENDPOINT").replace(
            #     "http://", ""
            # ),
            # "pyarrow.s3.access_key": os.getenv("AWS_ACCESS_KEY_ID"),
            # "pyarrow.s3.secret_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
            # "pyarrow.s3.region": "us-east-1",
            # "pyarrow.s3.scheme": "http",  # Use "https" if your MinIO endpoint uses HTTPS
        },
    )

    # Create namespace if not exists
    catalog.create_namespace_if_not_exists("iceberg_db")

    # Define schema with unique field IDs
    schema = Schema(
        NestedField(
            name="controller_id",
            field_id=1000,
            field_type=LongType(),
            required=True,
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
        NestedField(
            name="diff_ts", field_id=1005, field_type=LongType(), required=True
        ),
    )

    # Create table if not exists
    table = catalog.create_table_if_not_exists(
        "iceberg_db.sensor_historical_data",
        schema=schema,
        location="iceberg-datalake/iceberg_db/sensor_historical_data",
        properties={
            "write.format.default": "parquet",
            "write.parquet.compression-codec": "zstd",
        },
    )

    # Append data
    to_append = pa.Table.from_pandas(df, schema=table.schema().as_arrow())
    with table.transaction() as trx:
        trx.append(to_append)

    return {"statusCode": 200, "file": key}
