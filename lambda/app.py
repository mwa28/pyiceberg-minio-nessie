import os

import boto3
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog


def lambda_handler(event, context):
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
    local_path = key.split("/")[-1]
    s3.download_file(bucket, key, local_path)

    df = pd.read_json(local_path, lines=True)

    # Calculate the difference from previous timestamp
    df["diff_ts"] = df.groupby(["controller_id", "parameter"])["timestamp"].diff()

    catalog = load_catalog(
        "rest",
        **{
            "uri": "http://localhost:19120/iceberg",
            "warehouse": "s3a://iceberg-datalake",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        },
    )

    catalog.create_namespace_if_not_exists("iceberg_db")

    table = catalog.load_table("iceberg_db.sensor_historical_data")
    to_append = pa.Table.from_pandas(df, schema=table.schema().as_arrow())

    with table.transaction() as trx:
        trx.append(to_append)

    return {"statusCode": 200, "file": key}
