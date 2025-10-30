import os
import boto3
import duckdb
from pyiceberg.catalog import load_catalog

def lambda_handler(event, context):
    # Extract filename from S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    print(f"Processing file: {key}")

    # Read file from MinIO (simulated S3)
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_ENDPOINT'),
        aws_access_key_id=os.getenv('MINIO_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('MINIO_SECRET_KEY'),
    )
    file_obj = s3.get_object(Bucket=bucket, Key=key)
    data = file_obj['Body'].read().decode('utf-8')

    # Transform with DuckDB
    conn = duckdb.connect()
    conn.execute("CREATE OR REPLACE TABLE temp_data AS SELECT * FROM read_json(?)", [data])
    transformed = conn.execute("SELECT *, current_timestamp() as ingest_time FROM temp_data").fetchdf()


    catalog = load_catalog(
        "rest",
        **{
            "uri": "http://localhost:19120/iceberg",
            "warehouse": "s3a://iceberg-datalake",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "minioadmin",
            "s3.secret-access-key": "minioadmin",
            "s3.path-style-access": "true",
        }
    )

    catalog.create_namespace_if_not_exists("iceberg_db")

   table = catalog.load_table("my_namespace.sensor_data")
    with table.update_schema() as update:
        update.add_column("ingest_time", "timestamp")
    with table.update_mode("append") as writer:
        writer.write_df(transformed)

    return {"status": "success", "file": key}
