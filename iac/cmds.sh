podman run --rm -it \
        -e MINIO_ENDPOINT=http://localhost:9000 \
        -e MINIO_ACCESS_KEY=minioadmin \
        -e MINIO_SECRET_KEY=minioadmin \
        -e NESSIE_ENDPOINT=http://localhost:19120/iceberg \
        --network host \
        localhost/iot-file-processor:latest

curl -XPOST "http://localhost:8080/2015-03-31/functions/function/invocations" -d @test/event.json

docker-compose exec spark /opt/spark/bin/spark-submit \
        --master spark://spark:7077 \
        --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions" \
        --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
        --conf spark.sql.catalog.nessie.ref=main \
        --conf spark.sql.catalog.nessie.authentication.type=NONE \
        --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
        --conf spark.sql.catalog.nessie.s3.endpoint=http://minio:9000 \
        --conf spark.sql.catalog.nessie.warehouse=s3://warehouse/ \
        --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
        --conf spark.hadoop.fs.s3a.connection.establish.timeout=60000 \
        /opt/spark/work-dir/batch_ingest.py
