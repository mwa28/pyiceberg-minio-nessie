FROM apache/spark:3.5.0-python3
USER root

# Install dependencies
RUN apt-get update && apt-get install -y curl python3-venv && \
  mkdir -p /opt/spark/work-dir/common && \
  rm -rf /var/lib/apt/lists/*

# Download Hadoop AWS and AWS SDK v2 JARs
RUN curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
  curl -L -o /opt/spark/jars/aws-java-sdk-bundle-2.20.131.jar \
  https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar && \
  curl -L -o /opt/spark/jars/aws-java-sdk-url-connection-client-2.20.131.jar \
  https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.20.131/url-connection-client-2.20.131.jar && \
  curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar \
  https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.0/iceberg-spark-runtime-3.5_2.12-1.5.0.jar && \
  # curl -L -o /opt/spark/jars/nessie-spark-extensions-0.76.0.jar \
  # https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions_2.12/0.76.0/nessie-spark-extensions_2.12-0.76.0.jar
  curl -L -o /opt/spark/jars/nessie-spark-extensions_2.12-0.76.0.jar \
  https://repo1.maven.org/maven2/org/projectnessie/nessie-spark-extensions_2.12/0.76.0/nessie-spark-extensions_2.12-0.76.0.jar
USER spark

# Python virtual environment
RUN python3 -m venv /opt/spark/venv
ENV PATH=/opt/spark/venv/bin:$PATH
ENV PYTHONPATH=/opt/spark/work-dir

# Install Python dependencies
COPY spark/requirements.txt /tmp/
RUN /opt/spark/venv/bin/pip install --no-cache-dir packaging setuptools -r /tmp/requirements.txt

# Copy application code
COPY spark/batch_ingest.py /opt/spark/work-dir/
COPY common/transform.py /opt/spark/work-dir/common/

# Start Spark
CMD ["bash", "-c", "source /opt/spark/venv/bin/activate && /opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://spark:7077 && tail -f /opt/spark/logs/*"]
