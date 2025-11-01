# Use the AWS Lambda Python 3.13 ARM64 base image
FROM public.ecr.aws/lambda/python:3.13-arm64

# Create a directory for the non-root user
RUN mkdir -p /home/lambdauser && \
  chown -R 1000:1000 /home/lambdauser

# Install build tools and dependencies using dnf
RUN dnf install -y gcc gcc-c++ python3-devel openssl-devel bzip2-devel libffi-devel zlib-devel

# Switch to the non-root user
USER 1000

# Set the environment variable for user-specific Python packages
ENV PYTHONUSERBASE=/home/lambdauser/.local
ENV PATH=/home/lambdauser/.local/bin:$PATH

# Copy requirements file
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install Python dependencies
RUN pip install --upgrade pip && pip install --user -r requirements.txt --no-cache-dir

# Copy function code
COPY ingest.py ${LAMBDA_TASK_ROOT}

# Set the Lambda handler
CMD ["ingest.handler"]
