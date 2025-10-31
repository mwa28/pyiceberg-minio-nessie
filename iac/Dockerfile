FROM docker.io/amazon/aws-cli:arm64

# Install dependencies using yum
RUN yum install -y \
  wget \
  unzip \
  docker \
  && yum clean all

# Install AWS SAM CLI (ARM64 version)
RUN wget --progress=dot:giga https://github.com/aws/aws-sam-cli/releases/latest/download/aws-sam-cli-linux-arm64.zip && \
  unzip aws-sam-cli-linux-arm64.zip -d sam-installation && \
  ./sam-installation/install && \
  rm -rf aws-sam-cli-linux-arm64.zip sam-installation

ENTRYPOINT ["sam"]
