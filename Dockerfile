# Databricks Runtime 14.3 LTS Compatible Environment
# Based on: https://docs.databricks.com/aws/en/release-notes/runtime/14.3lts

FROM python:3.10.12-slim-bookworm

# Install Java 8 (Zulu) - matches Databricks Runtime 14.3 LTS
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        gnupg \
        curl \
        wget \
        procps \
        git \
    && curl -s https://repos.azul.com/azul-repo.key | gpg --dearmor -o /usr/share/keyrings/azul.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" > /etc/apt/sources.list.d/zulu.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends zulu8-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/zulu8
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Spark 3.5.0 (matches DBR 14.3 LTS)
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Python packages matching Databricks Runtime 14.3 LTS
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Create workspace directory
WORKDIR /workspace

# Expose Spark UI ports
EXPOSE 4040 4041 8080 8081 7077 8888

CMD ["bash"]
