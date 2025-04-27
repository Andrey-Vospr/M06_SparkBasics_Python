# --------------------------------------
# Updated Dockerfile for Spark Basics (Azure Ready)
# --------------------------------------

# Use Ubuntu base
FROM ubuntu:22.04

# Define build arguments
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3.3.6
ARG SPARK_UID=185
ARG SPARK_GID=999

# Set environment variables
ENV SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_CONF_DIR=/opt/spark/conf \
    PATH="/opt/spark/bin:/opt/hadoop/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Install OS packages and Python
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    bash curl wget openjdk-11-jdk libstdc++6 krb5-user libnss3 tini python3 python3-pip python3-setuptools && \
    rm -rf /var/lib/apt/lists/*

# Set python3 as default
RUN ln -s /usr/bin/python3 /usr/bin/python

# Link tini
RUN ln -s /usr/bin/tini /sbin/tini

# Download Spark without Hadoop
RUN wget -O /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" && \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-without-hadoop ${SPARK_HOME} && \
    rm -f /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz

# Download Hadoop
RUN wget -O /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz

# Install required Azure and Geohash jars manually
RUN wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/${HADOOP_VERSION}/hadoop-azure-${HADOOP_VERSION}.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/microsoft/azure/azure-storage/8.6.3/azure-storage-8.6.3.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/microsoft/azure/azure-data-lake-store-sdk/2.3.6/azure-data-lake-store-sdk-2.3.6.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/com/microsoft/azure/azure-keyvault-core/1.0.0/azure-keyvault-core-1.0.0.jar && \
    wget -P /opt/spark/jars/ https://repo1.maven.org/maven2/ch/hsr/geohash/1.4.0/geohash-1.4.0.jar

# Install Python packages
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir pygeohash requests opencage

# Copy entrypoint script and application code
COPY ./entrypoint.sh /opt/entrypoint.sh
COPY ./dist/sparkbasics-*.egg /opt/
COPY ./src /opt/src

# Set permissions
RUN chmod +x /opt/entrypoint.sh && chmod 644 /opt/sparkbasics-*.egg

# Create Spark user
RUN groupadd -g $SPARK_GID spark && \
    useradd -m -u $SPARK_UID -g $SPARK_GID spark

# Set working directory
WORKDIR ${SPARK_HOME}/work-dir

# Fix permissions
RUN mkdir -p ${SPARK_HOME}/work-dir && \
    chown -R spark:spark ${SPARK_HOME}/work-dir /opt/entrypoint.sh /opt/sparkbasics-*.egg

# Switch to non-root
USER ${SPARK_UID}

# Entrypoint
ENTRYPOINT ["/usr/bin/tini", "--", "bash", "/opt/entrypoint.sh"]




