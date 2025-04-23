# Use Ubuntu for better compatibility with Spark and Hadoop tools
FROM ubuntu:22.04

# Define build arguments
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3.3.6
ARG SPARK_UID=185
ARG SPARK_GID=999

# Environment variables
ENV SPARK_HOME=/opt/spark \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
    SPARK_CONF_DIR=/opt/spark/conf \
    PATH="/opt/spark/bin:/opt/hadoop/bin:$PATH" \
    PYTHONUNBUFFERED=1

# Install system dependencies and Python 3
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
    bash curl wget openjdk-11-jdk libstdc++6 krb5-user libnss3 tini \
    python3 python3-pip python3-setuptools && \
    rm -rf /var/lib/apt/lists/*

# Make python3 the default
RUN ln -s /usr/bin/python3 /usr/bin/python && python3 --version

# Ensure Spark finds tini at the expected path (Spark uses /sbin/tini)
RUN ln -s /usr/bin/tini /sbin/tini

# Download and install Spark (without Hadoop)
RUN wget -O /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz" && \
    tar -xzf /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-without-hadoop ${SPARK_HOME} && \
    rm -f /tmp/spark-${SPARK_VERSION}-bin-without-hadoop.tgz

# Download and install Hadoop
RUN wget -O /tmp/hadoop-${HADOOP_VERSION}.tar.gz \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && \
    tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    ln -s /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm -f /tmp/hadoop-${HADOOP_VERSION}.tar.gz

# Set Spark classpath
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/tools/lib/*"

# Install Python dependencies
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir pygeohash requests opencage

# Copy files to image
COPY ./entrypoint.sh /opt/entrypoint.sh
COPY ./dist/sparkbasics-*.egg /opt/
COPY ./src /opt/src

# Set file permissions
RUN chmod +x /opt/entrypoint.sh && \
    chmod 644 /opt/sparkbasics-*.egg

# Create Spark user
RUN groupadd -g $SPARK_GID spark && \
    useradd -m -u $SPARK_UID -g $SPARK_GID spark

# Set working directory
WORKDIR ${SPARK_HOME}/work-dir

# Fix permissions for working directories
RUN mkdir -p ${SPARK_HOME}/work-dir && \
    chown -R spark:spark ${SPARK_HOME}/work-dir /opt/src /opt/entrypoint.sh /opt/sparkbasics-*.egg

# Switch to non-root user
USER ${SPARK_UID}

# Entrypoint
ENTRYPOINT ["/usr/bin/tini", "--", "/opt/entrypoint.sh"]

