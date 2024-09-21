FROM apache/airflow:2.9.3-python3.9

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

ARG SPARK_DOWNLOAD_URL=https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3.tgz

RUN curl -fL ${SPARK_DOWNLOAD_URL} -o /tmp/spark && \
    tar -xvf /tmp/spark -C /tmp && \
    mv /tmp/spark-3.5.2-bin-hadoop3 /opt/spark


# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH


USER airflow


COPY ./requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
