# Образ для мастера, воркера и Jupyter: Spark 3.5.3 + драйвер S3 (s3a) + Jupyter.
FROM apache/spark:3.5.3

USER root

# Драйвер s3a: hadoop-aws 3.3.4 и модули AWS SDK 1.12.262 (под Hadoop 3.3.4 внутри Spark 3.5.3).
# Лежат в проекте, чтобы сборка не зависела от Maven Central.
COPY jars/*.jar /opt/spark/jars/

RUN python3 -m pip install --no-cache-dir --default-timeout=100 notebook==7.2.2 ipykernel==6.29.5

RUN mkdir -p /home/spark/work && chown -R spark:spark /home/spark
ENV HOME=/home/spark
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip

USER spark
WORKDIR /home/spark/work
