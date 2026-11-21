def test_imports():
    import boto3  # noqa: F401
    import clickhouse_connect  # noqa: F401
    import confluent_kafka  # noqa: F401
    import pyspark

    assert pyspark.__version__ == "3.5.4"


def test_spark_runs(spark):
    assert spark.range(3).count() == 3
