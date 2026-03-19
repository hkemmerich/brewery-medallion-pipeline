from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope='session')
def spark():
    spark = SparkSession.builder.appName('pytest_brewey').config('spark.sql.suffle.partition','2').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    yield spark
    spark.stop()
