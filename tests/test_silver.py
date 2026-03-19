import pytest


def validate_silver(df_silver):
    total = df_silver.count()
    diff_ids = df_silver.select('brewery_id').distinct().count()

    if total == 0:
        raise ValueError('A camada silver está vazia')
    
    if total != diff_ids:
        raise ValueError('A camada silver possui brewery_id duplicados.')


def test_silver_remove_duplicates(spark):
    data = [
        {'brewery_id': '1', 'name': 'A'},
        {'brewery_id': '1', 'name': 'A'}
    ]

    df = spark.createDataFrame(data)
    df_siver = df.dropDuplicates(['brewery_id'])

    assert df_siver.count() == 1