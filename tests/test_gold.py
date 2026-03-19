from pyspark.sql.functions import col, count
import pytest


def validate_gold(df_gold):
    total = df_gold.count()
    invalid = df_gold.filter(col('brewery_count') <= 0).count()

    if total == 0:
        raise ValueError('A camada gold está vazia.')

    if invalid > 0:
        raise ValueError('A camada gold possui contagens inválidas.')

def test_gold_agg_corr(spark):
    data = [
        {'brewery_id': '1', 'country': 'US', 'state': 'California', 'city': 'San Diego', 'brewery_type': 'micro'},
        {'brewery_id': '2', 'country': 'US', 'state': 'California', 'city': 'San Diego', 'brewery_type': 'micro'},
        {'brewery_id': '3', 'country': 'US', 'state': 'California', 'city': 'San Diego', 'brewery_type': 'brewpub'},
    ]

    df = spark.createDataFrame(data)
    df_gold = df.groupby('country', 'state', 'city', 'brewery_type').agg(count('brewery_id').alias('brewery_count'))
    rows = df_gold.collect()

    result = {
        (row['country'], row['state'], row['city'], row['brewery_type']): row['brewery_count'] for row in rows
    }


    assert result['US', 'California', 'San Diego', 'micro'] == 2
    assert result['US', 'California', 'San Diego', 'brewpub'] == 1