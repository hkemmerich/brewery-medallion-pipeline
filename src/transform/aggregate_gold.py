import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp


# Logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Config
RUN_TS = sys.argv[1]

SILVER_PATH = f'/app/data/silver/breweries/run_ts={RUN_TS}'
GOLD_BASE_PATH = '/app/data/gold/breweries_by_type_location'
GOLD_PATH = f'{GOLD_BASE_PATH}/run_ts={RUN_TS}'

os.makedirs(GOLD_BASE_PATH, exist_ok=True)

# Spark Session

spark = SparkSession.builder.appName('open-brewery-gold-aggregate').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

logger.info('Iniciando processamento da camada gold')

# Leitura camada Silver

df_silver = spark.read.parquet(SILVER_PATH)

logger.info('Camada silver lida com sucesso')
logger.info(f'Quantidade de registros na camada silver: {df_silver.count()}')


# Criação da camada Gold
df_gold = (
    df_silver
    .groupBy(
        col('country'),
        col('state'),
        col('city'),
        col('brewery_type')
    )
    .agg(
        count('brewery_id').alias('brewery_count')
    )
    .withColumn('created_at', current_timestamp())
)

logger.info('Agregação realizada com sucesso')
gold_count = df_gold.count()
invalid_brewery_count = df_gold.filter(col('brewery_count') <= 0).count()

if gold_count == 0:
    logger.error('A camada gold está vazia.')
    spark.stop()
    raise ValueError('A camada gold está vazia.')

if invalid_brewery_count > 0:
    logger.error(
        f'A camada gold possui {invalid_brewery_count} registros com brewery_count <= 0.'
    )
    spark.stop()
    raise ValueError('A camada gold possui contagens inválidas.')

logger.info(f'Quantidade de registros na camada gold: {gold_count}')
logger.info(f'Registros inválidos com brewery_count <= 0: {invalid_brewery_count}')


# Escrita gold

df_gold.write.mode('overwrite').parquet(GOLD_PATH)

logger.info(f'Camada gold salva em: {GOLD_PATH}')

spark.stop()
logger.info('Processo finalizado com sucesso')