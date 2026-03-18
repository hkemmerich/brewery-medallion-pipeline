import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, upper, initcap, regexp_replace, when, to_timestamp, current_timestamp, lit
from pyspark.sql.types import DoubleType, IntegerType

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

# Config
RUN_TS = sys.argv[1]

BRONZE_PATH = f'/app/data/bronze/open_brewery_db/run_ts={RUN_TS}'
SILVER_BASE_PATH = '/app/data/silver/breweries'
SILVER_PATH = f'{SILVER_BASE_PATH}/run_ts={RUN_TS}'

os.makedirs(SILVER_BASE_PATH, exist_ok=True)

# Spark Session
spark = SparkSession.builder.appName('open-brewery-silver-transform').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

logger.info('Iniciando transformação da camada silver')

# Leitura da bronze
df_bronze = spark.read.json(BRONZE_PATH)

logger.info('Camada bronze lida com sucesso')
logger.info(f'Quantidade de registros lidos da bronze: {df_bronze.count()}')


# Criação camada Silver
df_silver = (
    df_bronze
    .select(
        col('id').alias('brewery_id'),
        trim(col('name')).alias('name'),
        lower(trim(col('brewery_type'))).alias('brewery_type'),
        trim(col('address_1')).alias('address_1'),
        trim(col('address_2')).alias('address_2'),
        trim(col('address_3')).alias('address_3'),
        initcap(trim(col('city'))).alias('city'),
        initcap(trim(col('state_province'))).alias('state_province'),
        trim(col('postal_code')).alias('postal_code'),
        upper(col('country')).alias('country'),
        col('longitude').cast(DoubleType()).alias('longitude'),
        col('latitude').cast(DoubleType()).alias('latitude'),
        regexp_replace(trim(col('phone')), r'[^0-9]', '').alias('phone'),
        trim(col('website_url')).alias('website_url'),
        initcap(trim(col('state'))).alias('state'),
        trim(col('street')).alias('street'),
        trim(col('source')).alias('source'),
        col('page').cast(IntegerType()).alias('page'),
        to_timestamp(col('extracted_at')).alias('extracted_at'),        
    )
    .withColumn(
        'state', 
        when(col('state').isNull() | (trim(col('state')) == ''), col('state_province'))
        .otherwise(col('state'))
    )
    .withColumn(
        'city',
        when(col('city').isNull() | (trim(col('city')) == ''), lit('Unknown'))
        .otherwise(col('city'))
    )
    .withColumn(
        'country',
        when(col('country').isNull() | (trim(col('country')) == ''), lit('Unknown'))
        .otherwise(col('country'))
    )
    .withColumn('ingestion_timestamp', current_timestamp())
    .dropDuplicates(['brewery_id'])
)


logger.info('Transformações aplicadas com sucesso')
silver_count = df_silver.count()
distinct_brewery_count = df_silver.select('brewery_id').distinct().count()

if silver_count == 0:
    logger.error('A camada silver está vazia.')
    spark.stop()
    raise ValueError('A camada silver está vazia.')

if silver_count != distinct_brewery_count:
    logger.error(
        f'A camada silver possui duplicidade por brewery_id. '
        f'Total: {silver_count} | Distintos: {distinct_brewery_count}'
    )
    spark.stop()
    raise ValueError('A camada silver possui duplicidade por brewery_id.')

logger.info(f'Quantidade de registros na camada silver: {silver_count}')
logger.info(f'Quantidade de brewery_id distintos: {distinct_brewery_count}')

# Escrita da silver

df_silver.write.mode('overwrite').partitionBy('country', 'state').parquet(SILVER_PATH)

logger.info(f'Camada silver salva em: {SILVER_PATH}')

spark.stop()
logger.info('Processo finalizado com sucesso')