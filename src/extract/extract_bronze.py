import logging
import requests
import os
import sys

from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType


# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)


# Spark Session
spark = SparkSession.builder.appName('open-brewery-bronze-extract').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Config
URL = 'https://api.openbrewerydb.org/v1/breweries'
SNAPSHOT_DATE = sys.argv[1]
BRONZE_BASE_PATH = '/app/data/bronze/open_brewery_db'
BRONZE_PATH = f'{BRONZE_BASE_PATH}/snapshot_date={SNAPSHOT_DATE}'

os.makedirs(BRONZE_BASE_PATH, exist_ok=True)

# Extração
page = 1
all_records = []

extracted_at = datetime.now(timezone.utc).isoformat()
source = URL

logger.info('Iniciando extração da Open Brewery DB API')

while True:
    logger.info(f'Buscando página {page}')

    response = requests.get(
        url=URL,
        params={'page': page, 'per_page': 200},
        timeout=30
    )
    response.raise_for_status()

    data = response.json()

    if not data:
        logger.info('Nenhum dado retornado. Fim da paginação.')
        break

    logger.info(f'Página {page} retornou {len(data)} registros')

    enriched_page = [
        {
            **record,
            'extracted_at': extracted_at,
            'source': source,
            'page': page
        }
        for record in data
    ]

    all_records.extend(enriched_page)
    page += 1

logger.info(f'Total de registros extraídos: {len(all_records)}')

if len(all_records) == 0:
    logger.error('A camada bronze está vazia. Nenhum registro foi extraído da API.')
    spark.stop()
    raise ValueError('A camada bronze está vazia. Nenhum registro foi extraído da API.')

# Criação camada Bronze

schema_bronze = StructType([
    StructField('id', StringType(), True),
    StructField('name', StringType(), True),
    StructField('brewery_type', StringType(), True),
    StructField('address_1', StringType(), True),
    StructField('address_2', StringType(), True),
    StructField('address_3', StringType(), True),
    StructField('city', StringType(), True),
    StructField('state_province', StringType(), True),
    StructField('postal_code', StringType(), True),
    StructField('country', StringType(), True),
    StructField('longitude', StringType(), True),
    StructField('latitude', StringType(), True),
    StructField('phone', StringType(), True),
    StructField('website_url', StringType(), True),
    StructField('state', StringType(), True),
    StructField('street', StringType(), True),
    StructField('source', StringType(), True),
    StructField('page', IntegerType(), True),
    StructField('extracted_at', StringType(), True)
])

df_bronze = spark.createDataFrame(all_records, schema=schema_bronze)
logger.info('DataFrame Spark criado com sucesso')
logger.info(f'Quantidade de registros no DataFrame: {df_bronze.count()}')

# Escrita da Bronze

df_bronze.write.mode('overwrite').json(BRONZE_PATH)

logger.info(f'Camada bronze salva em: {BRONZE_PATH}')

spark.stop()
logger.info('Processo finalizado com sucesso')