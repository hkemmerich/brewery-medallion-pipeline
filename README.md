# Pipeline Medallion de Cervejarias

## Visão Geral
Este projeto implementa um pipeline de dados em lote que ingere dados de cervejarias da [Open Brewery DB API](https://api.openbrewerydb.org/v1/breweries), processa esses dados por meio de uma **Arquitetura Medallion** (camadas **Bronze**, **Silver** e **Gold**) e orquestra o fluxo de trabalho com **Apache Airflow**. A solução é totalmente conteinerizada com **Docker** e **Docker Compose**, utilizando **PySpark** para o processamento de dados.

O pipeline foi projetado para ser fácil de reproduzir localmente, ao mesmo tempo em que segue padrões que podem ser estendidos para ambientes em nuvem.

---

## Arquitetura

### Fonte
- **Open Brewery DB API**
- Endpoint utilizado: `https://api.openbrewerydb.org/v1/breweries`
- A extração de dados é paginada utilizando os parâmetros `page` e `per_page`.

### Camadas

#### Bronze
Objetivo:
- Armazenar os dados brutos da API com mudanças mínimas
- Preservar o esquema original o máximo possível
- Adicionar metadados técnicos de ingestão

O que acontece nesta camada:
- Os dados são extraídos da API página por página
- Três campos técnicos de metadados são adicionados:
  - `extracted_at`
  - `source`
  - `page`
- O resultado é gravado como JSON em um caminho versionado usando `snapshot_date`

Validação implementada:
- A camada Bronze não pode estar vazia

#### Silver
Objetivo:
- Limpar e padronizar os dados
- Converter strings brutas em tipos analíticos
- Remover duplicatas
- Preparar os dados para agregação posterior

Transformações aplicadas:
- Renomeado `id` para `brewery_id`
- Removidos espaços extras das colunas de texto
- Padronização do uso de maiúsculas e minúsculas no texto
- Conversão de `latitude` e `longitude` para `double`
- Conversão de `page` para `integer`
- Conversão de `extracted_at` para timestamp
- Remoção de caracteres não numéricos de `phone`
- Preenchimento de valores ausentes de localização quando apropriado
- Adicionado `ingestion_timestamp`
- Removidas duplicatas usando `brewery_id`

Formato de armazenamento:
- Parquet
- Particionado por `country` e `state`

Validações implementadas:
- Silver não pode estar vazia
- Silver não pode conter valores duplicados de `brewery_id`

#### Gold
Objetivo:
- Entregar um conjunto de dados agregado pronto para o negócio

Agregação aplicada:
- Contagem de cervejarias por:
  - `country`
  - `state`
  - `city`
  - `brewery_type`

Saída gerada:
- `brewery_count`
- `created_at`

Formato de armazenamento:
- Parquet

Validações implementadas:
- Gold não pode estar vazia
- `brewery_count` deve ser maior que zero

---

## Estrutura do Projeto

```text
brewery-case/
├── dags/
│   └── pipeline_dag_brewery.py
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── docker/
│   └── airflow/
│       ├── Dockerfile
│       └── requirements.txt
├── src/
│   ├── extract/
│   │   └── extract_bronze.py
│   └── transform/
│       ├── transform_silver.py
│       └── aggregate_gold.py
├── .dockerignore
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Orquestração

O pipeline é orquestrado pelo Airflow usando uma DAG chamada:

- `brewery_medallion_pipeline`

Ordem de execução:
1. `extract_bronze`
2. `transform_silver`
3. `aggregate_gold`

A estratégia recomendada é passar `{{ ds }}` do Airflow para cada script. Esse valor é usado como `snapshot_date`, permitindo que cada execução diária do pipeline grave em um diretório único por dia.

Essa estratégia versionada evita conflitos de sobrescrita, preserva o histórico de execuções e melhora a rastreabilidade. Além disso, facilita rollback para o snapshot do dia anterior caso uma execução mais recente apresente problemas.

---

## Por Que o Docker Foi Utilizado

A conteinerização foi incluída para melhorar:
- reprodutibilidade
- modularidade
- facilidade de configuração para avaliadores
- consistência do ambiente entre máquinas

---

## Escolhas de Design

### 1. Sistema de arquivos local como data lake
Eu escolhi armazenar Bronze, Silver e Gold localmente em `data/` porque isso torna a solução fácil de executar e avaliar sem exigir credenciais de nuvem.

### 2. Airflow para orquestração
O Airflow foi selecionado porque o case valoriza explicitamente orquestração, tentativas de repetição, modularização e agendamento.

### 3. PySpark para transformações
O PySpark foi usado para implementar as transformações, o tratamento de esquemas, as verificações de qualidade de dados e a lógica de agregação.

### 4. Diretórios versionados por `snapshot_date`
A solução foi desenhada para manter um snapshot diário das camadas Bronze, Silver e Gold. Em vez de sobrescrever sempre o mesmo diretório, cada execução grava em uma pasta versionada por data, o que reduz problemas operacionais e facilita a recuperação do estado anterior.

### 5. Bronze mantida o mais próxima possível dos dados brutos
A camada Bronze preserva a resposta da API com apenas os metadados técnicos de ingestão adicionados. A conversão de tipos e a normalização foram intencionalmente adiadas para a Silver.

---

## Trade-offs

### Simplicidade vs. implementação cloud-native
A solução atual roda localmente usando Docker em vez de exigir serviços em nuvem. Isso reduz o atrito de configuração e facilita a avaliação.

Trade-off:
- mais fácil de executar localmente
- menos parecida com produção do que usar armazenamento em nuvem diretamente

### PySpark localmente vs. Spark lendo armazenamento em nuvem diretamente
O projeto usa PySpark localmente por simplicidade. Leituras e escritas diretas em armazenamento de objetos na nuvem exigiriam configuração adicional de conectores do Spark.

Trade-off:
- configuração local mais simples
- etapa adicional de integração com nuvem necessária para produção

### Metadados do Airflow em Postgres vs. dados do pipeline em um banco de dados
O banco Postgres incluído armazena apenas os metadados do Airflow. Os dados do pipeline em si são armazenados como arquivos nas camadas Medallion.

Trade-off:
- separação mais clara entre metadados de orquestração e dados do pipeline
- nenhum banco de serving direto incluído na implementação local atual

---


## Estratégia de Monitoramento e Alertas

A implementação inclui controles operacionais e de qualidade de dados.

### Monitoramento operacional
O Airflow já fornece:
- status em nível de tarefa
- tentativas de repetição
- logs
- histórico de execuções da DAG
- visibilidade de falhas por tarefa

Estratégia atual de repetição:
- tentativas de repetição configuradas na DAG
- atraso de repetição de 2 minutos

### Monitoramento da qualidade de dados
Os scripts incluem validações para cada camada:

#### Bronze
- o pipeline falha se nenhum registro for extraído

#### Silver
- o pipeline falha se a Silver estiver vazia
- o pipeline falha se forem encontrados valores duplicados de `brewery_id`

#### Gold
- o pipeline falha se a Gold estiver vazia
- o pipeline falha se qualquer `brewery_count <= 0`

### Recomendação de alertas
Se esta solução fosse reforçada ainda mais, eu adicionaria:
- alertas por e-mail em falhas da DAG
- notificações no Slack ou Teams em falhas de tarefa
- detecção de anomalias para quedas bruscas de volume entre execuções
- tarefas dedicadas de verificação de qualidade ou integração com Great Expectations / Soda

---

## Testes Automatizados

O projeto inclui testes automatizados implementados com `pytest` para validar regras-chave de negócio e de qualidade dos dados:

- Validação da Bronze: garante que os registros extraídos não estejam vazios

- Validação da Silver: garante a remoção de duplicidades e que a saída não esteja vazia

- Validação da Gold: garante a agregação correta e que brewery_count seja positivo

Esses testes se concentram na lógica central dos dados do pipeline e ajudam a verificar a consistência das camadas da Arquitetura Medalhão.

---

## Migração Planejada para a Nuvem

A solução atual foi intencionalmente projetada para rodar localmente, mas pode ser adaptada para armazenamento em nuvem e serviços analíticos.

### GCP
A evolução em nuvem mais simples para este projeto é a **Google Cloud Platform**, usando:
- **Google Cloud Storage (GCS)** para Bronze, Silver e Gold
- **BigQuery** para servir o dataset final da Gold

### Arquitetura-alvo recomendada
```text
API -> GCS Bronze -> GCS Silver -> GCS Gold -> BigQuery
```

### Por que o GCP é uma boa escolha
- O GCS se encaixa naturalmente no layout atual de `data/bronze`, `data/silver` e `data/gold`
- O BigQuery é um destino analítico natural para a camada Gold
- O padrão de orquestração com Airflow permanece o mesmo

#### Migração mais simples
Manter o processamento local com PySpark e adicionar:
- upload de Bronze, Silver e Gold para o GCS
- carga da Gold no BigQuery

Benefícios:
- mudanças mínimas no pipeline atual
- nenhuma complexidade com conectores do Spark

### Publicação no BigQuery
Quando a Gold estiver pronta, uma tarefa extra pode publicá-la no BigQuery:

```text
extract_bronze -> transform_silver -> aggregate_gold -> load_gold_to_bigquery
```

Caminhos equivalentes no GCP:
- `gs://<bucket>/bronze/...`
- `gs://<bucket>/silver/...`
- `gs://<bucket>/gold/...`

Destino final de serving:
- `<gcp_project>.<dataset>.breweries_by_type_location`

---

## Melhorias Futuras

Próximos passos potenciais para este projeto:
- publicar Bronze, Silver e Gold no Google Cloud Storage
- carregar a Gold no BigQuery
- adicionar verificações de qualidade de dados como tarefas separadas do Airflow
- adicionar alertas via Slack ou e-mail
- parametrizar variáveis de ambiente com `.env`
- expor métricas de qualidade e resumos de execução em uma tabela de manifesto ou dashboard de monitoramento

---