CLAUDE.md — Guia para AI Assistants
Este arquivo fornece contexto para que assistentes de IA (como o Claude) possam ajudar de forma eficaz em tarefas relacionadas a este projeto.

O que é este projeto
Pipeline de dados para ingestão, transformação e análise das corridas de táxi amarelo e verde de Nova York (Janeiro a Maio de 2023), desenvolvido como case técnico para vaga de Data Engineer.
Implementa a arquitetura medalhão (Bronze → Prata → Ouro) sobre um Data Lake local usando Docker, MinIO, PySpark e Delta Lake.

Estrutura do projeto
ifood-case/
├── docker-compose.yml         # Infraestrutura: MinIO + Jupyter
├── src/
│   ├── 01_ingestion.py        # Camada Bronze: download + carga no MinIO
│   ├── 02_transformation.py   # Camada Prata: limpeza + unificação yellow/green
│   ├── 03_gold.py             # Camada Ouro: agregações prontas para consumo
│   └── pipeline.py            # Orquestrador sequencial do pipeline
├── analysis/
│   └── 04_analysis.ipynb      # Notebook com respostas e visualizações
├── landing_temp/              # Parquets originais da NYC TLC (não versionados)
├── CLAUDE.md                  # Este arquivo
└── README.md                  # Documentação do projeto

Como rodar o projeto
Pré-requisitos

Docker Desktop instalado e em execução
8 GB de RAM disponível

Passo a passo
bash# 1. Suba a infraestrutura
docker compose up -d

# 2. Acesse o MinIO em http://localhost:9001 (admin / admin123)
#    Crie o bucket: ifood-data-lake

# 3. Pegue o token do Jupyter
docker logs ifood_jupyter 2>&1 | grep token       # Mac/Linux
docker logs ifood_jupyter 2>&1 | findstr token    # Windows

# 4. No terminal do Jupyter, rode o pipeline completo
python work/src/pipeline.py

# 5. Ou pule o download se os dados já existem em landing_temp/
python work/src/pipeline.py --skip-ingestion

# 6. Abra o notebook de análise
# analysis/04_analysis.ipynb

Camadas do Data Lake
Bronze — s3a://ifood-data-lake/bronze/
Dados brutos preservados exatamente como vieram da fonte. Nunca modificar diretamente.

bronze/yellow_taxi/ — corridas yellow com coluna taxi_type = "yellow"
bronze/green_taxi/  — corridas green com coluna taxi_type = "green"

Prata — s3a://ifood-data-lake/silver/all_taxi
Dados limpos, filtrados e unificados. Particionado por taxi_type.

Período: apenas Jan–Mai 2023
Outliers de duração removidos via critério IQR dinâmico
Colunas: VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime, taxi_type

Ouro — s3a://ifood-data-lake/gold/
Agregações pré-computadas. Leitura direta — sem necessidade de processar a Prata.

avg_total_by_month     — média do total_amount por mês (yellow taxi)
avg_passengers_by_hour — média de passageiros por hora em maio (todos os táxis)
trips_by_taxi_type     — volume e ticket médio por tipo e mês
top_hours_by_taxi_type — horários de pico por tipo de táxi


Convenções de código

Todos os scripts usam PySpark com Delta Lake
Configuração do Spark é sempre feita via SparkSession.builder
Leitura de tabelas Delta: spark.read.format("delta").load(caminho)
Escrita de tabelas Delta: df.write.format("delta").mode("overwrite").save(caminho)
Colunas de tempo são extraídas com F.month(), F.hour(), F.unix_timestamp()
Nomes de variáveis em snake_case, funções em português para clareza


Credenciais do ambiente local
ServiçoURLUsuárioSenhaMinIO Consolehttp://localhost:9001adminadmin123Jupyter Labhttp://localhost:8888—ver token nos logs

Perguntas frequentes para o AI
Como adicionar um novo mês de dados?
Atualize a lista MESES em 01_ingestion.py e ajuste DATA_FIM em 02_transformation.py. Depois rode o pipeline completo.
Como adicionar uma nova fonte de táxi (ex: FHV)?
Adicione a entrada no dicionário TAXI_TYPES em 01_ingestion.py, trate renomeações de colunas em ler_e_padronizar() no 02_transformation.py e adicione a nova tabela Gold em 03_gold.py.
Como consultar os dados sem o notebook?
No terminal do Jupyter, abra um shell Python e use:
pythonfrom pyspark.sql import SparkSession
spark = SparkSession.builder.appName("query").getOrCreate()
spark.read.format("delta").load("s3a://ifood-data-lake/gold/avg_total_by_month").show()
Como regenerar apenas a camada Ouro sem reprocessar tudo?
bashpython work/src/03_gold.py
O que fazer se o Spark ficar sem memória?
Reduza spark.sql.shuffle.partitions para 4 ou diminua SPARK_WORKER_MEMORY no docker-compose.yml.

Contexto técnico importante

Os arquivos Parquet da NYC TLC de 2023 têm schemas inconsistentes entre meses — VendorID aparece como INT32 em alguns e BIGINT em outros. Isso é tratado com mergeSchema=true e enableVectorizedReader=false.
O green taxi usa lpep_pickup_datetime e lpep_dropoff_datetime em vez de tpep_*. A padronização acontece em ler_e_padronizar() no script de transformação.
O corte de duração por IQR é recalculado a cada execução da transformação — não há valores hardcoded.