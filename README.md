Case Técnico — Data Engineer iFood

Pipeline de dados para ingestão, transformação e análise das corridas de táxi de Nova York (Jan–Mai 2023)


Arquitetura
Este projeto implementa a arquitetura medalhão (Bronze → Prata → Ouro) sobre um Data Lake local, orquestrado pelo Apache Airflow e executado com PySpark e Delta Lake.
NYC TLC (fonte)
      │
      ▼
Bronze — dados brutos preservados (MinIO + Delta Lake)
      │
      ▼  PySpark + limpeza IQR dinâmico
      ▼
Prata — dados limpos e unificados (MinIO + Delta Lake)
      │
      ▼  PySpark agregações
      ▼
Ouro  — respostas prontas para consumo (MinIO + Delta Lake)
      │
      ├──► Apache Airflow (orquestração)
      └──► Jupyter Notebook (análise e visualizações)

Tecnologias utilizadas
FerramentaPapel no projetoPor que foi escolhidaDocker ComposeOrquestra todos os serviçosAmbiente reproduzível sem instalações locaisMinIOData Lake local (simula S3)API 100% compatível com AWS S3PySparkProcessamento e transformaçãoProcessa milhões de registros em paraleloDelta LakeFormato de armazenamentoTransações ACID, versionamento e schema enforcementApache AirflowOrquestração do pipelineInterface visual, retry automático e histórico de execuçõesPostgreSQLBanco de metadados do AirflowArmazena histórico e status das execuçõesJupyter NotebookAnálise exploratória e entregaCódigo, resultado e gráficos no mesmo documentopytestTestes unitáriosValida as funções de limpeza e transformação

Estrutura do projeto
ifood-case/
├── docker-compose.yml              # Infraestrutura completa
├── Dockerfile                      # Airflow customizado com PySpark
├── README.md                       # Este arquivo
├── CLAUDE.md                       # Guia para AI assistants
├── dags/
│   └── pipeline_taxi.py            # DAG do Airflow
├── src/
│   ├── logger.py                   # Módulo central de logging
│   ├── transformation_functions.py # Funções de limpeza (testáveis)
│   ├── 01_ingestion.py             # Camada Bronze
│   ├── 02_transformation.py        # Camada Prata
│   ├── 03_gold.py                  # Camada Ouro
│   ├── 04_ai_insights.py           # Skills de IA com Claude
│   └── pipeline.py                 # Orquestrador sequencial alternativo
├── analysis/
│   └── 04_analysis.ipynb           # Notebook com respostas e visualizações
├── tests/
│   ├── __init__.py
│   └── test_transformation.py      # 16 testes unitários com pytest
└── landing_temp/                   # Parquets originais (não versionados)

Camadas do Data Lake
Bronze — s3a://ifood-data-lake/bronze/
Dados brutos exatamente como vieram da fonte. Nunca modificar diretamente.

bronze/yellow_taxi/ — corridas yellow com coluna taxi_type = "yellow"
bronze/green_taxi/  — corridas green com coluna taxi_type = "green"

Prata — s3a://ifood-data-lake/silver/all_taxi
Dados limpos, filtrados e unificados. Particionado por taxi_type.
Filtros aplicados:

Período: apenas corridas de janeiro a maio de 2023
passenger_count > 0 e não nulo
total_amount >= 0 e < 10.000 e não nulo
tpep_dropoff_datetime > tpep_pickup_datetime
Duração máxima pelo critério IQR calculado dinamicamente:

P25 = 0.12h, P75 = 0.33h → corte = P75 + 1.5 × IQR = 0.645h (~38 min)



Ouro — s3a://ifood-data-lake/gold/
Agregações pré-computadas prontas para consumo:
TabelaConteúdoavg_total_by_monthMédia do total_amount por mês — yellow taxiavg_passengers_by_hourMédia de passageiros por hora em maio — todos os táxistrips_by_taxi_typeVolume e ticket médio por tipo e mêstop_hours_by_taxi_typeHorários de pico por tipo de táxi

Como executar
Pré-requisitos

Docker Desktop instalado e em execução
8 GB de RAM disponível

Passo a passo
bash# 1. Suba toda a infraestrutura
docker compose up -d

# 2. Verifique se todos os containers estão rodando
docker compose ps

# 3. Acesse o MinIO e confirme que o bucket foi criado
# http://localhost:9001 — admin / admin123

# 4. Acesse o Airflow e dispare o pipeline
# http://localhost:8080 — admin / admin
# Ative o DAG pipeline_ifood e clique em Trigger DAG

# 5. Ou rode manualmente pelo terminal do Jupyter
docker logs ifood_jupyter 2>&1 | grep token   # Mac/Linux
docker logs ifood_jupyter 2>&1 | findstr token # Windows
# Abra a URL com token em http://localhost:8888

python work/src/pipeline.py                    # pipeline completo
python work/src/pipeline.py --skip-ingestion   # pula o download
Rodando os testes unitários
bash# No terminal do Jupyter
cd /home/jovyan/work
pytest tests/test_transformation.py -v

Serviços e portas
ServiçoURLCredenciaisMinIO Consolehttp://localhost:9001admin / admin123Airflowhttp://localhost:8080admin / adminJupyter Labhttp://localhost:8888ver token nos logsSpark UIhttp://localhost:4040—

Decisões técnicas
Por que Delta Lake e não Parquet puro?
Delta Lake adiciona um transaction log (_delta_log/) que garante que reprocessamentos com mode("overwrite") sejam atômicos. Nunca deixa a tabela em estado inconsistente e permite time travel para auditar versões anteriores.
Por que IQR para remover outliers de duração?
A distribuição de duração tem p99 = 1.08h e máximo de 167h. A média seria distorcida pelos outliers. O IQR usa os percentis centrais (P25 e P75), sendo robusto a valores extremos. O corte é calculado dinamicamente a cada execução — sem valores hardcoded.
Por que Airflow e não um script sequencial?
O Airflow garante que cada etapa só executa se a anterior foi bem-sucedida, com retry automático, histórico de execuções e interface visual. Em produção isso é essencial para monitorar e debugar pipelines.
Por que extrair as funções para transformation_functions.py?
Para permitir testes unitários independentes. Funções embutidas no script principal não podem ser testadas sem executar o pipeline inteiro. A separação segue o princípio de responsabilidade única.
Por que Dockerfile customizado para o Airflow?
O container do Airflow não tem PySpark por padrão. Em vez de depender do container do Jupyter para executar os scripts, o Dockerfile instala Java e PySpark diretamente no Airflow — arquitetura mais robusta e independente.

Testes unitários
O projeto tem 16 testes cobrindo os casos críticos da limpeza de dados:

Registros válidos são mantidos
passenger_count zero, negativo ou nulo é removido
total_amount negativo, acima de 10.000 ou nulo é removido
Datas fora do período Jan-Mai 2023 são removidas
Corridas onde dropoff é antes do pickup são removidas
Corridas com duração acima do corte IQR são removidas
A coluna auxiliar duracao_horas não aparece no resultado final
O corte IQR é sempre positivo e maior que P75


Dados utilizados
NYC Taxi & Limousine Commission (TLC) — Trip Record Data
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
Período: Janeiro a Maio de 2023 — Yellow Taxi e Green Taxi