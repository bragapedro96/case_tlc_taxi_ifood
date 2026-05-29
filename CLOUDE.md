# CLAUDE.md — Guia para AI Assistants

Este arquivo fornece contexto para que assistentes de IA (como o Claude) possam ajudar de forma eficaz em tarefas relacionadas a este projeto.

---

## O que é este projeto

Pipeline de dados para ingestão, transformação e análise das corridas de táxi amarelo e verde de Nova York (Janeiro a Maio de 2023), desenvolvido como case técnico para vaga de Data Engineer.

Implementa a **arquitetura medalhão** (Bronze → Prata → Ouro) sobre um Data Lake local usando Docker, MinIO, PySpark, Delta Lake e Apache Airflow.

---

## Estrutura do projeto

```
ifood-case/
├── docker-compose.yml              # Infraestrutura completa
├── Dockerfile                      # Airflow customizado com PySpark e Java
├── README.md                       # Documentação do projeto
├── CLAUDE.md                       # Este arquivo
├── dags/
│   └── pipeline_taxi.py            # DAG do Airflow — orquestração visual
├── src/
│   ├── logger.py                   # Módulo central de logging estruturado
│   ├── transformation_functions.py # Funções de limpeza extraídas para teste
│   ├── 01_ingestion.py             # Camada Bronze: download + carga no MinIO
│   ├── 02_transformation.py        # Camada Prata: limpeza + unificação
│   ├── 03_gold.py                  # Camada Ouro: agregações do case
│   ├── 04_ai_insights.py           # Skills de IA com API do Claude
│   └── pipeline.py                 # Orquestrador sequencial alternativo (sem Airflow)
├── analysis/
│   └── 04_analysis.ipynb           # Notebook com respostas e visualizações
├── tests/
│   ├── __init__.py
│   └── test_transformation.py      # 16 testes unitários com pytest
└── landing_temp/                   # Parquets originais da NYC TLC (não versionados)
```

---

## Como rodar o projeto

### Pré-requisitos
- Docker Desktop instalado e em execução
- 8 GB de RAM disponível

### Subindo o ambiente

```bash
# Sobe toda a infraestrutura
docker compose up -d

# Verifica se todos os containers estão rodando
docker compose ps
```

### Via Airflow (recomendado)
1. Acesse http://localhost:8080 — usuário `admin`, senha `admin`
2. Ative o DAG `pipeline_ifood`
3. Clique em **Trigger DAG** para disparar a execução
4. Acompanhe o progresso em **Graph View**

### Via terminal do Jupyter
```bash
# Pega o token de acesso
docker logs ifood_jupyter 2>&1 | grep token

# No terminal do Jupyter
python work/src/pipeline.py                   # pipeline completo
python work/src/pipeline.py --skip-ingestion  # pula o download
```

### Rodando os testes
```bash
# No terminal do Jupyter
cd /home/jovyan/work
pytest tests/test_transformation.py -v
```

---

## Serviços disponíveis

| Serviço | URL | Credenciais |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / admin123 |
| Airflow | http://localhost:8080 | admin / admin |
| Jupyter Lab | http://localhost:8888 | ver token nos logs |
| Spark UI | http://localhost:4040 | — |

---

## Camadas do Data Lake

### Bronze — `s3a://ifood-data-lake/bronze/`
Dados brutos preservados exatamente como vieram da fonte.
- `bronze/yellow_taxi/` — corridas yellow com coluna `taxi_type = "yellow"`
- `bronze/green_taxi/`  — corridas green com coluna `taxi_type = "green"`

### Prata — `s3a://ifood-data-lake/silver/all_taxi`
Dados limpos, filtrados e unificados. Particionado por `taxi_type`.
Colunas: `VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `taxi_type`

### Ouro — `s3a://ifood-data-lake/gold/`
Tabelas pré-agregadas. Usar sempre que possível — muito mais rápidas que a Prata.
- `avg_total_by_month`     — média do total_amount por mês (yellow taxi)
- `avg_passengers_by_hour` — média de passageiros por hora em maio (todos)
- `trips_by_taxi_type`     — volume e ticket médio por tipo e mês
- `top_hours_by_taxi_type` — horários de pico por tipo de táxi

---

## Convenções de código

- Todos os scripts importam `from logger import obter_logger, LoggerPipeline`
- Nunca usar `print()` — usar sempre `logger.info()`, `logger.warning()`, `logger.error()`
- Funções testáveis ficam em `transformation_functions.py`, não embutidas nos scripts
- Leitura de tabelas Delta: `spark.read.format("delta").load(caminho)`
- Escrita de tabelas Delta: `df.write.format("delta").mode("overwrite").save(caminho)`
- Colunas de tempo são extraídas com `F.month()`, `F.hour()`, `F.unix_timestamp()`
- O script `01_ingestion.py` detecta automaticamente o ambiente (Jupyter vs Airflow)

---

## Contexto técnico importante

**Schema inconsistente nos Parquets da NYC TLC:**
Os arquivos de 2023 têm `VendorID` como `INT32` em alguns meses e `BIGINT` em outros. Tratado com `mergeSchema=true` e `enableVectorizedReader=false`.

**Green taxi usa nomes de coluna diferentes:**
`lpep_pickup_datetime` e `lpep_dropoff_datetime` em vez de `tpep_*`. A padronização acontece em `ler_e_padronizar()` no `02_transformation.py`.

**Corte IQR calculado dinamicamente:**
P25=0.12h, P75=0.33h → corte = 0.645h (~38 min). Recalculado a cada execução — sem hardcode.

**Detecção automática de ambiente no `01_ingestion.py`:**
```python
if os.path.exists("/home/jovyan"):
    TEMP_DIR = "/home/jovyan/work/landing_temp"  # Jupyter
else:
    TEMP_DIR = "/opt/airflow/landing_temp"        # Airflow
```

**Airflow usa Dockerfile customizado:**
A imagem base do Airflow não tem PySpark. O `Dockerfile` instala Java 17 e PySpark 3.5.1.

---

## Perguntas frequentes para o AI

**Como adicionar um novo mês de dados?**
Atualize `MESES` em `01_ingestion.py` e `DATA_FIM` em `transformation_functions.py`. Rode o pipeline completo.

**Como adicionar uma nova fonte de táxi?**
Adicione entrada em `TAXI_TYPES` no `01_ingestion.py`, trate renomeações em `ler_e_padronizar()` no `02_transformation.py` e adicione tabela Gold em `03_gold.py`.

**Como rodar apenas a camada Ouro?**
```bash
python work/src/03_gold.py
```

**Como consultar os dados sem o notebook?**
```python
spark.read.format("delta").load("s3a://ifood-data-lake/gold/avg_total_by_month").show()
```

**O que fazer se o Spark ficar sem memória?**
Reduza `spark.sql.shuffle.partitions` para 4 nos scripts ou aumente a memória no `docker-compose.yml`.

**Como rodar um teste específico?**
```bash
pytest tests/test_transformation.py::TestLimpar::test_registro_valido_e_mantido -v
```

**Como ver os logs de uma execução do Airflow?**
```bash
docker exec ifood_airflow_scheduler find /opt/airflow/logs -name "*.log" -type f
docker exec ifood_airflow_scheduler cat "/opt/airflow/logs/dag_id=pipeline_ifood/..."
```