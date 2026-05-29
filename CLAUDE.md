# CLAUDE.md — Guia para AI Assistants

Este arquivo fornece contexto para que assistentes de IA (como o Claude) possam ajudar de forma eficaz em tarefas relacionadas a este projeto.

---

## O que e este projeto

Pipeline de dados para ingestion, transformacao e analise das corridas de taxi amarelo e verde de Nova York (Janeiro a Maio de 2023), desenvolvido como case tecnico para vaga de Data Engineer.

Implementa a **arquitetura medalhao** (Bronze -> Prata -> Ouro) sobre um Data Lake local usando Docker, MinIO, PySpark, Delta Lake e Apache Airflow.

---

## Fluxo do pipeline

```
Download (NYC TLC)
    |
    v
Bronze  ->  dados brutos no MinIO
    |
    v
Prata   ->  dados limpos e unificados
    |
    v
Ouro    ->  agregacoes prontas para consumo
```

---

## Estrutura do projeto

```
ifood-case/
├── docker-compose.yml              # Infraestrutura completa
├── Dockerfile                      # Airflow customizado com PySpark e Java
├── README.md
├── CLAUDE.md                       # Este arquivo
├── .env.example                    # Template de credenciais
├── dags/
│   └── pipeline_taxi.py            # DAG do Airflow
├── src/
│   ├── logger.py                   # Modulo central de logging estruturado
│   ├── transformation_functions.py # Funcoes de limpeza (importadas nos testes)
│   ├── 01_ingestion.py             # Camada Bronze
│   ├── 02_transformation.py        # Camada Prata
│   ├── 03_gold.py                  # Camada Ouro
│   ├── 04_ai_insights.py           # Skills de IA com API do Claude
│   └── pipeline.py                 # Orquestrador sequencial alternativo
├── analysis/
│   └── 04_analysis.ipynb           # Notebook com respostas e visualizacoes
├── tests/
│   ├── __init__.py
│   └── test_transformation.py      # 16 testes unitarios com pytest
└── landing_temp/                   # Parquets originais (nao versionados)
```

---

## Como rodar o projeto

### Pre-requisitos

- Docker Desktop instalado e em execucao
- 8 GB de RAM disponivel

### Subindo o ambiente

```bash
# Configuracao inicial
cp .env.example .env
# Edite o .env com sua ANTHROPIC_API_KEY (opcional)

# Sobe a infraestrutura
docker compose up -d

# Verifica os containers
docker compose ps
```

### Via Airflow (recomendado)

```
1. Acesse http://localhost:8080 — admin / admin
2. Ative o DAG pipeline_ifood
3. Clique em Trigger DAG
4. Acompanhe em Graph View
```

### Via terminal do Jupyter

```bash
# Obtem o token
docker logs ifood_jupyter 2>&1 | grep token

# No terminal do Jupyter
python work/src/pipeline.py
python work/src/pipeline.py --skip-ingestion
```

### Rodando os testes

```bash
cd /home/jovyan/work
pytest tests/test_transformation.py -v
```

---

## Servicos disponiveis

| Servico | URL | Credenciais |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / admin123 |
| Airflow | http://localhost:8080 | admin / admin |
| Jupyter Lab | http://localhost:8888 | ver token nos logs |
| Spark UI | http://localhost:4040 | — |

---

## Camadas do Data Lake

### Bronze

```
s3a://ifood-data-lake/bronze/yellow_taxi/
s3a://ifood-data-lake/bronze/green_taxi/
```

Dados brutos preservados exatamente como vieram da fonte. Nunca modificar diretamente.

### Prata

```
s3a://ifood-data-lake/silver/all_taxi
```

Dados limpos, filtrados e unificados. Particionado por `taxi_type`.

Colunas: `VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `taxi_type`

### Ouro

```
s3a://ifood-data-lake/gold/avg_total_by_month
s3a://ifood-data-lake/gold/avg_passengers_by_hour
s3a://ifood-data-lake/gold/trips_by_taxi_type
s3a://ifood-data-lake/gold/top_hours_by_taxi_type
```

Tabelas pre-agregadas. Usar sempre que possivel — muito mais rapidas que a Prata.

---

## Convencoes de codigo

- Todos os scripts importam `from logger import obter_logger, LoggerPipeline`
- Nunca usar `print()` — usar sempre `logger.info()`, `logger.warning()`, `logger.error()`
- Funcoes testaveis ficam em `transformation_functions.py`
- Leitura Delta: `spark.read.format("delta").load(caminho)`
- Escrita Delta: `df.write.format("delta").mode("overwrite").save(caminho)`
- O script `01_ingestion.py` detecta automaticamente o ambiente (Jupyter vs Airflow)

---

## Contexto tecnico importante

**Schema inconsistente nos Parquets da NYC TLC:**
Os arquivos de 2023 tem `VendorID` como `INT32` em alguns meses e `BIGINT` em outros. Tratado com `mergeSchema=true` e `enableVectorizedReader=false`.

**Green taxi usa nomes de coluna diferentes:**
`lpep_pickup_datetime` e `lpep_dropoff_datetime` em vez de `tpep_*`. Padronizados em `ler_e_padronizar()` no `02_transformation.py`.

**Corte IQR calculado dinamicamente:**
P25=0.12h, P75=0.33h -> corte = 0.645h (~38 min). Recalculado a cada execucao.

**Deteccao automatica de ambiente:**

```python
if os.path.exists("/home/jovyan"):
    TEMP_DIR = "/home/jovyan/work/landing_temp"  # Jupyter
else:
    TEMP_DIR = "/opt/airflow/landing_temp"        # Airflow
```

**Airflow usa Dockerfile customizado:**
A imagem base do Airflow nao tem PySpark. O Dockerfile instala Java 17 e PySpark 3.5.1.

---

## Perguntas frequentes para o AI

**Como adicionar um novo mes de dados?**

```bash
# Em 01_ingestion.py: adicione o mes em MESES
MESES = ["01", "02", "03", "04", "05", "06"]

# Em transformation_functions.py: atualize DATA_FIM
DATA_FIM = "2023-06-30"
```

**Como adicionar uma nova fonte de taxi?**

```python
# Em 01_ingestion.py: adicione em TAXI_TYPES
TAXI_TYPES = {
    "yellow": "s3a://ifood-data-lake/bronze/yellow_taxi",
    "green":  "s3a://ifood-data-lake/bronze/green_taxi",
    "fhv":    "s3a://ifood-data-lake/bronze/fhv_taxi",   # nova fonte
}
```

**Como rodar apenas a camada Ouro?**

```bash
python work/src/03_gold.py
```

**Como consultar os dados sem o notebook?**

```python
spark.read.format("delta").load("s3a://ifood-data-lake/gold/avg_total_by_month").show()
```

**Como rodar um teste especifico?**

```bash
pytest tests/test_transformation.py::TestLimpar::test_registro_valido_e_mantido -v
```

**Como ver os logs de uma execucao do Airflow?**

```bash
docker exec ifood_airflow_scheduler find /opt/airflow/logs -name "*.log" -type f
```

**O que fazer se o Spark ficar sem memoria?**

Reduza `spark.sql.shuffle.partitions` para 4 nos scripts ou aumente a memoria no `docker-compose.yml`.