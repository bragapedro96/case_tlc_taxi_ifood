# Case Técnico — Data Engineer iFood

> Pipeline de dados para ingestão, transformação e análise das corridas de táxi de Nova York (Jan–Mai 2023)

---

## Arquitetura

Este projeto implementa a **arquitetura medalhão** (Bronze → Prata → Ouro) sobre um Data Lake local, orquestrado pelo Apache Airflow e executado com PySpark e Delta Lake.

```
NYC TLC (fonte)
    |
    v
Bronze  ->  dados brutos preservados (MinIO + Delta Lake)
    |
    v  PySpark + limpeza IQR dinamico
Prata   ->  dados limpos e unificados (MinIO + Delta Lake)
    |
    v  PySpark agregacoes
Ouro    ->  respostas prontas para consumo (MinIO + Delta Lake)
    |
    |-->  Apache Airflow  (orquestracao visual)
    |-->  Jupyter Notebook (analise e visualizacoes)
```

---

## Tecnologias utilizadas

| Ferramenta | Papel no projeto | Por que foi escolhida |
|---|---|---|
| **Docker Compose** | Orquestra todos os servicos | Ambiente reproduzivel sem instalacoes locais |
| **MinIO** | Data Lake local (simula S3) | API 100% compativel com AWS S3 |
| **PySpark** | Processamento e transformacao | Processa milhoes de registros em paralelo |
| **Delta Lake** | Formato de armazenamento | Transacoes ACID, versionamento e schema enforcement |
| **Apache Airflow** | Orquestracao do pipeline | Interface visual, retry automatico e historico de execucoes |
| **PostgreSQL** | Banco de metadados do Airflow | Armazena historico e status das execucoes |
| **Jupyter Notebook** | Analise exploratoria e entrega | Codigo, resultado e graficos no mesmo documento |
| **pytest** | Testes unitarios | Valida as funcoes de limpeza e transformacao |

---

## Estrutura do projeto

```
ifood-case/
├── docker-compose.yml
├── Dockerfile
├── README.md
├── CLAUDE.md
├── .env.example
├── dags/
│   └── pipeline_taxi.py
├── src/
│   ├── logger.py
│   ├── transformation_functions.py
│   ├── 01_ingestion.py
│   ├── 02_transformation.py
│   ├── 03_gold.py
│   ├── 04_ai_insights.py
│   └── pipeline.py
├── analysis/
│   └── 04_analysis.ipynb
├── tests/
│   ├── __init__.py
│   └── test_transformation.py
└── landing_temp/
```

---

## Camadas do Data Lake

### Bronze

Dados brutos exatamente como vieram da fonte. Nunca modificar diretamente.

- `bronze/yellow_taxi/` — corridas yellow com coluna `taxi_type = "yellow"`
- `bronze/green_taxi/`  — corridas green com coluna `taxi_type = "green"`

### Prata

Dados limpos, filtrados e unificados. Particionado por `taxi_type`.

Filtros aplicados:

- Periodo: apenas corridas de **janeiro a maio de 2023**
- `passenger_count > 0` e nao nulo
- `total_amount >= 0` e `< 10.000` e nao nulo
- `tpep_dropoff_datetime > tpep_pickup_datetime`
- Duracao maxima pelo criterio **IQR calculado dinamicamente** — P75 + 1.5 x IQR = 0.645h (~38 min)

### Ouro

Agregacoes pre-computadas prontas para consumo:

| Tabela | Conteudo |
|---|---|
| `avg_total_by_month` | Media do total_amount por mes — yellow taxi |
| `avg_passengers_by_hour` | Media de passageiros por hora em maio — todos os taxis |
| `trips_by_taxi_type` | Volume e ticket medio por tipo e mes |
| `top_hours_by_taxi_type` | Horarios de pico por tipo de taxi |

---

## Como executar

### Pre-requisitos

- Docker Desktop instalado e em execucao
- 8 GB de RAM disponivel

### Configuracao inicial

```bash
# 1. Clone o repositorio
git clone https://github.com/seu-usuario/ifood-data-architecture-case.git
cd ifood-data-architecture-case

# 2. Crie o arquivo de credenciais
cp .env.example .env
# Edite o .env com sua ANTHROPIC_API_KEY (opcional — apenas para as skills de IA)

# 3. Suba toda a infraestrutura
docker compose up -d

# 4. Verifique se todos os containers estao rodando
docker compose ps
```

### Via Airflow (recomendado)

1. Acesse **http://localhost:8080** — usuario `admin`, senha `admin`
2. Ative o DAG `pipeline_ifood`
3. Clique em **Trigger DAG** para disparar a execucao
4. Acompanhe o progresso em **Graph View**

### Via terminal do Jupyter

```bash
# Obtenha o token de acesso
docker logs ifood_jupyter 2>&1 | grep token      # Mac/Linux
docker logs ifood_jupyter 2>&1 | findstr token   # Windows

# No terminal do Jupyter (http://localhost:8888)
python work/src/pipeline.py                   # pipeline completo
python work/src/pipeline.py --skip-ingestion  # pula o download
```

### Rodando os testes unitarios

```bash
# No terminal do Jupyter
cd /home/jovyan/work
pytest tests/test_transformation.py -v
```

### Skills de IA (opcional)

```bash
# Requer ANTHROPIC_API_KEY no arquivo .env
python work/src/04_ai_insights.py --skill insights  # relatorio automatico
python work/src/04_ai_insights.py --skill query     # assistente de consultas
```

---

## Servicos e portas

| Servico | URL | Credenciais |
|---|---|---|
| MinIO Console | http://localhost:9001 | admin / admin123 |
| Airflow | http://localhost:8080 | admin / admin |
| Jupyter Lab | http://localhost:8888 | ver token nos logs |
| Spark UI | http://localhost:4040 | — |

---

## Decisoes tecnicas

**Por que Delta Lake e nao Parquet puro?**
Delta Lake adiciona um transaction log (`_delta_log/`) que garante que reprocessamentos com `mode("overwrite")` sejam atomicos — nunca deixa a tabela em estado inconsistente. Permite time travel para auditar versoes anteriores.

**Por que IQR para remover outliers de duracao?**
A distribuicao de duracao tem p99 = 1.08h e maximo de 167h. A media seria distorcida pelos outliers. O IQR usa os percentis centrais (P25 e P75), sendo robusto a valores extremos. O corte e calculado dinamicamente a cada execucao — sem valores hardcoded.

**Por que Airflow?**
O Airflow garante que cada etapa so executa se a anterior foi bem-sucedida, com retry automatico, historico de execucoes e interface visual. Em producao isso e essencial para monitorar e debugar pipelines.

**Por que extrair as funcoes para `transformation_functions.py`?**
Para permitir testes unitarios independentes. Funcoes embutidas no script principal nao podem ser testadas sem executar o pipeline inteiro.

**Por que Dockerfile customizado para o Airflow?**
O container do Airflow nao tem PySpark por padrao. O Dockerfile instala Java e PySpark diretamente no Airflow — arquitetura mais robusta e independente do container do Jupyter.

---

## Testes unitarios

O projeto tem **16 testes** cobrindo os casos criticos da limpeza de dados:

- Registros validos sao mantidos
- `passenger_count` zero, negativo ou nulo e removido
- `total_amount` negativo, acima de 10.000 ou nulo e removido
- Datas fora do periodo Jan-Mai 2023 sao removidas
- Corridas onde dropoff e antes do pickup sao removidas
- Corridas com duracao acima do corte IQR sao removidas
- A coluna auxiliar `duracao_horas` nao aparece no resultado final
- O corte IQR e sempre positivo

---

## Dados utilizados

NYC Taxi & Limousine Commission (TLC) — Trip Record Data

- Fonte: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Periodo: Janeiro a Maio de 2023
- Tipos: Yellow Taxi e Green Taxi