# Case Técnico — Data Engineer iFood
> Pipeline de dados para ingestão, transformação e análise das corridas de táxi de Nova York (Jan–Mai 2023)

---

## Arquitetura

Este projeto implementa a **arquitetura medalhão** (Bronze → Prata → Ouro) sobre um Data Lake local, simulando um ambiente de produção com as mesmas ferramentas usadas em grandes empresas.

```
NYC TLC (fonte)
      │
      ▼
Bronze — dados brutos preservados (MinIO)
      │
      ▼  PySpark + filtros IQR
      ▼
Prata — dados limpos e unificados (MinIO + Delta Lake)
      │
      ▼  PySpark agregações
      ▼
Ouro  — respostas prontas para consumo (MinIO + Delta Lake)
      │
      ▼
Jupyter Notebook — análise e visualizações
```

---

## Tecnologias utilizadas

| Ferramenta | Papel no projeto | Por que foi escolhida |
|---|---|---|
| **Docker Compose** | Orquestra todos os serviços | Ambiente reproduzível sem instalações locais |
| **MinIO** | Data Lake (simula S3) | API 100% compatível com AWS S3 — código funciona em produção sem alteração |
| **PySpark** | Processamento e transformação | Exigido pelo case; permite processar milhões de registros em paralelo |
| **Delta Lake** | Formato de armazenamento | Adiciona transações ACID, versionamento e schema enforcement ao Parquet |
| **Jupyter Notebook** | Análise exploratória e entrega | Código, resultado e gráficos no mesmo documento |

---

## Estrutura do projeto

```
ifood-case/
├── docker-compose.yml        # Infraestrutura: MinIO + Jupyter
├── src/
│   ├── 01_ingestion.py       # Camada Bronze: download + carga no Data Lake
│   ├── 02_transformation.py  # Camada Prata: limpeza + unificação
│   └── 03_gold.py            # Camada Ouro: agregações do case
├── analysis/
│   └── 04_analysis.ipynb     # Notebook com respostas e visualizações
├── landing_temp/             # Arquivos Parquet originais (persistidos localmente)
└── README.md
```

---

## Camadas do Data Lake

### Bronze — `s3a://ifood-data-lake/bronze/`
Dados brutos exatamente como vieram da fonte. Nenhuma modificação de negócio — apenas a adição da coluna `taxi_type` para identificar a origem. Preservado como fonte de verdade imutável.

### Prata — `s3a://ifood-data-lake/silver/all_taxi`
Dados limpos, filtrados e unificados. Filtros aplicados:
- Período: apenas corridas de **janeiro a maio de 2023**
- `passenger_count > 0`
- `total_amount >= 0` e `< 10.000`
- `tpep_dropoff_datetime > tpep_pickup_datetime`
- **Duração máxima pelo critério IQR** — calculado dinamicamente a partir dos dados:
  - P25 = 0.12h, P75 = 0.33h
  - IQR = 0.21h → corte = P75 + 1.5 × IQR = **0.645h (~38 min)**
  - Corridas com duração acima desse limite são descartadas como outliers

### Ouro — `s3a://ifood-data-lake/gold/`
Agregações pré-computadas, prontas para consulta instantânea:

| Tabela | Conteúdo |
|---|---|
| `avg_total_by_month` | Resposta à Pergunta 1 do case |
| `avg_passengers_by_hour` | Resposta à Pergunta 2 do case |
| `trips_by_taxi_type` | Bônus: volume e ticket médio por tipo e mês |
| `top_hours_by_taxi_type` | Bônus: horários de pico por tipo de táxi |

---

## Decisões técnicas

**Por que Delta Lake e não Parquet puro?**
Delta Lake adiciona um transaction log (`_delta_log/`) que registra todas as operações. Isso garante que reprocessamentos com `mode("overwrite")` sejam atômicos — nunca deixam a tabela em estado inconsistente — e permite time travel para auditar versões anteriores dos dados.

**Por que IQR para remover outliers de duração?**
A distribuição de duração das corridas tem p99 = 1.08h e máximo de 167h — uma cauda longa típica. A média seria distorcida pelos outliers e geraria um corte alto demais. O IQR usa os percentis centrais da distribuição (P25 e P75), sendo robusto a valores extremos. O corte é calculado dinamicamente a cada execução, sem valores hardcoded.

**Por que unificar yellow e green na camada Prata e não no Bronze?**
O Bronze preserva cada fonte separadamente — isso é uma boa prática porque os schemas são diferentes (green usa `lpep_*` em vez de `tpep_*`). A padronização e unificação acontece na Prata, separando claramente a responsabilidade de cada camada.

---

## Como executar

### Pré-requisitos
- Docker Desktop instalado e em execução
- 8 GB de RAM disponível

### Passo a passo

```bash
# 1. Suba a infraestrutura
docker compose up -d

# 2. Acesse o MinIO e crie o bucket ifood-data-lake
# http://localhost:9001 — usuário: admin / senha: admin123

# 3. Abra o Jupyter e pegue o token
docker logs ifood_jupyter 2>&1 | findstr token   # Windows
docker logs ifood_jupyter 2>&1 | grep token      # Mac/Linux

# 4. No terminal do Jupyter, rode o pipeline em sequência
python work/src/01_ingestion.py
python work/src/02_transformation.py
python work/src/03_gold.py

# 5. Abra o notebook de análise
# analysis/04_analysis.ipynb
```

### Tempo estimado
| Etapa | Tempo aproximado |
|---|---|
| Download dos dados | 10–15 min |
| Ingestão Bronze | 5 min |
| Transformação Prata | 10–15 min |
| Agregações Ouro | 10–15 min |

---

## Respostas do case

### Pergunta 1 — Média do `total_amount` por mês (yellow taxi)
Respondida pela tabela `gold/avg_total_by_month` e visualizada no notebook `04_analysis.ipynb`.

### Pergunta 2 — Média de passageiros por hora em maio (todos os táxis)
Respondida pela tabela `gold/avg_passengers_by_hour` e visualizada no notebook `04_analysis.ipynb`.

---

## Dados utilizados
NYC Taxi & Limousine Commission (TLC) — Trip Record Data  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page  
Período: Janeiro a Maio de 2023 — Yellow Taxi e Green Taxi
