"""
pipeline_ifood.py — DAG do Airflow
------------------------------------
Define o pipeline de dados do case iFood como um DAG do Airflow.
 
O Airflow lê este arquivo automaticamente da pasta dags/ e
disponibiliza o pipeline na interface visual em http://localhost:8080.
 
Arquitetura de execução:
    O Airflow orquestra a ordem das tasks e executa os scripts
    diretamente — o container do Airflow tem PySpark instalado
    via Dockerfile customizado.
 
Estrutura do pipeline:
    ingestao_bronze >> transformacao_prata >> agregacoes_ouro
"""
 
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
 
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}
 
with DAG(
    dag_id="pipeline_taxi",
    description="Pipeline de dados NYC Taxi — Bronze → Prata → Ouro",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ifood", "nyc-taxi", "medallion"],
) as dag:
 
    # ── Task 1: Ingestão — Camada Bronze ─────────────────────────────────────
    ingestao_bronze = BashOperator(
        task_id="ingestao_bronze",
        bash_command="python /opt/airflow/src/01_ingestion.py",
        execution_timeout=timedelta(hours=2),
    )
 
    # ── Task 2: Transformação — Camada Prata ─────────────────────────────────
    transformacao_prata = BashOperator(
        task_id="transformacao_prata",
        bash_command="python /opt/airflow/src/02_transformation.py",
        execution_timeout=timedelta(hours=1),
    )
 
    # ── Task 3: Agregações — Camada Ouro ─────────────────────────────────────
    agregacoes_ouro = BashOperator(
        task_id="agregacoes_ouro",
        bash_command="python /opt/airflow/src/03_gold.py",
        execution_timeout=timedelta(hours=1),
    )
 
    # ── Ordem de execução ─────────────────────────────────────────────────────
    ingestao_bronze >> transformacao_prata >> agregacoes_ouro