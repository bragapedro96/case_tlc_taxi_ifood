"""
04_ai_insights.py — Skills de IA para análise dos dados
---------------------------------------------------------
Duas skills que usam a API do Claude para ajudar na análise:
 
  Skill 1 — Geração de insights automáticos
  Lê as tabelas Gold e pede ao Claude para gerar um relatório
  com observações, tendências e comparativos sobre os dados.
 
  Skill 2 — Assistente de consultas em linguagem natural
  A pessoa descreve o que quer saber e o Claude sugere o
  código PySpark correto para responder a pergunta.
 
Pré-requisito:
  Defina a variável de ambiente com sua chave da API Anthropic:
    Linux/Mac: export ANTHROPIC_API_KEY="sua-chave"
    Windows:   $env:ANTHROPIC_API_KEY="sua-chave"
 
  Instale a biblioteca:
    pip install anthropic
 
Execução:
  # Skill 1 — relatório de insights
  python work/src/04_ai_insights.py --skill insights
 
  # Skill 2 — assistente de consultas
  python work/src/04_ai_insights.py --skill query
"""

import os
import sys
import json
import argparse
import anthropic
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
 
# ── Configuração do Spark ─────────────────────────────────────────────────────
def criar_spark():
    return SparkSession.builder \
        .appName("ifood-ai-insights") \
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "io.delta:delta-spark_2.12:3.2.0"
        ) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
 
 
# ── Utilitário: converte DataFrame para texto legível ────────────────────────
def df_para_texto(df, titulo: str) -> str:
    """Converte um DataFrame Spark em string formatada para enviar ao Claude."""
    pandas_df = df.toPandas()
    return f"\n### {titulo}\n{pandas_df.to_string(index=False)}\n"
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  SKILL 1 — Geração automática de insights
# ─────────────────────────────────────────────────────────────────────────────
def skill_insights(spark, cliente):
    """
    Lê todas as tabelas Gold, monta um contexto com os dados
    e pede ao Claude para gerar um relatório de insights.
    """
    print("\n📊 Lendo tabelas Gold...")
 
    avg_month = spark.read.format("delta") \
        .load("s3a://ifood-data-lake/gold/avg_total_by_month") \
        .orderBy("month")
 
    avg_hour = spark.read.format("delta") \
        .load("s3a://ifood-data-lake/gold/avg_passengers_by_hour") \
        .orderBy("hour")
 
    trips_type = spark.read.format("delta") \
        .load("s3a://ifood-data-lake/gold/trips_by_taxi_type") \
        .orderBy("taxi_type", "month")
 
    peak_hours = spark.read.format("delta") \
        .load("s3a://ifood-data-lake/gold/top_hours_by_taxi_type") \
        .orderBy("taxi_type", F.desc("total_corridas"))
 
    # Monta o contexto com todos os dados
    contexto = "".join([
        df_para_texto(avg_month,   "Média do valor total por mês (yellow taxi)"),
        df_para_texto(avg_hour,    "Média de passageiros por hora em maio (todos os táxis)"),
        df_para_texto(trips_type,  "Volume e ticket médio por tipo de táxi e mês"),
        df_para_texto(peak_hours,  "Horários de pico por tipo de táxi"),
    ])
 
    prompt = f"""Você é um analista de dados especialista em mobilidade urbana.
 
Abaixo estão os dados agregados de corridas de táxi de Nova York (Janeiro a Maio de 2023),
cobrindo táxis amarelos (yellow) e verdes (green).
 
{contexto}
 
Com base nesses dados, gere um relatório de insights em português com:
1. Principais tendências observadas ao longo dos meses
2. Diferenças relevantes entre yellow e green taxi
3. Padrões de horário de pico e o que eles podem indicar
4. Pelo menos uma observação surpreendente ou não óbvia
5. Uma recomendação de negócio baseada nos dados
 
Seja objetivo e use os números dos dados para embasar cada insight."""
 
    print("🤖 Gerando insights com Claude...\n")
    print("=" * 55)
 
    # Streaming para mostrar a resposta em tempo real
    with cliente.messages.stream(
        model="claude-sonnet-4-20250514",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}]
    ) as stream:
        for texto in stream.text_stream:
            print(texto, end="", flush=True)
 
    print("\n" + "=" * 55)
    print("\n✅ Insights gerados com sucesso!")
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  SKILL 2 — Assistente de consultas em linguagem natural
# ─────────────────────────────────────────────────────────────────────────────
def skill_query(spark, cliente):
    """
    Recebe uma pergunta em linguagem natural e sugere o código
    PySpark correto para respondê-la com base no schema do projeto.
    """
 
    # Schema das tabelas disponíveis — contexto para o Claude
    schema_contexto = """
Você é um assistente especialista em PySpark e Delta Lake.
 
O projeto tem as seguintes tabelas Delta Lake disponíveis no MinIO:
 
**Camada Prata** — `s3a://ifood-data-lake/silver/all_taxi`
Colunas: VendorID (long), passenger_count (double), total_amount (double),
         tpep_pickup_datetime (timestamp), tpep_dropoff_datetime (timestamp),
         taxi_type (string: "yellow" ou "green")
Particionada por: taxi_type
Período: Janeiro a Maio de 2023
 
**Camada Ouro** — tabelas pré-agregadas (preferir para consultas simples):
- `s3a://ifood-data-lake/gold/avg_total_by_month`
  Colunas: month (int), avg_total_amount (double), total_corridas (long)
  Filtro: apenas yellow taxi
 
- `s3a://ifood-data-lake/gold/avg_passengers_by_hour`
  Colunas: hour (int), avg_passengers (double), total_corridas (long)
  Filtro: apenas maio, todos os táxis
 
- `s3a://ifood-data-lake/gold/trips_by_taxi_type`
  Colunas: taxi_type (string), month (int), total_corridas (long),
           avg_total_amount (double), avg_passengers (double)
 
- `s3a://ifood-data-lake/gold/top_hours_by_taxi_type`
  Colunas: taxi_type (string), hour (int), total_corridas (long)
 
**Configuração Spark já disponível** — a sessão `spark` já está criada.
Use sempre `.show()` ou `.toPandas()` para exibir o resultado.
Use `from pyspark.sql import functions as F` para funções.
"""
 
    print("\n🤖 Assistente de consultas PySpark")
    print("=" * 55)
    print("Digite sua pergunta sobre os dados em português.")
    print("Digite 'sair' para encerrar.\n")
 
    while True:
        pergunta = input("Sua pergunta: ").strip()
 
        if pergunta.lower() in ["sair", "exit", "quit"]:
            print("Encerrando assistente.")
            break
 
        if not pergunta:
            continue
 
        prompt = f"""{schema_contexto}
 
O usuário quer saber: "{pergunta}"
 
Gere o código PySpark mais adequado para responder essa pergunta.
Prefira usar as tabelas Gold quando possível (são mais rápidas).
Use a tabela Prata apenas quando a pergunta exigir granularidade de corrida individual.
 
Responda com:
1. Qual tabela você vai usar e por quê (1-2 linhas)
2. O código PySpark completo e funcional
3. O que o resultado vai mostrar (1 linha)
 
Seja direto e objetivo."""
 
        print("\n💡 Gerando código...\n")
        print("-" * 40)
 
        with cliente.messages.stream(
            model="claude-sonnet-4-20250514",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        ) as stream:
            for texto in stream.text_stream:
                print(texto, end="", flush=True)
 
        print("\n" + "-" * 40 + "\n")
 
 
# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Skills de IA para análise dos dados")
    parser.add_argument(
        "--skill",
        choices=["insights", "query"],
        required=True,
        help="insights: relatório automático | query: assistente de consultas"
    )
    args = parser.parse_args()
 
    # Valida a API key
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ Variável de ambiente ANTHROPIC_API_KEY não definida.")
        print("   Linux/Mac: export ANTHROPIC_API_KEY='sua-chave'")
        print("   Windows:   $env:ANTHROPIC_API_KEY='sua-chave'")
        sys.exit(1)
 
    cliente = anthropic.Anthropic(api_key=api_key)
 
    print("🚀 Iniciando Spark...")
    spark = criar_spark()
    spark.sparkContext.setLogLevel("WARN")
 
    try:
        if args.skill == "insights":
            skill_insights(spark, cliente)
        elif args.skill == "query":
            skill_query(spark, cliente)
    finally:
        spark.stop()
 
 
if __name__ == "__main__":
    main()