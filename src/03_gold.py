"""
03_gold.py — Camada Ouro
--------------------------
Lê a camada Prata e gera agregações prontas para consumo.
 
Perguntas obrigatórias do case:
  P1: Média do total_amount por mês — APENAS yellow taxi
  P2: Média de passageiros por hora em maio — TODOS os táxis da frota
 
Análises bônus:
  B1: Volume de corridas por taxi_type e mês
  B2: Horários de pico por taxi_type
"""
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
 
spark = SparkSession.builder \
    .appName("ifood-gold") \
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
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
 
SILVER_PATH = "s3a://ifood-data-lake/silver/all_taxi"
GOLD_BASE   = "s3a://ifood-data-lake/gold"
 
print("\n" + "="*55)
print(" AGREGAÇÕES — CAMADA OURO")
print("="*55)
 
 
def salvar_gold(df, path, descricao):
    print(f"\n{descricao}")
    df.show(truncate=False)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(path)
    print(f"  [OK] Salvo em: {path}")
 
 
# ── Pergunta 1 ────────────────────────────────────────────────────────────────
# "Qual a média de valor total (total_amount) recebido em um mês
#  considerando todos os yellow táxis da frota?"
# Filtro: taxi_type == yellow
print("\n[1/4] Pergunta 1: média do total_amount por mês — yellow taxi...")
df1 = spark.read.format("delta").load(SILVER_PATH) \
    .filter(F.col("taxi_type") == "yellow") \
    .withColumn("month", F.month("tpep_pickup_datetime")) \
    .groupBy("month") \
    .agg(
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.count("*").alias("total_corridas")
    ) \
    .orderBy("month")
 
salvar_gold(df1, f"{GOLD_BASE}/avg_total_by_month",
            "[1/4] Média do valor total por mês (yellow taxi):")
 
 
# ── Pergunta 2 ────────────────────────────────────────────────────────────────
# "Qual a média de passageiros (passenger_count) por cada hora do dia
#  que pegaram táxi no mês de maio considerando todos os táxis da frota?"
# Filtro: maio, sem filtro de taxi_type
print("\n[2/4] Pergunta 2: média de passageiros por hora em maio — todos os táxis...")
df2 = spark.read.format("delta").load(SILVER_PATH) \
    .withColumn("month", F.month("tpep_pickup_datetime")) \
    .withColumn("hour",  F.hour("tpep_pickup_datetime")) \
    .filter(F.col("month") == 5) \
    .groupBy("hour") \
    .agg(
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
        F.count("*").alias("total_corridas")
    ) \
    .orderBy("hour")
 
salvar_gold(df2, f"{GOLD_BASE}/avg_passengers_by_hour",
            "[2/4] Média de passageiros por hora — maio (todos os táxis):")
 
 
# ── Bônus 1: Volume por taxi_type e mês ──────────────────────────────────────
print("\n[3/4] Bônus: volume por taxi_type e mês...")
df3 = spark.read.format("delta").load(SILVER_PATH) \
    .withColumn("month", F.month("tpep_pickup_datetime")) \
    .groupBy("taxi_type", "month") \
    .agg(
        F.count("*").alias("total_corridas"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers")
    ) \
    .orderBy("taxi_type", "month")
 
salvar_gold(df3, f"{GOLD_BASE}/trips_by_taxi_type",
            "[3/4] Volume por taxi_type e mês:")
 
 
# ── Bônus 2: Horários de pico por taxi_type ───────────────────────────────────
print("\n[4/4] Bônus: horários de pico por taxi_type...")
df4 = spark.read.format("delta").load(SILVER_PATH) \
    .withColumn("hour", F.hour("tpep_pickup_datetime")) \
    .groupBy("taxi_type", "hour") \
    .agg(
        F.count("*").alias("total_corridas")
    ) \
    .orderBy("taxi_type", F.desc("total_corridas"))
 
salvar_gold(df4, f"{GOLD_BASE}/top_hours_by_taxi_type",
            "[4/4] Horários de pico por taxi_type:")
 
 
print("\n" + "="*55)
print(" RESUMO — TABELAS OURO GERADAS")
print("="*55)
print("  avg_total_by_month     → P1: média total_amount, yellow, por mês")
print("  avg_passengers_by_hour → P2: média passageiros, todos, maio por hora")
print("  trips_by_taxi_type     → Bônus: volume por tipo e mês")
print("  top_hours_by_taxi_type → Bônus: horários de pico por tipo")
print("\nCamada Ouro concluída!")
 
spark.stop()
