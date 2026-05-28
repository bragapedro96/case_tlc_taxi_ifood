"""
01_ingestion.py — Camada Bronze
--------------------------------
Baixa os arquivos de corridas de táxi amarelo (yellow) e verde (green)
da NYC TLC e salva na camada Bronze do Data Lake no formato Delta.
 
Os arquivos de 2023 têm schemas ligeiramente diferentes entre meses
(ex: VendorID como INT32 em alguns, BIGINT em outros). Por isso usamos
mergeSchema=true e desativamos o leitor vetorizado para que o Spark
leia cada arquivo de forma flexível sem travar na conversão de tipos.
"""
 
import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
 
# ── Configuração da sessão Spark ──────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("ifood-bronze-ingestion") \
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
    .getOrCreate()
 
spark.sparkContext.setLogLevel("WARN")
 
# ── Parâmetros ────────────────────────────────────────────────────────────────
MESES    = ["01", "02", "03", "04", "05"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TEMP_DIR = "/home/jovyan/work/landing_temp"
 
TAXI_TYPES = {
    "yellow": "s3a://ifood-data-lake/bronze/yellow_taxi",
    "green":  "s3a://ifood-data-lake/bronze/green_taxi",
}
 
os.makedirs(TEMP_DIR, exist_ok=True)
 
 
def download_arquivos(taxi_type: str):
    """Baixa os arquivos parquet de um tipo de táxi para o diretório temporário."""
    print(f"\n=== Download: {taxi_type} taxi ===")
    for mes in MESES:
        nome    = f"{taxi_type}_tripdata_2023-{mes}.parquet"
        destino = os.path.join(TEMP_DIR, nome)
 
        if os.path.exists(destino):
            print(f"  [SKIP] {nome} já existe localmente.")
            continue
 
        url = f"{BASE_URL}/{nome}"
        print(f"  [DOWNLOAD] {url}")
        resposta = requests.get(url, stream=True, timeout=120)
        resposta.raise_for_status()
 
        with open(destino, "wb") as f:
            for chunk in resposta.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"  [OK] {nome} salvo.")
 
 
def salvar_bronze(taxi_type: str, bronze_path: str):
    """
    Lê os parquets arquivo por arquivo para evitar conflito de schema,
    adiciona a coluna taxi_type e salva no Bronze em formato Delta.
 
    Lemos arquivo por arquivo (não com *) porque os arquivos de 2023
    têm schemas inconsistentes entre meses. O unionByName com
    allowMissingColumns garante que colunas extras em alguns meses
    não quebrem a união.
    """
    print(f"\n=== Bronze: {taxi_type} taxi ===")
 
    dfs = []
    for mes in MESES:
        caminho = f"{TEMP_DIR}/{taxi_type}_tripdata_2023-{mes}.parquet"
        df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(caminho)
        df = df.withColumn("taxi_type", F.lit(taxi_type))
        dfs.append(df)
        print(f"  [LIDO] 2023-{mes} — {df.columns}")
 
    # Une todos os meses, tolerando colunas ausentes em alguns arquivos
    df_unido = dfs[0]
    for df in dfs[1:]:
        df_unido = df_unido.unionByName(df, allowMissingColumns=True)
 
    total = df_unido.count()
    print(f"  Total de registros: {total:,}")
 
    df_unido.write \
        .format("delta") \
        .mode("overwrite") \
        .save(bronze_path)
 
    print(f"  [OK] Salvo em: {bronze_path}")
    return total
 
 
# ── Execução ──────────────────────────────────────────────────────────────────
print("\n" + "="*55)
print(" INGESTÃO — CAMADA BRONZE")
print("="*55)
 
totais = {}
for taxi_type, bronze_path in TAXI_TYPES.items():
    download_arquivos(taxi_type)
    totais[taxi_type] = salvar_bronze(taxi_type, bronze_path)
 
print("\n" + "="*55)
print(" RESUMO DA INGESTÃO")
print("="*55)
for taxi_type, total in totais.items():
    print(f"  {taxi_type:>6} taxi: {total:>12,} registros")
print(f"  {'TOTAL':>6}      : {sum(totais.values()):>12,} registros")
print("\nIngestão concluída!")
 
spark.stop()