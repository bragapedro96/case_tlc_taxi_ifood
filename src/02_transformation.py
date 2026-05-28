"""
02_transformation.py — Camada Prata
--------------------------------------
Lê as tabelas Bronze de yellow e green taxi, aplica limpeza,
seleciona as colunas obrigatórias do case, unifica as duas fontes
numa única tabela e salva na camada Prata em formato Delta.
 
Filtros de qualidade aplicados:
- Apenas corridas de janeiro a maio de 2023 (escopo do case)
- passenger_count > 0 e não nulo
- total_amount >= 0 e < 10.000 e não nulo
- datas de pickup e dropoff não nulas
- dropoff sempre após o pickup
- duração da corrida dentro do limite IQR calculado dinamicamente
"""
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
 
spark = SparkSession.builder \
    .appName("ifood-silver-transformation") \
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
 
BRONZE_YELLOW = "s3a://ifood-data-lake/bronze/yellow_taxi"
BRONZE_GREEN  = "s3a://ifood-data-lake/bronze/green_taxi"
SILVER_PATH   = "s3a://ifood-data-lake/silver/all_taxi"
 
COLUNAS_FINAIS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "taxi_type",
]
 
DATA_INICIO = "2023-01-01"
DATA_FIM    = "2023-05-31"
 
 
def ler_e_padronizar(path: str, taxi_type: str):
    df = spark.read.format("delta").load(path)
    if taxi_type == "green":
        df = df \
            .withColumnRenamed("lpep_pickup_datetime",  "tpep_pickup_datetime") \
            .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
    return df
 
 
def calcular_corte_iqr(df):
    """
    Calcula o limite superior de duração usando o critério IQR.
    Fórmula: corte = P75 + 1.5 * (P75 - P25)
    Aplicado sobre corridas com datas válidas e dentro do período do case.
    """
    percentis = df \
        .filter(
            (F.col("tpep_pickup_datetime") >= DATA_INICIO) &
            (F.col("tpep_pickup_datetime") <= DATA_FIM) &
            (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        ) \
        .withColumn(
            "duracao_horas",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
        ) \
        .agg(
            F.percentile_approx("duracao_horas", 0.25).alias("p25"),
            F.percentile_approx("duracao_horas", 0.75).alias("p75"),
        ) \
        .collect()[0]
 
    p25  = percentis["p25"]
    p75  = percentis["p75"]
    iqr  = p75 - p25
    corte = p75 + 1.5 * iqr
 

    # Utilizar apenas para debug e entendimento
    # print(f"  P25          : {p25:.4f}h")
    # print(f"  P75          : {p75:.4f}h")
    # print(f"  IQR          : {iqr:.4f}h")
    # print(f"  Corte (IQR)  : {corte:.4f}h ({corte * 60:.1f} min)")
 
    return corte
 
 
def limpar(df, corte):
    """
    Aplica todos os filtros de qualidade.
    O parâmetro corte é o limite superior de duração em horas,
    calculado dinamicamente pelo critério IQR.
    """
    return df \
        .withColumn(
            "duracao_horas",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
        ) \
        .filter(
            # Retira dados que estejam fora do intervalo definido de datas
            (F.col("tpep_pickup_datetime") >= DATA_INICIO) &
            (F.col("tpep_pickup_datetime") <= DATA_FIM) &
            # Retira registros em que o total de passageiros é 0 ou nulo
            F.col("passenger_count").isNotNull() &
            (F.col("passenger_count") > 0) &
            #Retira registros em que o valor total da viagem é 0 ou Nulo
            F.col("total_amount").isNotNull() &
            (F.col("total_amount") >= 0) &
            (F.col("total_amount") < 10000) &
            #Retira os registros em que as datas de início e fim da corrida  estão Nulos e que tenham início posterior ao fim
            F.col("tpep_pickup_datetime").isNotNull() &
            F.col("tpep_dropoff_datetime").isNotNull() &
            (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
            # Retira outliers de tempo de corrida definidos pelo racional IQR
            (F.col("duracao_horas") <= corte)
        ) \
        .drop("duracao_horas")
 
 
print("\n" + "="*55)
print(" TRANSFORMAÇÃO — CAMADA PRATA")
print("="*55)
 
print("\n[1/5] Lendo camada Bronze...")
df_yellow = ler_e_padronizar(BRONZE_YELLOW, "yellow")
df_green  = ler_e_padronizar(BRONZE_GREEN,  "green")
 
print(f"  yellow — {df_yellow.count():,} registros brutos")
print(f"  green  — {df_green.count():,} registros brutos")
 
print("\n[2/5] Selecionando colunas obrigatórias...")
df_yellow = df_yellow.select(COLUNAS_FINAIS)
df_green  = df_green.select(COLUNAS_FINAIS)
 
print("\n[3/5] Unificando...")
df_unido = df_yellow.unionByName(df_green)
 
print("\n[4/5] Calculando corte IQR de duração...")
corte = calcular_corte_iqr(df_unido)
 
print("\n[5/5] Aplicando limpeza e salvando na camada Prata...")
df_silver = limpar(df_unido, corte)
 
total_bruto = df_unido.count()
total_limpo = df_silver.count()
removidos   = total_bruto - total_limpo
 
print(f"\n  Total bruto  : {total_bruto:,}")
print(f"  Total limpo  : {total_limpo:,}")
print(f"  Removidos    : {removidos:,} ({removidos/total_bruto*100:.1f}%)")
 
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("taxi_type") \
    .save(SILVER_PATH)
 
print(f"\n[OK] Camada Prata salva em: {SILVER_PATH}")
 
print("\n Distribuição por taxi_type:")
df_silver.groupBy("taxi_type").count().orderBy("taxi_type").show()
 
print("\n Meses presentes na Prata:")
df_silver \
    .withColumn("month", F.month("tpep_pickup_datetime")) \
    .groupBy("month").count().orderBy("month") \
    .show()
 
print("Transformação concluída!")
spark.stop()