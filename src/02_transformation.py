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
 
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
 
sys.path.append(os.path.dirname(__file__))
from logger import obter_logger, LoggerPipeline
 
logger = obter_logger(__name__)
 
# ── Configuração da sessão Spark ──────────────────────────────────────────────
def criar_spark() -> SparkSession:
    logger.info("Iniciando sessão Spark...")
    try:
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
        logger.info("Sessão Spark iniciada com sucesso.")
        return spark
    except Exception as e:
        logger.critical(f"Falha ao iniciar sessão Spark: {e}", exc_info=True)
        sys.exit(1)
 
 
# ── Parâmetros ────────────────────────────────────────────────────────────────
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
 
 
# ── Funções ───────────────────────────────────────────────────────────────────
def ler_e_padronizar(spark: SparkSession, path: str, taxi_type: str):
    """
    Lê a tabela Bronze e padroniza o schema.
    O green taxi usa nomes de coluna diferentes para pickup/dropoff
    então renomeamos para o padrão do yellow (tpep_*).
    """
    try:
        df = spark.read.format("delta").load(path)
        if taxi_type == "green":
            df = df \
                .withColumnRenamed("lpep_pickup_datetime",  "tpep_pickup_datetime") \
                .withColumnRenamed("lpep_dropoff_datetime", "tpep_dropoff_datetime")
        return df
    except Exception as e:
        logger.error(f"Falha ao ler Bronze {taxi_type}: {e}", exc_info=True)
        raise
 
 
def calcular_corte_iqr(df) -> float:
    """
    Calcula o limite superior de duração usando o critério IQR.
    Fórmula: corte = P75 + 1.5 * (P75 - P25)
    Calculado dinamicamente a partir dos dados — sem hardcode.
    """
    try:
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
 
        p25   = percentis["p25"]
        p75   = percentis["p75"]
        iqr   = p75 - p25
        corte = p75 + 1.5 * iqr
 
        logger.info(f"P25={p25:.4f}h | P75={p75:.4f}h | IQR={iqr:.4f}h | Corte={corte:.4f}h ({corte*60:.1f} min)")
        return corte
 
    except Exception as e:
        logger.error(f"Falha ao calcular corte IQR: {e}", exc_info=True)
        raise
 
 
def limpar(df, corte: float):
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
            (F.col("tpep_pickup_datetime") >= DATA_INICIO) &
            (F.col("tpep_pickup_datetime") <= DATA_FIM) &
            F.col("passenger_count").isNotNull() &
            (F.col("passenger_count") > 0) &
            F.col("total_amount").isNotNull() &
            (F.col("total_amount") >= 0) &
            (F.col("total_amount") < 10000) &
            F.col("tpep_pickup_datetime").isNotNull() &
            F.col("tpep_dropoff_datetime").isNotNull() &
            (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")) &
            (F.col("duracao_horas") <= corte)
        ) \
        .drop("duracao_horas")
 
 
# ── Execução ──────────────────────────────────────────────────────────────────
def main():
    with LoggerPipeline(logger, "TRANSFORMAÇÃO — CAMADA PRATA"):
        spark = criar_spark()
        spark.sparkContext.setLogLevel("WARN")
 
        with LoggerPipeline(logger, "Leitura Bronze"):
            df_yellow = ler_e_padronizar(spark, BRONZE_YELLOW, "yellow")
            df_green  = ler_e_padronizar(spark, BRONZE_GREEN,  "green")
            logger.info(f"yellow — {df_yellow.count():,} registros brutos")
            logger.info(f"green  — {df_green.count():,} registros brutos")
 
        with LoggerPipeline(logger, "Seleção de colunas"):
            df_yellow = df_yellow.select(COLUNAS_FINAIS)
            df_green  = df_green.select(COLUNAS_FINAIS)
 
        with LoggerPipeline(logger, "Unificação"):
            df_unido = df_yellow.unionByName(df_green)
 
        with LoggerPipeline(logger, "Cálculo do corte IQR"):
            corte = calcular_corte_iqr(df_unido)
 
        with LoggerPipeline(logger, "Limpeza e filtragem"):
            df_silver   = limpar(df_unido, corte)
            total_bruto = df_unido.count()
            total_limpo = df_silver.count()
            removidos   = total_bruto - total_limpo
            logger.info(f"Total bruto  : {total_bruto:,}")
            logger.info(f"Total limpo  : {total_limpo:,}")
            logger.info(f"Removidos    : {removidos:,} ({removidos/total_bruto*100:.1f}%)")
 
        with LoggerPipeline(logger, "Escrita na camada Prata"):
            try:
                df_silver.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .partitionBy("taxi_type") \
                    .save(SILVER_PATH)
                logger.info(f"Salvo em: {SILVER_PATH}")
            except Exception as e:
                logger.error(f"Falha ao salvar camada Prata: {e}", exc_info=True)
                raise
 
        logger.info("Distribuição por taxi_type:")
        df_silver.groupBy("taxi_type").count().orderBy("taxi_type").show()
 
        logger.info("Meses presentes na Prata:")
        df_silver \
            .withColumn("month", F.month("tpep_pickup_datetime")) \
            .groupBy("month").count().orderBy("month") \
            .show()
 
        spark.stop()
 
 
if __name__ == "__main__":
    main()