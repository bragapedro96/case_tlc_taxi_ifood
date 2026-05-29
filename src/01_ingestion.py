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
import sys
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Adiciona o diretório src ao path para importar o logger
sys.path.append(os.path.dirname(__file__))
from logger import obter_logger, LoggerPipeline
 
logger = obter_logger(__name__)

# ── Detecta o ambiente e define o TEMP_DIR correto ───────────────────────────
if os.path.exists("/home/jovyan"):
    TEMP_DIR = "/home/jovyan/work/landing_temp"
else:
    TEMP_DIR = "/opt/airflow/landing_temp"
 
logger.info(f"Ambiente detectado — TEMP_DIR: {TEMP_DIR}")
 
# ── Configuração da sessão Spark ──────────────────────────────────────────────
def criar_spark() -> SparkSession:
    logger.info("Iniciando sessão Spark...")
    try:
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
        logger.info("Sessão Spark iniciada com sucesso.")
        return spark
    except Exception as e:
        logger.critical(f"Falha ao iniciar sessão Spark: {e}", exc_info=True)
        sys.exit(1)
 
# ── Parâmetros ────────────────────────────────────────────────────────────────
MESES    = ["01", "02", "03", "04", "05"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
 
TAXI_TYPES = {
    "yellow": "s3a://ifood-data-lake/bronze/yellow_taxi",
    "green":  "s3a://ifood-data-lake/bronze/green_taxi",
}
 
os.makedirs(TEMP_DIR, exist_ok=True)
 
# ── Download dos arquivos ───────────────────────────────────────────────────── 
def download_arquivos(taxi_type: str) -> list:
    """
    Baixa os arquivos parquet de um tipo de táxi.
    Retorna lista de arquivos baixados com sucesso.
    Pula arquivos que já existem localmente (idempotência).
    """
    arquivos_ok = []
 
    with LoggerPipeline(logger, f"Download {taxi_type} taxi") as ctx:
        for mes in MESES:
            nome    = f"{taxi_type}_tripdata_2023-{mes}.parquet"
            destino = os.path.join(TEMP_DIR, nome)
 
            if os.path.exists(destino):
                ctx.info(f"[SKIP] {nome} já existe localmente.")
                arquivos_ok.append(destino)
                continue
 
            url = f"{BASE_URL}/{nome}"
            ctx.info(f"[DOWNLOAD] {url}")
 
            try:
                resposta = requests.get(url, stream=True, timeout=120)
                resposta.raise_for_status()
 
                with open(destino, "wb") as f:
                    for chunk in resposta.iter_content(chunk_size=8192):
                        f.write(chunk)
 
                tamanho_mb = os.path.getsize(destino) / (1024 * 1024)
                ctx.info(f"[OK] {nome} salvo — {tamanho_mb:.1f} MB")
                arquivos_ok.append(destino)
 
            except requests.exceptions.Timeout:
                ctx.error(f"[ERRO] Timeout ao baixar {nome} — verifique a conexão")
            except requests.exceptions.HTTPError as e:
                ctx.error(f"[ERRO] HTTP {e.response.status_code} ao baixar {nome}")
            except Exception as e:
                ctx.error(f"[ERRO] Falha inesperada ao baixar {nome}: {e}")
 
    return arquivos_ok
 
# ── Carga na camada Bronze ──────────────────────────────────────────────────── 
def salvar_bronze(spark: SparkSession, taxi_type: str, bronze_path: str) -> int:
    """
    Lê os parquets arquivo por arquivo, adiciona taxi_type
    e salva no Bronze em formato Delta.
    Retorna o total de registros salvos.
    """
    with LoggerPipeline(logger, f"Bronze {taxi_type} taxi") as ctx:
        dfs = []
        for mes in MESES:
            caminho = f"{TEMP_DIR}/{taxi_type}_tripdata_2023-{mes}.parquet"
 
            if not os.path.exists(caminho):
                ctx.warning(f"Arquivo não encontrado, pulando: {caminho}")
                continue
 
            try:
                df = spark.read \
                    .option("mergeSchema", "true") \
                    .parquet(caminho)
                df = df.withColumn("taxi_type", F.lit(taxi_type))
                dfs.append(df)
                ctx.info(f"[LIDO] 2023-{mes} — {len(df.columns)} colunas")
            except Exception as e:
                ctx.error(f"[ERRO] Falha ao ler 2023-{mes}: {e}")
                raise
 
        if not dfs:
            raise RuntimeError(f"Nenhum arquivo encontrado para {taxi_type} taxi.")
 
        df_unido = dfs[0]
        for df in dfs[1:]:
            df_unido = df_unido.unionByName(df, allowMissingColumns=True)
 
        total = df_unido.count()
        ctx.info(f"Total de registros: {total:,}")
 
        try:
            df_unido.write \
                .format("delta") \
                .mode("overwrite") \
                .save(bronze_path)
            ctx.info(f"[OK] Salvo em: {bronze_path}")
        except Exception as e:
            ctx.error(f"[ERRO] Falha ao salvar no MinIO: {e}")
            raise
 
        return total
 
 
# ── Execução ──────────────────────────────────────────────────────────────────
def main():
    with LoggerPipeline(logger, "INGESTÃO — CAMADA BRONZE") as ctx:
        spark = criar_spark()
        spark.sparkContext.setLogLevel("WARN")
 
        totais = {}
        erros  = []
 
        for taxi_type, bronze_path in TAXI_TYPES.items():
            try:
                download_arquivos(taxi_type)
                totais[taxi_type] = salvar_bronze(spark, taxi_type, bronze_path)
            except Exception as e:
                logger.error(f"Falha na ingestão de {taxi_type} taxi: {e}", exc_info=True)
                erros.append(taxi_type)
 
        # Resumo
        logger.info("=" * 45)
        logger.info("RESUMO DA INGESTÃO")
        logger.info("=" * 45)
        for taxi_type, total in totais.items():
            logger.info(f"{taxi_type:>6} taxi: {total:>12,} registros")
        if totais:
            logger.info(f"{'TOTAL':>6}      : {sum(totais.values()):>12,} registros")
        if erros:
            logger.error(f"Táxis com erro: {', '.join(erros)}")
            sys.exit(1)
 
        spark.stop()
 
 
if __name__ == "__main__":
    main()