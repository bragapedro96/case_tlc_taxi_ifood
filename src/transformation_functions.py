"""
transformation_functions.py — Funções de transformação
--------------------------------------------------------
Módulo com as funções de limpeza e transformação extraídas
do 02_transformation.py para permitir testes unitários independentes.
 
Importado por:
  - 02_transformation.py (pipeline principal)
  - tests/test_transformation.py (testes unitários)
"""
 
from pyspark.sql import functions as F
 
DATA_INICIO = "2023-01-01"
DATA_FIM    = "2023-05-31"
 
 
def calcular_corte_iqr(df) -> float:
    """
    Calcula o limite superior de duração usando o critério IQR.
    Fórmula: corte = P75 + 1.5 * (P75 - P25)
    Calculado dinamicamente a partir dos dados — sem hardcode.
    """
    percentis = df \
        .filter(
            (F.col("tpep_pickup_datetime") >= DATA_INICIO) &
            (F.col("tpep_pickup_datetime") <= DATA_FIM) &
            (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        ) \
        .withColumn(
            "duracao_horas",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 3600
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
 
    return corte
 
 
def limpar(df, corte: float):
    """
    Aplica todos os filtros de qualidade ao DataFrame.
 
    Parâmetros:
        df    — DataFrame unificado com colunas obrigatórias
        corte — limite superior de duração em horas (calculado pelo IQR)
 
    Filtros aplicados:
        - Período: apenas Jan-Mai 2023
        - passenger_count > 0 e não nulo
        - total_amount >= 0 e < 10.000 e não nulo
        - datas não nulas e dropoff > pickup
        - duração <= corte IQR
    """
    return df \
        .withColumn(
            "duracao_horas",
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 3600
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