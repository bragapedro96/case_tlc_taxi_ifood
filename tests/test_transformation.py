"""
test_transformation.py — Testes unitários da camada de transformação
----------------------------------------------------------------------
Testa as funções de limpeza e transformação do pipeline.
 
As funções testadas são as mais críticas do pipeline — se tiverem
bugs, dados corrompidos chegam na camada Prata sem que ninguém perceba.
 
Execução:
    pip install pytest
    pytest tests/test_transformation.py -v
 
O -v (verbose) mostra o nome de cada teste e se passou ou falhou.
"""
 
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, TimestampType, StringType
)
 
 
# ── Fixture: sessão Spark compartilhada entre todos os testes ─────────────────
# O @pytest.fixture com scope="session" garante que o Spark é iniciado
# apenas uma vez e reutilizado em todos os testes — evita overhead.
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("ifood-tests") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.ui.enabled", "false") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
 
 
# ── Schema base para os testes ────────────────────────────────────────────────
SCHEMA = StructType([
    StructField("VendorID",              LongType(),      True),
    StructField("passenger_count",       DoubleType(),    True),
    StructField("total_amount",          DoubleType(),    True),
    StructField("tpep_pickup_datetime",  TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("taxi_type",             StringType(),    True),
])
 
 
# ── Função auxiliar: cria DataFrame de teste ──────────────────────────────────
def criar_df(spark, dados: list):
    """Cria um DataFrame com o schema padrão a partir de uma lista de tuplas."""
    return spark.createDataFrame(dados, schema=SCHEMA)
 
 
# ── Importa as funções a serem testadas ───────────────────────────────────────
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "src"))
 
# Importa as funções diretamente do módulo de transformação
from transformation_functions import limpar, calcular_corte_iqr
 
DATA_INICIO = "2023-01-01"
DATA_FIM    = "2023-05-31"
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  TESTES DA FUNÇÃO limpar()
# ─────────────────────────────────────────────────────────────────────────────
 
class TestLimpar:
 
    def test_registro_valido_e_mantido(self, spark):
        """Um registro completamente válido deve passar pela limpeza."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 1
 
    def test_passenger_count_zero_removido(self, spark):
        """Corridas com 0 passageiros devem ser removidas."""
        dados = [(
            1, 0.0, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_passenger_count_negativo_removido(self, spark):
        """Corridas com passageiros negativos devem ser removidas."""
        dados = [(
            1, -1.0, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_passenger_count_nulo_removido(self, spark):
        """Corridas com passenger_count nulo devem ser removidas."""
        dados = [(
            1, None, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_total_amount_negativo_removido(self, spark):
        """Corridas com valor total negativo devem ser removidas."""
        dados = [(
            1, 2.0, -5.0,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_total_amount_acima_limite_removido(self, spark):
        """Corridas com valor acima de 10.000 devem ser removidas."""
        dados = [(
            1, 2.0, 15000.0,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_total_amount_nulo_removido(self, spark):
        """Corridas com total_amount nulo devem ser removidas."""
        dados = [(
            1, 2.0, None,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_data_fora_do_periodo_removida(self, spark):
        """Corridas fora de Jan-Mai 2023 devem ser removidas."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2023, 6, 10, 8, 0, 0),  # junho — fora do escopo
            datetime(2023, 6, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_data_ano_anterior_removida(self, spark):
        """Corridas de 2022 devem ser removidas."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2022, 12, 31, 23, 0, 0),
            datetime(2022, 12, 31, 23, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_dropoff_antes_pickup_removido(self, spark):
        """Corridas onde dropoff é antes do pickup devem ser removidas."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2023, 3, 10, 8, 30, 0),  # pickup às 8:30
            datetime(2023, 3, 10, 8, 0, 0),   # dropoff às 8:00 — impossível
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 0
 
    def test_duracao_acima_do_corte_removida(self, spark):
        """Corridas com duração acima do corte IQR devem ser removidas."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 10, 0, 0),  # 2 horas — acima do corte de 0.645h
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=0.645)
        assert resultado.count() == 0
 
    def test_duracao_dentro_do_corte_mantida(self, spark):
        """Corridas com duração dentro do corte IQR devem ser mantidas."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 30, 0),  # 0.5h — abaixo do corte de 0.645h
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=0.645)
        assert resultado.count() == 1
 
    def test_multiplos_registros_filtragem_correta(self, spark):
        """Apenas registros válidos devem sobreviver quando há mistura."""
        dados = [
            # Válido
            (1, 2.0, 15.50, datetime(2023, 3, 10, 8, 0), datetime(2023, 3, 10, 8, 20), "yellow"),
            # Inválido: passenger_count = 0
            (1, 0.0, 15.50, datetime(2023, 3, 10, 8, 0), datetime(2023, 3, 10, 8, 20), "yellow"),
            # Inválido: fora do período
            (1, 2.0, 15.50, datetime(2023, 7, 10, 8, 0), datetime(2023, 7, 10, 8, 20), "yellow"),
            # Válido
            (2, 1.0, 22.00, datetime(2023, 1, 5, 9, 0), datetime(2023, 1, 5, 9, 25), "green"),
        ]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert resultado.count() == 2
 
    def test_coluna_duracao_nao_presente_na_saida(self, spark):
        """A coluna auxiliar duracao_horas não deve aparecer no resultado final."""
        dados = [(
            1, 2.0, 15.50,
            datetime(2023, 3, 10, 8, 0, 0),
            datetime(2023, 3, 10, 8, 20, 0),
            "yellow"
        )]
        df = criar_df(spark, dados)
        resultado = limpar(df, corte=1.0)
        assert "duracao_horas" not in resultado.columns
 
 
# ─────────────────────────────────────────────────────────────────────────────
#  TESTES DA FUNÇÃO calcular_corte_iqr()
# ─────────────────────────────────────────────────────────────────────────────
 
class TestCalcularCorteIqr:
 
    def test_corte_maior_que_p75(self, spark):
        """O corte IQR deve ser sempre maior que o P75."""
        dados = [
            (1, 2.0, 10.0, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 10), "yellow"),
            (1, 2.0, 10.0, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 20), "yellow"),
            (1, 2.0, 10.0, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 8, 30), "yellow"),
            (1, 2.0, 10.0, datetime(2023, 1, 1, 8, 0), datetime(2023, 1, 1, 9, 0),  "yellow"),
        ]
        df = criar_df(spark, dados)
        corte = calcular_corte_iqr(df)
        assert corte > 0
 
    def test_corte_e_positivo(self, spark):
        """O corte IQR deve ser sempre um valor positivo."""
        dados = [
            (1, 2.0, 10.0, datetime(2023, 2, 1, 8, 0), datetime(2023, 2, 1, 8, 15), "yellow"),
            (1, 2.0, 10.0, datetime(2023, 2, 1, 8, 0), datetime(2023, 2, 1, 8, 30), "yellow"),
            (1, 2.0, 10.0, datetime(2023, 2, 1, 8, 0), datetime(2023, 2, 1, 8, 45), "yellow"),
        ]
        df = criar_df(spark, dados)
        corte = calcular_corte_iqr(df)
        assert corte > 0