"""
logger.py — Módulo central de logging
---------------------------------------
Configura e fornece um logger padronizado para todos os scripts
do pipeline. Usar este módulo garante consistência no formato
dos logs em todas as etapas.
 
Uso em qualquer script:
    from logger import obter_logger
    logger = obter_logger(__name__)
    logger.info("Mensagem informativa")
    logger.warning("Algo inesperado mas não crítico")
    logger.error("Algo deu errado", exc_info=True)
 
Níveis de log:
    DEBUG   — detalhes internos (desativado em produção)
    INFO    — progresso normal do pipeline
    WARNING — algo inesperado mas o pipeline continua
    ERROR   — falha em uma operação específica
    CRITICAL— falha que impede o pipeline de continuar
"""
 
import logging
import sys
from datetime import datetime
 
 
# ── Formato do log ────────────────────────────────────────────────────────────
# Exemplo de saída:
# 2024-01-15 10:23:45 | INFO     | 01_ingestion | Download concluído — yellow_tripdata_2023-01.parquet
FORMATO = "%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s"
DATA_FORMATO = "%Y-%m-%d %H:%M:%S"
 
 
def obter_logger(nome: str, nivel: int = logging.INFO) -> logging.Logger:
    """
    Cria e retorna um logger configurado para o módulo informado.
 
    Parâmetros:
        nome  — nome do módulo (use __name__ para pegar automaticamente)
        nivel — nível mínimo de log (padrão: INFO)
 
    Retorna:
        logging.Logger configurado com handler para o terminal
    """
    logger = logging.getLogger(nome)
 
    # Evita adicionar handlers duplicados se o logger já foi configurado
    if logger.handlers:
        return logger
 
    logger.setLevel(nivel)
 
    # Handler para o terminal (stdout)
    # Em produção este handler seria substituído por um que envia
    # para CloudWatch, Datadog ou outro sistema de observabilidade
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(nivel)
 
    formatter = logging.Formatter(fmt=FORMATO, datefmt=DATA_FORMATO)
    handler.setFormatter(formatter)
 
    logger.addHandler(handler)
 
    # Não propaga para o root logger para evitar logs duplicados
    logger.propagate = False
 
    return logger
 
 
def obter_logger_arquivo(nome: str, caminho_arquivo: str = None, nivel: int = logging.INFO) -> logging.Logger:
    """
    Cria um logger que escreve tanto no terminal quanto em um arquivo.
    Útil para preservar histórico de execuções do pipeline.
 
    Parâmetros:
        nome           — nome do módulo
        caminho_arquivo — caminho do arquivo de log (ex: "logs/pipeline.log")
        nivel          — nível mínimo de log
    """
    logger = obter_logger(nome, nivel)
 
    if caminho_arquivo:
        import os
        os.makedirs(os.path.dirname(caminho_arquivo), exist_ok=True)
 
        file_handler = logging.FileHandler(caminho_arquivo, encoding="utf-8")
        file_handler.setLevel(nivel)
        file_handler.setFormatter(
            logging.Formatter(fmt=FORMATO, datefmt=DATA_FORMATO)
        )
        logger.addHandler(file_handler)
 
    return logger
 
 
class LoggerPipeline:
    """
    Contexto de execução de uma etapa do pipeline.
    Registra automaticamente início, fim e duração de cada etapa.
 
    Uso:
        with LoggerPipeline(logger, "Ingestão Bronze") as ctx:
            # código da etapa
            ctx.info(f"Baixando {arquivo}")
    """
 
    def __init__(self, logger: logging.Logger, nome_etapa: str):
        self.logger = logger
        self.nome_etapa = nome_etapa
        self.inicio = None
 
    def __enter__(self):
        self.inicio = datetime.now()
        self.logger.info(f"[INÍCIO] {self.nome_etapa}")
        return self
 
    def __exit__(self, tipo_exc, valor_exc, traceback):
        duracao = (datetime.now() - self.inicio).total_seconds()
 
        if tipo_exc is None:
            self.logger.info(f"[FIM] {self.nome_etapa} — concluído em {duracao:.1f}s")
        else:
            self.logger.error(
                f"[ERRO] {self.nome_etapa} — falhou após {duracao:.1f}s: {valor_exc}"
            )
        # Retorna False para não suprimir exceções
        return False
 
    def info(self, mensagem: str):
        self.logger.info(mensagem)
 
    def warning(self, mensagem: str):
        self.logger.warning(mensagem)
 
    def error(self, mensagem: str):
        self.logger.error(mensagem)