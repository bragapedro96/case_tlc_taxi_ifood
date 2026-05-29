# Dockerfile — Airflow com PySpark e Delta Lake
# Extende a imagem oficial do Airflow adicionando
# as dependências necessárias para rodar os scripts do pipeline.
 
FROM apache/airflow:2.9.1
 
# Instala Java — necessário para o PySpark
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
 
# Define a variável de ambiente do Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
 
# Volta para o usuário padrão do Airflow
USER airflow
 
# Instala PySpark, Delta Lake e dependências do pipeline
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.2.0 \
    boto3==1.34.0 \
    requests==2.31.0