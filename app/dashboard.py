"""
dashboard.py — Interface web com Streamlit
-------------------------------------------
Exibe os resultados das perguntas do case iFood de forma
interativa, lendo diretamente das tabelas Gold no MinIO.
 
Não depende do Spark — usa pandas e pyarrow para leitura
dos arquivos Parquet, o que torna a interface muito mais rápida.
 
Acesso: http://localhost:8501
"""
 
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import io
import os
 
# ── Configuração da página ────────────────────────────────────────────────────
st.set_page_config(
    page_title="iFood — NYC Taxi Dashboard",
    page_icon="🚕",
    layout="wide",
)
 
# ── Configuração do MinIO ─────────────────────────────────────────────────────
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET           = "ifood-data-lake"
 
MESES = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai"}
 
 
# ── Função para ler tabelas Gold do MinIO ─────────────────────────────────────
@st.cache_data(ttl=300)  # cache de 5 minutos para não reler a cada interação
def ler_tabela_gold(prefixo: str) -> pd.DataFrame:
    """
    Lê uma tabela Delta/Parquet do MinIO e retorna como DataFrame pandas.
    O @st.cache_data evita reler os dados a cada interação do usuario.
    """
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )
 
    # Lista todos os arquivos Parquet na pasta
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefixo)
    arquivos = [
        obj["Key"] for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]
 
    if not arquivos:
        return pd.DataFrame()
 
    # Lê e concatena todos os arquivos Parquet
    dfs = []
    for chave in arquivos:
        obj = s3.get_object(Bucket=BUCKET, Key=chave)
        buffer = io.BytesIO(obj["Body"].read())
        dfs.append(pq.read_table(buffer).to_pandas())
 
    return pd.concat(dfs, ignore_index=True)
 
 
# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚕 NYC Taxi — Case iFood")
st.markdown("Dashboard de análise das corridas de táxi de Nova York — Janeiro a Maio de 2023")
st.divider()
 
# ── Verifica se os dados estão disponíveis ────────────────────────────────────
try:
    df_month    = ler_tabela_gold("gold/avg_total_by_month/")
    df_hour     = ler_tabela_gold("gold/avg_passengers_by_hour/")
    df_type     = ler_tabela_gold("gold/trips_by_taxi_type/")
    df_peak     = ler_tabela_gold("gold/top_hours_by_taxi_type/")
    dados_ok = True
except Exception as e:
    dados_ok = False
    st.error(f"Não foi possível conectar ao MinIO: {e}")
    st.info("Verifique se o pipeline foi executado e se o MinIO está rodando em http://localhost:9001")
    st.stop()
 
# ── Prepara os dados ──────────────────────────────────────────────────────────
df_month["month"] = df_month["month"].astype(int)
df_month["mes_nome"] = df_month["month"].map(MESES)
df_month = df_month.sort_values("month")
 
df_hour["hour"] = df_hour["hour"].astype(int)
df_hour = df_hour.sort_values("hour")
 
df_type["month"] = df_type["month"].astype(int)
df_type["mes_nome"] = df_type["month"].map(MESES)
 
df_peak["hour"] = df_peak["hour"].astype(int)
 
 
# ── Métricas resumo ───────────────────────────────────────────────────────────
st.subheader("Resumo geral")
 
total_yellow = df_type[df_type["taxi_type"] == "yellow"]["total_corridas"].sum()
total_green  = df_type[df_type["taxi_type"] == "green"]["total_corridas"].sum()
ticket_yellow = df_type[df_type["taxi_type"] == "yellow"]["avg_total_amount"].mean()
ticket_green  = df_type[df_type["taxi_type"] == "green"]["avg_total_amount"].mean()
 
col1, col2, col3, col4 = st.columns(4)
col1.metric("Corridas Yellow", f"{total_yellow:,.0f}")
col2.metric("Corridas Green",  f"{total_green:,.0f}")
col3.metric("Ticket Médio Yellow", f"U$ {ticket_yellow:.2f}")
col4.metric("Ticket Médio Green",  f"U$ {ticket_green:.2f}")
 
st.divider()
 
 
# ── Pergunta 1 ────────────────────────────────────────────────────────────────
st.subheader("Pergunta 1 — Média do valor total por mês (yellow taxi)")
 
fig1, ax1 = plt.subplots(figsize=(8, 4))
bars = ax1.bar(df_month["mes_nome"], df_month["avg_total_amount"], color="#EF9F27")
ax1.bar_label(bars, fmt="U$ %.2f", padding=4, fontsize=10)
ax1.set_xlabel("Mês")
ax1.set_ylabel("Média (USD)")
ax1.set_ylim(0, df_month["avg_total_amount"].max() * 1.2)
ax1.yaxis.set_major_formatter(mticker.FormatStrFormatter("U$ %.0f"))
ax1.grid(axis="y", linestyle="--", alpha=0.3)
plt.tight_layout()
 
col_graf, col_tab = st.columns([2, 1])
with col_graf:
    st.pyplot(fig1)
with col_tab:
    st.dataframe(
        df_month[["mes_nome", "avg_total_amount", "total_corridas"]]
        .rename(columns={
            "mes_nome": "Mês",
            "avg_total_amount": "Média (USD)",
            "total_corridas": "Total corridas"
        }),
        hide_index=True,
        use_container_width=True,
    )
 
st.divider()
 
 
# ── Pergunta 2 ────────────────────────────────────────────────────────────────
st.subheader("Pergunta 2 — Média de passageiros por hora do dia — maio 2023 (todos os táxis)")
 
fig2, ax2 = plt.subplots(figsize=(12, 4))
ax2.plot(df_hour["hour"], df_hour["avg_passengers"], marker="o", color="#1D9E75", linewidth=2)
ax2.fill_between(df_hour["hour"], df_hour["avg_passengers"], alpha=0.15, color="#1D9E75")
ax2.set_xlabel("Hora do dia")
ax2.set_ylabel("Média de passageiros")
ax2.set_xticks(range(0, 24))
ax2.grid(axis="y", linestyle="--", alpha=0.4)
plt.tight_layout()
 
col_graf2, col_tab2 = st.columns([2, 1])
with col_graf2:
    st.pyplot(fig2)
with col_tab2:
    st.dataframe(
        df_hour[["hour", "avg_passengers", "total_corridas"]]
        .rename(columns={
            "hour": "Hora",
            "avg_passengers": "Média passageiros",
            "total_corridas": "Total corridas"
        }),
        hide_index=True,
        use_container_width=True,
        height=300,
    )
 
st.divider()
 
 
# ── Bônus 1: Volume por tipo e mês ───────────────────────────────────────────
st.subheader("Análise Bônus — Volume de corridas e ticket médio por tipo de táxi")

yellow = df_type[df_type["taxi_type"] == "yellow"].sort_values("month")
green  = df_type[df_type["taxi_type"] == "green"].sort_values("month")

# Usa os meses reais do DataFrame em vez de range fixo
x = range(len(yellow))
labels = yellow["mes_nome"].tolist()

fig3, axes = plt.subplots(1, 2, figsize=(14, 4))

axes[0].bar([i - 0.2 for i in x], yellow["total_corridas"], width=0.4, label="Yellow", color="#EF9F27")
axes[0].bar([i + 0.2 for i in x], green["total_corridas"].values,  width=0.4, label="Green",  color="#1D9E75")
axes[0].set_title("Volume de corridas por tipo e mês")
axes[0].set_xlabel("Mês")
axes[0].set_ylabel("Total de corridas")
axes[0].set_xticks(list(x))
axes[0].set_xticklabels(labels)
axes[0].yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e6:.1f}M"))
axes[0].legend()
axes[0].grid(axis="y", linestyle="--", alpha=0.3)

axes[1].plot(list(x), yellow["avg_total_amount"].values, marker="o", color="#EF9F27", label="Yellow", linewidth=2)
axes[1].plot(list(x), green["avg_total_amount"].values,  marker="o", color="#1D9E75", label="Green",  linewidth=2)
axes[1].set_title("Ticket médio por tipo e mês")
axes[1].set_xlabel("Mês")
axes[1].set_ylabel("Média (USD)")
axes[1].set_xticks(list(x))
axes[1].set_xticklabels(labels)
axes[1].yaxis.set_major_formatter(mticker.FormatStrFormatter("U$ %.0f"))
axes[1].legend()
axes[1].grid(axis="y", linestyle="--", alpha=0.3)

plt.tight_layout()
st.pyplot(fig3)
 
 
# ── Bônus 2: Horários de pico ─────────────────────────────────────────────────
st.subheader("Análise Bônus — Horários de pico por tipo de táxi")
 
fig4, axes2 = plt.subplots(1, 2, figsize=(14, 4))
 
for ax, taxi, cor in zip(axes2, ["yellow", "green"], ["#EF9F27", "#1D9E75"]):
    dados = df_peak[df_peak["taxi_type"] == taxi].sort_values("hour")
    ax.bar(dados["hour"], dados["total_corridas"], color=cor)
    ax.set_title(f"Corridas por hora — {taxi} taxi")
    ax.set_xlabel("Hora do dia")
    ax.set_ylabel("Total de corridas")
    ax.set_xticks(range(0, 24))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v/1e3:.0f}k"))
    ax.grid(axis="y", linestyle="--", alpha=0.3)
 
plt.tight_layout()
st.pyplot(fig4)
 
st.divider()
st.caption("Fonte: NYC Taxi & Limousine Commission (TLC) — Jan a Mai 2023 | Yellow e Green Taxi")