import pandas as pd
import psycopg
import streamlit as st

from src.config import NEON_DATABASE_URL


@st.cache_data(ttl=600)
def carregar_dados():
    with psycopg.connect(NEON_DATABASE_URL) as conexao:
        with conexao.cursor() as cursor:
            cursor.execute(
                "SELECT cidade, data_hora, temperatura, umidade, chuva "
                "FROM historico_clima ORDER BY data_hora"
            )
            registros = cursor.fetchall()

    df = pd.DataFrame(
        registros,
        columns=["cidade", "data_hora", "temperatura", "umidade", "chuva"],
    )
    df["data_hora"] = pd.to_datetime(df["data_hora"])
    for coluna in ["temperatura", "umidade", "chuva"]:
        df[coluna] = pd.to_numeric(df[coluna])
    return df


st.set_page_config(page_title="Clima nas cidades", layout="wide")
st.title("Clima nas cidades")
st.caption("Dados horários da Open-Meteo tratados pelo pipeline e armazenados no Neon.")

if not NEON_DATABASE_URL:
    st.error("Configure NEON_DATABASE_URL (ou DATABASE_URL) no arquivo .env.")
    st.stop()

try:
    dados = carregar_dados()
except psycopg.Error:
    st.error("Não foi possível ler os dados do Neon. Confira a conexão e execute o pipeline antes de abrir o dashboard.")
    st.stop()

if dados.empty:
    st.info("Ainda não há medições. Execute o pipeline para carregar os dados.")
    st.stop()

cidade = st.selectbox("Cidade", sorted(dados["cidade"].unique()))
dados_cidade = dados[dados["cidade"] == cidade]
data_minima = dados_cidade["data_hora"].min().date()
data_maxima = dados_cidade["data_hora"].max().date()
periodo = st.date_input(
    "Período",
    value=(data_minima, data_maxima),
    min_value=data_minima,
    max_value=data_maxima,
    format="DD/MM/YYYY",
    key=f"periodo_{cidade}",
)

if len(periodo) != 2:
    st.info("Selecione a data inicial e a data final.")
    st.stop()

filtro = dados_cidade[dados_cidade["data_hora"].dt.date.between(*periodo)]
if filtro.empty:
    st.info("Não há medições no período selecionado.")
    st.stop()

temperatura_media = filtro["temperatura"].mean()
st.metric(
    "Temperatura média no período",
    "Sem dados" if pd.isna(temperatura_media) else f"{temperatura_media:.1f} °C",
)
st.subheader("Temperatura por hora (°C)")
st.line_chart(filtro, x="data_hora", y="temperatura")
st.caption("A média considera apenas temperaturas disponíveis. Dados ausentes não são substituídos por zero.")
st.subheader("Medições do período")
st.dataframe(filtro, hide_index=True)
st.caption("Horários em America/Sao_Paulo. Consultas ao Neon ficam em cache por até 10 minutos.")
