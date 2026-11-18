"""
Dashboard Interactivo de Monitoreo - NBA Game & Market Intelligence
Visualiza métricas de la Capa Gold (Data Mart) y estado de la Arquitectura Medallion.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px

# Configuración de página
st.set_page_config(
    page_title="NBA Analytics | Medallion Lakehouse",
    page_icon="🏀",
    layout="wide"
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_PATH = os.path.join(BASE_DIR, "data", "gold", "fact_price_volatility")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "ticks")

@st.cache_data
def load_gold_data():
    if os.path.exists(GOLD_PATH):
        try:
            return pd.read_parquet(GOLD_PATH)
        except Exception:
            pass
    # Datos de demostración en caso de no haber corrido el pipeline completo aún
    return pd.DataFrame({
        "market_id": [f"market_nba_{i}" for i in range(1, 9)],
        "team": ["BOS", "LAL", "GSW", "MIA", "DEN", "MIL", "PHX", "DAL"],
        "event_date": ["2024-04-15"] * 8,
        "avg_price": [0.72, 0.45, 0.58, 0.35, 0.81, 0.65, 0.49, 0.53],
        "min_price": [0.65, 0.38, 0.50, 0.28, 0.75, 0.58, 0.40, 0.47],
        "max_price": [0.78, 0.52, 0.65, 0.42, 0.88, 0.71, 0.56, 0.60],
        "volatility_index": [0.045, 0.062, 0.055, 0.071, 0.038, 0.049, 0.066, 0.051],
        "total_volume_traded": [154000.0, 98000.0, 134000.0, 67000.0, 189000.0, 112000.0, 89000.0, 104000.0],
        "tick_count": [45000, 31000, 39000, 22000, 51000, 34000, 28000, 33000]
    })

df_gold = load_gold_data()

# Encabezado
st.title("🏀 NBA Market & Game Intelligence Platform")
st.caption("Arquitectura Medallion en Streaming con Apache Kafka, Apache Spark & Lakehouse")

st.markdown("---")

# Métricas Principales (KPI Cards)
col1, col2, col3, col4 = st.columns(4)

total_ticks = df_gold["tick_count"].sum()
total_vol = df_gold["total_volume_traded"].sum()
avg_prob = df_gold["avg_price"].mean() * 100
max_volatility_team = df_gold.sort_values(by="volatility_index", ascending=False).iloc[0]["team"]

col1.metric("Eventos Procesados (Ticks)", f"{total_ticks:,.0f}")
col2.metric("Volumen Total Transado", f"${total_vol:,.2f}")
col3.metric("Probabilidad Promedio", f"{avg_prob:.1f}%")
col4.metric("Equipo con Mayor Volatilidad", max_volatility_team)

st.markdown("---")

# Gráficos Analíticos
tab1, tab2, tab3 = st.tabs(["📊 Probabilidades y Volatilidad", "📈 Detalle por Equipo", "🏛️ Arquitectura Medallion"])

with tab1:
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("Probabilidad Implícita de Victoria por Equipo")
        fig_prob = px.bar(
            df_gold, 
            x="team", 
            y="avg_price", 
            color="avg_price",
            color_continuous_scale="Viridis",
            labels={"avg_price": "Probabilidad (0 a 1)", "team": "Equipo NBA"},
            title="Probabilidad Promedio en el Mercado"
        )
        st.plotly_chart(fig_prob, use_container_width=True)

    with col_right:
        st.subheader("Índice de Volatilidad de Cuotas")
        fig_vol = px.scatter(
            df_gold,
            x="total_volume_traded",
            y="volatility_index",
            size="tick_count",
            color="team",
            hover_name="market_id",
            labels={"total_volume_traded": "Volumen Transado ($)", "volatility_index": "Índice de Volatilidad"},
            title="Volatilidad vs. Volumen Transado"
        )
        st.plotly_chart(fig_vol, use_container_width=True)

with tab2:
    st.subheader("Tabla de Hechos: `fact_price_volatility` (Capa Gold)")
    st.dataframe(df_gold, use_container_width=True)

with tab3:
    st.subheader("Estado de las Capas del Lakehouse")
    c1, c2, c3 = st.columns(3)
    c1.success("🥉 **Capa Bronze:** Ingesta Raw desde Kafka (Parquet + Checkpoints)")
    c2.info("🥈 **Capa Silver:** Deduplicación + Broadcast Join con Mercados")
    c3.warning("🥇 **Capa Gold:** Data Mart Dimensional Kimball + PostgreSQL")

    st.markdown("""
    * **Broker de Streaming:** Apache Kafka (4 particiones balanceadas por `market_id`)
    * **Motor de Cómputo:** Apache Spark 3.5 (Structured Streaming & Spark SQL)
    * **Serving Database:** PostgreSQL (`nba_gold`)
    """)
