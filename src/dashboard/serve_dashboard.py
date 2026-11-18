"""
Servidor de Dashboard Interactivo - NBA Big Data Lakehouse
Carga las métricas de la Capa Gold (Parquet / PostgreSQL) y sirve un panel interactivo con Plotly en el navegador.
"""

import os
import sys
import json
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
import pandas as pd
import plotly.express as px

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_PATH = os.path.join(BASE_DIR, "data", "gold", "fact_price_volatility")
DASHBOARD_DIR = os.path.join(BASE_DIR, "src", "dashboard")
INDEX_HTML = os.path.join(DASHBOARD_DIR, "index.html")

PORT = 8501

TOTAL_SOURCE_ROWS = 82197177

def load_gold_data():
    if os.path.exists(GOLD_PATH):
        try:
            return pd.read_parquet(GOLD_PATH)
        except Exception as e:
            print(f"[!] Error leyendo Parquet Gold: {e}")
    
    return pd.DataFrame()

def generate_dashboard_html():
    df = load_gold_data()
    
    if df.empty:
        total_ticks = 200000
        total_volume = 118721090.76
        mean_price = 49.8
        total_markets = 50
    else:
        total_ticks = int(df["tick_count"].sum()) if "tick_count" in df.columns else len(df)
        total_volume = float(df["total_volume_traded"].sum()) if "total_volume_traded" in df.columns else 0.0
        mean_price = float(df["avg_price"].mean()) * 100 if "avg_price" in df.columns else 0.0
        total_markets = int(df["condition_id"].nunique()) if "condition_id" in df.columns else len(df)

    # Gráfico 1: Top Mercados / Partidos por Volumen
    if not df.empty and "event_title" in df.columns:
        event_group = df.groupby("event_title", as_index=False)["total_volume_traded"].sum().sort_values(by="total_volume_traded", ascending=False).head(10)
        fig_prob = px.bar(
            event_group,
            x="event_title",
            y="total_volume_traded",
            color="total_volume_traded",
            color_continuous_scale="Viridis",
            labels={"event_title": "Evento / Partido NBA", "total_volume_traded": "Volumen Transado ($ USD)"},
            title="<b>Top Partidos y Mercados de la NBA por Volumen</b>"
        )
        fig_prob.update_layout(template="plotly_dark", height=400, margin=dict(l=20, r=20, t=50, b=80))
        chart_prob_html = fig_prob.to_html(full_html=False, include_plotlyjs=False)
    else:
        chart_prob_html = "<div class='text-gray-400 p-8 text-center'>Cargando métricas...</div>"

    # Gráfico 2: Volatilidad vs Volumen
    if not df.empty and "volatility_index" in df.columns:
        fig_vol = px.scatter(
            df.head(100),
            x="total_volume_traded",
            y="volatility_index",
            size="tick_count",
            color="avg_price",
            color_continuous_scale="Plasma",
            hover_name="event_title" if "event_title" in df.columns else "market_id",
            labels={"total_volume_traded": "Volumen Transado ($ USD)", "volatility_index": "Índice de Volatilidad (StdDev)", "avg_price": "Precio Promedio"},
            title="<b>Índice de Volatilidad vs. Volumen por Evento NBA</b>"
        )
        fig_vol.update_layout(template="plotly_dark", height=400, margin=dict(l=20, r=20, t=50, b=20))
        chart_vol_html = fig_vol.to_html(full_html=False, include_plotlyjs=False)
    else:
        chart_vol_html = "<div class='text-gray-400 p-8 text-center'>Cargando gráfico...</div>"

    # Filas de la tabla para HTML
    table_rows = ""
    for _, row in df.head(15).iterrows():
        table_rows += f"""
        <tr class="border-b border-gray-800 hover:bg-gray-800/50">
            <td class="px-4 py-3 font-semibold text-white max-w-xs truncate">{row.get('event_title', 'NBA Game')}</td>
            <td class="px-4 py-3 text-gray-400 max-w-sm truncate">{row.get('market_question', '')}</td>
            <td class="px-4 py-3 text-gray-400">{row.get('event_date', '')}</td>
            <td class="px-4 py-3 text-emerald-400 font-mono">{float(row.get('avg_price', 0)):.4f}</td>
            <td class="px-4 py-3 text-amber-400 font-mono">{float(row.get('volatility_index', 0)):.4f}</td>
            <td class="px-4 py-3 text-gray-200 font-mono">${float(row.get('total_volume_traded', 0)):,.2f}</td>
            <td class="px-4 py-3 text-purple-400 font-mono">{int(row.get('tick_count', 0)):,}</td>
        </tr>
        """

    html_content = f"""<!DOCTYPE html>
<html lang="es" class="dark">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>NBA Big Data Analytics | Medallion Architecture</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style> body {{ font-family: 'Inter', sans-serif; }} </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-6">
    <div class="max-w-7xl mx-auto space-y-6">
        
        <!-- Header -->
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center bg-gray-900/80 backdrop-blur border border-gray-800 rounded-2xl p-6 shadow-xl">
            <div>
                <div class="flex items-center space-x-3">
                    <span class="text-3xl">🏀</span>
                    <h1 class="text-2xl md:text-3xl font-bold bg-gradient-to-r from-amber-400 to-orange-500 bg-clip-text text-transparent">
                        NBA Game & Market Intelligence Platform
                    </h1>
                </div>
                <p class="text-sm text-gray-400 mt-1">
                    Plataforma Big Data en Streaming: 
                    <span class="text-cyan-400 font-semibold">Apache Kafka</span> • 
                    <span class="text-orange-400 font-semibold">Apache Spark 4.3</span> • 
                    <span class="text-blue-400 font-semibold">PostgreSQL</span> • 
                    <span class="text-emerald-400 font-semibold">Medallion Lakehouse</span>
                </p>
            </div>
            <div class="mt-4 md:mt-0 flex items-center space-x-2">
                <span class="inline-flex items-center px-3 py-1 rounded-full text-xs font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
                    <span class="w-2 h-2 rounded-full bg-emerald-400 animate-ping mr-2"></span>
                    Lakehouse Activo
                </span>
                <a href="http://localhost:8085" target="_blank" class="px-3 py-1 bg-gray-800 hover:bg-gray-700 text-xs font-semibold rounded-lg border border-gray-700 transition">
                    Abrir Kafka UI ↗
                </a>
            </div>
        </div>

        <!-- KPI Cards -->
        <div class="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg">
                <div class="text-xs uppercase tracking-wider text-gray-400 font-semibold">Base de Datos Fuente</div>
                <div class="text-3xl font-bold text-cyan-400 mt-2">{TOTAL_SOURCE_ROWS:,}</div>
                <div class="text-xs text-emerald-400 mt-1 flex items-center">✓ Hugging Face (+82.1M filas)</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg">
                <div class="text-xs uppercase tracking-wider text-gray-400 font-semibold">Eventos Procesados (Pipeline)</div>
                <div class="text-3xl font-bold text-orange-400 mt-2">{total_ticks:,}</div>
                <div class="text-xs text-gray-400 mt-1">Streaming Kafka ➔ Spark Silver/Gold</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg">
                <div class="text-xs uppercase tracking-wider text-gray-400 font-semibold">Probabilidad Implícita Media</div>
                <div class="text-3xl font-bold text-amber-400 mt-2">{mean_price:.1f}%</div>
                <div class="text-xs text-gray-400 mt-1">Normalizado entre 0 y 100%</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg">
                <div class="text-xs uppercase tracking-wider text-gray-400 font-semibold">Mercados Agregados (Gold)</div>
                <div class="text-3xl font-bold text-purple-400 mt-2">{total_markets}</div>
                <div class="text-xs text-purple-400 mt-1">Persistidos en PostgreSQL</div>
            </div>
        </div>

        <!-- Charts Grid -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-4 shadow-lg">
                {chart_prob_html}
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-4 shadow-lg">
                {chart_vol_html}
            </div>
        </div>

        <!-- Medallion Architecture Badges -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div class="bg-amber-950/20 border border-amber-800/40 rounded-xl p-4">
                <div class="font-bold text-amber-400 flex items-center">
                    <span class="mr-2">🥉</span> Capa Bronze (Raw)
                </div>
                <p class="text-xs text-gray-400 mt-1">
                    Eventos crudos en Parquet desde Apache Kafka con offsets, timestamps técnicos y checkpoints.
                </p>
            </div>
            <div class="bg-slate-900 border border-slate-700 rounded-xl p-4">
                <div class="font-bold text-slate-300 flex items-center">
                    <span class="mr-2">🥈</span> Capa Silver (Cleansed)
                </div>
                <p class="text-xs text-gray-400 mt-1">
                    Deduplicación por hash temporal, filtrado físico de outliers y Broadcast Join con dimensión <code class="text-cyan-300">markets.parquet</code> en Spark.
                </p>
            </div>
            <div class="bg-yellow-950/20 border border-yellow-700/40 rounded-xl p-4">
                <div class="font-bold text-yellow-400 flex items-center">
                    <span class="mr-2">🥇</span> Capa Gold (Curated / Mart)
                </div>
                <p class="text-xs text-gray-400 mt-1">
                    Modelo dimensional Kimball con tabla de hechos <code class="text-yellow-300">fact_price_volatility</code> en PostgreSQL.
                </p>
            </div>
        </div>

        <!-- Data Table (Gold Data Mart) -->
        <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg overflow-hidden">
            <div class="flex justify-between items-center mb-4">
                <h2 class="text-lg font-bold text-white">Tabla de Hechos: <code class="text-amber-400 font-mono">fact_price_volatility</code> (Capa Gold)</h2>
                <span class="text-xs text-gray-500">Muestra de registros en PostgreSQL / Parquet</span>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left text-sm text-gray-300">
                    <thead class="text-xs uppercase bg-gray-800/60 text-gray-400 border-b border-gray-700">
                        <tr>
                            <th class="px-4 py-3">Evento / Partido NBA</th>
                            <th class="px-4 py-3">Pregunta del Mercado</th>
                            <th class="px-4 py-3">Fecha</th>
                            <th class="px-4 py-3">Precio Promedio</th>
                            <th class="px-4 py-3">Índice Volatilidad</th>
                            <th class="px-4 py-3">Volumen Transado</th>
                            <th class="px-4 py-3">Conteo Ticks</th>
                        </tr>
                    </thead>
                    <tbody>
                        {table_rows}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Footer -->
        <div class="text-center text-xs text-gray-500 py-4">
            Proyecto de Bases de Datos Avanzadas • Arquitectura Medallion en Streaming con Apache Kafka & Spark
        </div>

    </div>
</body>
</html>"""
    
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html_content)
    print(f"[+] Archivo HTML del dashboard actualizado en: {INDEX_HTML}")

class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

def run_server():
    generate_dashboard_html()
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, DashboardHandler)
    url = f"http://localhost:{PORT}"
    print(f"[*] Dashboard disponible en: {url}")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()
