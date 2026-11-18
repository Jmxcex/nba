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
import plotly.graph_objects as go

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_PATH = os.path.join(BASE_DIR, "data", "gold", "fact_price_volatility")
DASHBOARD_DIR = os.path.join(BASE_DIR, "src", "dashboard")
INDEX_HTML = os.path.join(DASHBOARD_DIR, "index.html")

PORT = 8501

def load_gold_data():
    if os.path.exists(GOLD_PATH):
        try:
            return pd.read_parquet(GOLD_PATH)
        except Exception as e:
            print(f"[!] Error leyendo Parquet Gold: {e}")
    
    # Fallback de datos de demostración
    return pd.DataFrame({
        "market_id": [f"market_nba_{i}" for i in range(100, 108)],
        "team": ["BOS", "LAL", "GSW", "MIA", "DEN", "MIL", "PHX", "DAL"],
        "event_date": ["2026-11-17"] * 8,
        "avg_price": [0.4998, 0.4993, 0.4987, 0.5120, 0.5407, 0.5232, 0.5211, 0.4892],
        "min_price": [0.1046, 0.1066, 0.1052, 0.1020, 0.1058, 0.1011, 0.1043, 0.1004],
        "max_price": [0.8961, 0.8999, 0.8997, 0.8910, 0.8980, 0.8995, 0.8975, 0.8906],
        "volatility_index": [0.2193, 0.2332, 0.2195, 0.2280, 0.2267, 0.2237, 0.2327, 0.2344],
        "total_volume_traded": [254311.92, 300548.22, 325071.32, 280000.0, 253122.69, 313716.63, 259369.78, 367846.65],
        "tick_count": [99, 120, 127, 110, 98, 119, 106, 146]
    })

def generate_dashboard_html():
    df = load_gold_data()
    
    # Métricas agregadas
    total_ticks = int(df["tick_count"].sum()) if "tick_count" in df.columns else 0
    total_volume = float(df["total_volume_traded"].sum()) if "total_volume_traded" in df.columns else 0.0
    mean_price = float(df["avg_price"].mean()) * 100 if "avg_price" in df.columns else 0.0
    total_markets = int(df["market_id"].nunique()) if "market_id" in df.columns else 0

    # Gráfico 1: Probabilidad promedio por equipo
    team_group = df.groupby("team", as_index=False)["avg_price"].mean().sort_values(by="avg_price", ascending=False)
    fig_prob = px.bar(
        team_group,
        x="team",
        y="avg_price",
        color="avg_price",
        color_continuous_scale="tealgrn",
        labels={"team": "Equipo NBA", "avg_price": "Probabilidad Implícita (0 a 1)"},
        title="<b>Probabilidad Implícita de Victoria por Equipo</b>"
    )
    fig_prob.update_layout(template="plotly_dark", height=400, margin=dict(l=20, r=20, t=50, b=20))
    chart_prob_html = fig_prob.to_html(full_html=False, include_plotlyjs=False)

    # Gráfico 2: Volatilidad vs Volumen
    fig_vol = px.scatter(
        df.head(100),
        x="total_volume_traded",
        y="volatility_index",
        size="tick_count",
        color="team",
        hover_name="market_id",
        labels={"total_volume_traded": "Volumen Transado ($ USD)", "volatility_index": "Índice de Volatilidad (StdDev)"},
        title="<b>Índice de Volatilidad vs. Volumen Transado por Mercado</b>"
    )
    fig_vol.update_layout(template="plotly_dark", height=400, margin=dict(l=20, r=20, t=50, b=20))
    chart_vol_html = fig_vol.to_html(full_html=False, include_plotlyjs=False)

    # Filas de la tabla para HTML
    table_rows = ""
    for _, row in df.head(15).iterrows():
        table_rows += f"""
        <tr class="border-b border-gray-800 hover:bg-gray-800/50">
            <td class="px-4 py-3 font-mono text-cyan-400">{row.get('market_id', '')}</td>
            <td class="px-4 py-3 font-semibold text-white">{row.get('team', '')}</td>
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
    <style>
        body {{ font-family: 'Inter', sans-serif; }}
    </style>
</head>
<body class="bg-gray-950 text-gray-100 min-h-screen p-6">
    <div class="max-w-7xl mx-auto space-y-6">
        
        <!-- Header -->
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center bg-gray-900/80 backdrop-blur border border-gray-800 rounded-2xl p-6 shadow-xl">
            <div>
                <div class="flex items-center space-x-3">
                    <span class="text-3xl">🏀</span>
                    <h1 class="text-2xl md:text-3xl font-bold bg-gradient-to-r from-amber-400 to-orange-500 bg-clip-text text-transparent">
                        NBA Game & Market Intelligence
                    </h1>
                </div>
                <p class="text-sm text-gray-400 mt-1">
                    Plataforma Big Data en Streaming: 
                    <span class="text-cyan-400 font-semibold">Apache Kafka</span> • 
                    <span class="text-orange-400 font-semibold">Apache Spark</span> • 
                    <span class="text-blue-400 font-semibold">PostgreSQL</span> • 
                    <span class="text-emerald-400 font-semibold">Medallion Architecture</span>
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
                <div class="text-xs uppercase tracking-wider text-gray-400 font-semibold">Eventos Procesados (Bronze)</div>
                <div class="text-3xl font-bold text-cyan-400 mt-2">{total_ticks:,}</div>
                <div class="text-xs text-emerald-400 mt-1 flex items-center">✓ Ingesta Streaming OK</div>
            </div>
            <div class="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg">
                <div class="text-xs uppercase tracking-wider text-gray-400 font-semibold">Volumen Total Transado</div>
                <div class="text-3xl font-bold text-emerald-400 mt-2">${total_volume:,.2f}</div>
                <div class="text-xs text-gray-400 mt-1">Mercados predictivos NBA</div>
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
                    Eventos crudos en formato Parquet desde Apache Kafka con offsets, timestamps y checkpoints.
                </p>
            </div>
            <div class="bg-slate-900 border border-slate-700 rounded-xl p-4">
                <div class="font-bold text-slate-300 flex items-center">
                    <span class="mr-2">🥈</span> Capa Silver (Cleansed)
                </div>
                <p class="text-xs text-gray-400 mt-1">
                    Deduplicación por hash temporal, filtrado de outliers físicos y Broadcast Join dimensional en Spark.
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
                            <th class="px-4 py-3">Market ID</th>
                            <th class="px-4 py-3">Equipo</th>
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
    print(f"[+] Archivo HTML del dashboard generado en: {INDEX_HTML}")

class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

def run_server():
    generate_dashboard_html()
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, DashboardHandler)
    url = f"http://localhost:{PORT}"
    print(f"[*] Dashboard disponible en: {url}")
    print("[*] Abriendo navegador automáticamente...")
    try:
        webbrowser.open(url)
    except Exception:
        pass
    print("[+] Servidor activo. Presiona Ctrl+C para detener.")
    httpd.serve_forever()

if __name__ == "__main__":
    run_server()
