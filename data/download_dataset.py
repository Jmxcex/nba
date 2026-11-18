"""
Script para descargar y preparar los datos de la NBA desde Hugging Face (epinnock/polymarket-nba).
Descarga las tablas dimensionales ('events', 'markets') y un bloque representativo de 'prices' y 'trades'.
"""

import os
import sys
import pandas as pd
from datasets import load_dataset

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
os.makedirs(RAW_DIR, exist_ok=True)

def download_data():
    print("=" * 60)
    print("DESCARGA DE DATOS: epinnock/polymarket-nba (Hugging Face)")
    print("=" * 60)

    # 1. Descargar Dimensiones (Events y Markets)
    print("\n[1/3] Descargando tabla dimensional 'events' (Partidos NBA)...")
    try:
        ds_events = load_dataset("epinnock/polymarket-nba", "events", split="train")
        df_events = ds_events.to_pandas()
        events_path = os.path.join(RAW_DIR, "events.parquet")
        df_events.to_parquet(events_path, index=False)
        print(f"  -> Guardado: {events_path} ({len(df_events):,} filas)")
    except Exception as e:
        print(f"  -> Error al descargar events: {e}")

    print("\n[2/3] Descargando tabla dimensional 'markets' (Mercados NBA)...")
    try:
        ds_markets = load_dataset("epinnock/polymarket-nba", "markets", split="train")
        df_markets = ds_markets.to_pandas()
        markets_path = os.path.join(RAW_DIR, "markets.parquet")
        df_markets.to_parquet(markets_path, index=False)
        print(f"  -> Guardado: {markets_path} ({len(df_markets):,} filas)")
    except Exception as e:
        print(f"  -> Error al descargar markets: {e}")

    # 3. Descargar Muestra de Streaming de Trades / Prices
    print("\n[3/3] Descargando bloque de streaming 'trades'...")
    try:
        # Descargamos los primeros 500,000 registros para simulación inmediata
        ds_trades = load_dataset("epinnock/polymarket-nba", "trades", split="train[:500000]")
        df_trades = ds_trades.to_pandas()
        trades_path = os.path.join(RAW_DIR, "trades_sample.parquet")
        df_trades.to_parquet(trades_path, index=False)
        print(f"  -> Guardado: {trades_path} ({len(df_trades):,} filas listas para streaming)")
    except Exception as e:
        print(f"  -> Error al descargar trades: {e}")

    print("\n¡Descarga completada con éxito en la carpeta data/raw!")

if __name__ == "__main__":
    download_data()

