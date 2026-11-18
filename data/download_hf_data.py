"""
Script de Descarga de Datos Oficiales de Hugging Face
Descarga los archivos Parquet reales del dataset epinnock/polymarket-nba (+82.9M de registros).
"""

import os
import shutil
from huggingface_hub import hf_hub_download
import pyarrow.parquet as pq

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
os.makedirs(RAW_DIR, exist_ok=True)

REPO_ID = "epinnock/polymarket-nba"

FILES_TO_DOWNLOAD = [
    ("data/events.parquet", "events.parquet"),
    ("data/markets.parquet", "markets.parquet"),
    ("data/prices/prices-2024-25.parquet", "prices-2024-25.parquet"),
    ("data/prices/prices-2025-26.parquet", "prices-2025-26.parquet")
]

def download_official_nba_dataset():
    print("=" * 65)
    print("DESCARGA DE DATASET OFICIAL: epinnock/polymarket-nba (Hugging Face)")
    print(f"Destino local: {RAW_DIR}")
    print("=" * 65)

    total_rows = 0

    for hf_path, local_name in FILES_TO_DOWNLOAD:
        target_path = os.path.join(RAW_DIR, local_name)
        
        if os.path.exists(target_path):
            print(f"\n[✓] Archivo ya presente: {local_name}")
        else:
            print(f"\n[*] Descargando {hf_path} desde Hugging Face...")
            cached_file = hf_hub_download(repo_id=REPO_ID, filename=hf_path, repo_type="dataset")
            shutil.copy(cached_file, target_path)
            print(f"  -> Guardado en: {target_path}")

        try:
            num_rows = pq.ParquetFile(target_path).metadata.num_rows
            file_size_mb = os.path.getsize(target_path) / (1024 * 1024)
            print(f"  -> Filas: {num_rows:,} | Tamaño: {file_size_mb:.2f} MB")
            if "prices" in local_name:
                total_rows += num_rows
        except Exception as e:
            print(f"  -> Error leyendo metadatos: {e}")

    print("\n" + "=" * 65)
    print(f"[+] ¡DESCARGA COMPLETADA! Total registros de precios: {total_rows:,}")
    print("=" * 65)

if __name__ == "__main__":
    download_official_nba_dataset()

