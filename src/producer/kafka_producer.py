"""
Productor de Alto Rendimiento para Apache Kafka con Datos Reales de la NBA
Lee por lotes (iter_batches) directamente desde los archivos Parquet oficiales (+82.1M de filas)
Optimizaciones: Streaming con PyArrow RecordBatches, Linger (50ms), Compresión GZIP y Key por condition_id.
"""

import json
import time
import os
import sys
import pandas as pd
import pyarrow.parquet as pq
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nba.ticks.raw")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PRICES_PATH = os.path.join(BASE_DIR, "data", "raw", "prices-2024-25.parquet")

# Cantidad de eventos a transmitir en la sesión (por defecto 200,000 en tiempo real; configurable por CLI o ENV)
STREAM_LIMIT = int(os.getenv("STREAM_LIMIT", "200000"))

def create_kafka_producer():
    print(f"[*] Conectando productor a Kafka Broker: {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        acks=1,
        retries=5,
        batch_size=65536,               # 64 KB por lote
        linger_ms=50,                   # Acumulación en buffer
        compression_type='gzip',        # Compresión GZIP
        max_in_flight_requests_per_connection=5
    )
    print("[+] Productor conectado con éxito.")
    return producer

def stream_real_nba_ticks():
    producer = create_kafka_producer()

    if not os.path.exists(PRICES_PATH):
        print(f"[!] Error: No se encontró el archivo real en {PRICES_PATH}. Ejecuta download_hf_data.py primero.")
        return

    parquet_file = pq.ParquetFile(PRICES_PATH)
    total_in_file = parquet_file.metadata.num_rows
    print(f"[*] Archivo fuente real: {PRICES_PATH}")
    print(f"[*] Total disponible en archivo: {total_in_file:,} registros reales de la NBA.")
    print(f"[*] Límite de transmisión para este flujo: {STREAM_LIMIT:,} eventos.")

    start_time = time.time()
    sent_count = 0

    # Lectura eficiente en batches con PyArrow para no saturar memoria RAM
    batch_size = 25000
    for batch in parquet_file.iter_batches(batch_size=batch_size, columns=["condition_id", "timestamp", "price", "token_id"]):
        df_batch = batch.to_pandas()
        
        for _, row in df_batch.iterrows():
            ts = row["timestamp"]
            # Convertir timestamp si es nanosegundos o Timestamp de pandas
            ts_val = int(ts.timestamp()) if hasattr(ts, "timestamp") else int(ts) if pd.notna(ts) else int(time.time())

            record = {
                "condition_id": str(row["condition_id"]),
                "token_id": str(row.get("token_id", "")),
                "timestamp": ts_val,
                "price": float(row["price"]) if pd.notna(row["price"]) else 0.0,
                "source": "huggingface_polymarket_nba"
            }

            key = record["condition_id"]
            producer.send(KAFKA_TOPIC, key=key, value=record)
            sent_count += 1

            if sent_count % 25000 == 0:
                elapsed = time.time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                print(f"  -> {sent_count:,} / {STREAM_LIMIT:,} eventos enviados | Tasa: {rate:,.0f} msg/seg")

            if sent_count >= STREAM_LIMIT:
                break
        
        if sent_count >= STREAM_LIMIT:
            break

    producer.flush()
    total_time = time.time() - start_time
    avg_rate = sent_count / total_time if total_time > 0 else 0
    print(f"\n[+] Transmisión finalizada: {sent_count:,} eventos reales enviados en {total_time:.2f}s ({avg_rate:,.0f} msg/seg)")
    producer.close()

if __name__ == "__main__":
    stream_real_nba_ticks()
