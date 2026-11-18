"""
Productor de Alto Rendimiento para Apache Kafka
Publica eventos de cambios de precios y transacciones deportivas (NBA)
Optimizaciones: Batching (64KB), Linger (50ms), Compresión GZIP y Message Key por market_id.
"""

import json
import time
import os
import sys
import pandas as pd
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nba.ticks.raw")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_PATH = os.path.join(BASE_DIR, "data", "raw", "trades_sample.parquet")

def create_kafka_producer():
    print(f"[*] Conectando productor a Kafka Broker: {KAFKA_BROKER}...")
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8'),
        acks=1,                         # Confirmación rápida
        retries=5,                      # Reintentos ante fallas temporales
        batch_size=65536,               # 64 KB por lote
        linger_ms=50,                   # Esperar hasta 50ms para acumular mensajes en lote
        compression_type='gzip',        # Compresión eficiente para millones de eventos
        max_in_flight_requests_per_connection=5
    )
    print("[+] Productor conectado con éxito.")
    return producer

def generate_mock_stream_data():
    """Genera datos sintéticos realistas en caso de no haber descargado el parquet aún"""
    import random
    from datetime import datetime
    teams = ["BOS", "LAL", "GSW", "MIA", "DEN", "MIL", "PHX", "DAL"]
    markets = [f"market_nba_{i}" for i in range(100, 150)]
    
    events = []
    for _ in range(50000):
        m_id = random.choice(markets)
        events.append({
            "market_id": m_id,
            "timestamp": int(time.time()),
            "price": round(random.uniform(0.10, 0.90), 4),
            "size": round(random.uniform(50, 5000), 2),
            "side": random.choice(["BUY", "SELL"]),
            "team": random.choice(teams),
            "source": "synthetic_generator"
        })
    return pd.DataFrame(events)

def start_producing():
    producer = create_kafka_producer()
    
    # Cargar datos reales o generar flujo
    if os.path.exists(RAW_DATA_PATH):
        print(f"[*] Leyendo datos fuente desde: {RAW_DATA_PATH}")
        df = pd.read_parquet(RAW_DATA_PATH)
    else:
        print("[!] No se encontró trades_sample.parquet. Generando lote sintético de alta velocidad...")
        df = generate_mock_stream_data()

    total_records = len(df)
    print(f"[*] Iniciando transmisión de {total_records:,} eventos hacia el tópico '{KAFKA_TOPIC}'...")

    start_time = time.time()
    sent_count = 0

    try:
        for idx, row in df.iterrows():
            record = row.to_dict()
            # Asegurar compatibilidad de tipos JSON
            for k, v in record.items():
                if pd.isna(v):
                    record[k] = None
                elif hasattr(v, "isoformat"):
                    record[k] = v.isoformat()

            key = str(record.get("market_id", "default_market"))
            
            # Enviar mensaje asíncrono con Message Key
            producer.send(KAFKA_TOPIC, key=key, value=record)
            sent_count += 1

            if sent_count % 10000 == 0:
                elapsed = time.time() - start_time
                rate = sent_count / elapsed if elapsed > 0 else 0
                print(f"  -> {sent_count:,} / {total_records:,} eventos enviados | Tasa: {rate:,.0f} msg/seg")

        # Forzar el vaciado del buffer final
        producer.flush()
        total_time = time.time() - start_time
        avg_rate = sent_count / total_time if total_time > 0 else 0
        print(f"\n[+] Transmisión completada: {sent_count:,} mensajes enviados en {total_time:.2f}s ({avg_rate:,.0f} msg/seg)")

    except KeyboardInterrupt:
        print("\n[!] Transmisión interrumpida por el usuario.")
    finally:
        producer.close()
        print("[*] Conexión de Kafka cerrada.")

if __name__ == "__main__":
    start_producing()
