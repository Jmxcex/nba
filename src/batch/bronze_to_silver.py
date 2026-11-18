"""
Procesamiento Batch en Spark: Capa Bronze -> Capa Silver
Aplica deduplicación, filtrado de calidad física, tipado y Broadcast Join con dimensiones.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime, to_date, broadcast, when

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "ticks")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "ticks")
MARKETS_DIM_PATH = os.path.join(BASE_DIR, "data", "raw", "markets.parquet")

def run_bronze_to_silver():
    print("=" * 60)
    print("PROCESAMIENTO SPARK: CAPA BRONZE -> CAPA SILVER")
    print(f"Origen: {BRONZE_PATH} | Destino: {SILVER_PATH}")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("NBA-Bronze-to-Silver-ETL") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(BRONZE_PATH):
        print(f"[!] Directorio Bronze no encontrado en: {BRONZE_PATH}. Ejecuta primero el stream de Bronze.")
        return

    # 1. Leer datos de la Capa Bronze
    print("[*] Leyendo datos de la Capa Bronze...")
    df_bronze = spark.read.parquet(BRONZE_PATH)
    initial_count = df_bronze.count()
    print(f"  -> Total de registros crudos en Bronze: {initial_count:,}")

    # 2. Limpieza y Tipado
    # - Conversión de UNIX timestamp a TimestampType y DateType
    # - Filtrado de precios anómalos (0.01 <= price <= 0.99)
    # - Deduplicación de eventos
    print("[*] Aplicando reglas de calidad de datos y deduplicación...")
    df_clean = df_bronze \
        .filter(col("market_id").isNotNull()) \
        .filter((col("price") > 0.0) & (col("price") < 1.0)) \
        .dropDuplicates(["market_id", "timestamp", "price"]) \
        .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp")))) \
        .withColumn("event_date", to_date(col("event_timestamp")))

    # 3. Cargar tabla dimensional de Mercados (o crear dimensión sintética)
    if os.path.exists(MARKETS_DIM_PATH):
        print(f"[*] Cargando dimensión de mercados desde: {MARKETS_DIM_PATH}")
        df_markets = spark.read.parquet(MARKETS_DIM_PATH).select(
            col("id").alias("market_id"),
            col("question").alias("market_question"),
            col("condition_id"),
            col("slug")
        )
    else:
        print("[!] No se encontró markets.parquet. Creando dimensión en memoria...")
        mock_markets = [
            ("market_nba_101", "Will Boston Celtics win the 2024 NBA Finals?", "cond_1", "celtics-win-2024"),
            ("market_nba_102", "Will LA Lakers reach Western Finals?", "cond_2", "lakers-reach-finals"),
            ("market_nba_103", "Will Golden State Warriors make playoffs?", "cond_3", "warriors-playoffs")
        ]
        df_markets = spark.createDataFrame(mock_markets, ["market_id", "market_question", "condition_id", "slug"])

    # 4. OPTIMIZACIÓN SPARK: Broadcast Hash Join
    # Evita el Shuffle de red al replicar la tabla dimensional pequeña en cada worker
    print("[*] Ejecutando Broadcast Hash Join (Optimización avanzada de Spark)...")
    df_silver = df_clean.join(broadcast(df_markets), on="market_id", how="left")

    silver_count = df_silver.count()
    print(f"  -> Registros refinados en Capa Silver: {silver_count:,} (Descartados/Deduplicados: {initial_count - silver_count:,})")

    # 5. Escribir a Capa Silver particionada físicamente en disco por fecha
    print(f"[*] Guardando en Capa Silver particionada por 'event_date' en: {SILVER_PATH}...")
    df_silver.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(SILVER_PATH)

    print("[+] Capa Silver generada con éxito.")

if __name__ == "__main__":
    run_bronze_to_silver()
