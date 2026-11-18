"""
Procesamiento Batch en Spark: Capa Bronze -> Capa Silver
Aplica deduplicación, filtrado de calidad física, tipado y Broadcast Join con dimensión markets.parquet.
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HADOOP_DIR = os.path.join(BASE_DIR, "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_unixtime, to_date, broadcast

BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "ticks")
SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "ticks")
MARKETS_DIM_PATH = os.path.join(BASE_DIR, "data", "raw", "markets.parquet")

def run_bronze_to_silver():
    print("=" * 60)
    print("PROCESAMIENTO SPARK: CAPA BRONZE -> CAPA SILVER (DATOS REALES NBA)")
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
    print("[*] Aplicando reglas de calidad de datos y deduplicación...")
    df_clean = df_bronze \
        .filter(col("condition_id").isNotNull()) \
        .filter((col("price") > 0.0) & (col("price") < 1.0)) \
        .dropDuplicates(["condition_id", "timestamp", "price"]) \
        .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp")))) \
        .withColumn("event_date", to_date(col("event_timestamp")))

    # 3. Cargar tabla dimensional de Mercados reales
    if os.path.exists(MARKETS_DIM_PATH):
        print(f"[*] Cargando dimensión de mercados desde: {MARKETS_DIM_PATH}")
        df_markets = spark.read.parquet(MARKETS_DIM_PATH).select(
            col("condition_id"),
            col("market_id"),
            col("event_title"),
            col("question").alias("market_question"),
            col("volume").alias("market_volume")
        ).dropDuplicates(["condition_id"])
    else:
        print("[!] No se encontró markets.parquet.")
        return

    # 4. OPTIMIZACIÓN SPARK: Broadcast Hash Join
    print("[*] Ejecutando Broadcast Hash Join (Optimización avanzada de Spark)...")
    df_silver = df_clean.join(broadcast(df_markets), on="condition_id", how="left")

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
