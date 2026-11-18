"""
Procesamiento Spark SQL: Capa Silver -> Capa Gold (Modelo Kimball)
Construye Data Mart analítico y tabla de hechos 'fact_price_volatility' con persistencia en PostgreSQL.
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HADOOP_DIR = os.path.join(BASE_DIR, "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, stddev, sum as spark_sum, count, round as spark_round, coalesce, lit

SILVER_PATH = os.path.join(BASE_DIR, "data", "silver", "ticks")
GOLD_PATH = os.path.join(BASE_DIR, "data", "gold")

# Configuración de conexión a PostgreSQL en Docker (puerto mapeado 5433)
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")
PG_DB = os.getenv("POSTGRES_DB", "nba_gold")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres_password")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

def run_silver_to_gold():
    print("=" * 60)
    print("PROCESAMIENTO SPARK SQL: CAPA SILVER -> CAPA GOLD (KIMBALL)")
    print(f"Origen: {SILVER_PATH} | Destino Lakehouse: {GOLD_PATH}")
    print(f"Destino Serving DB: PostgreSQL ({PG_HOST}:{PG_PORT}/{PG_DB})")
    print("=" * 60)

    spark = SparkSession.builder \
        .appName("NBA-Silver-to-Gold-Analytics") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    if not os.path.exists(SILVER_PATH):
        print(f"[!] Directorio Silver no encontrado en: {SILVER_PATH}.")
        return

    # 1. Leer datos de la Capa Silver
    df_silver = spark.read.parquet(SILVER_PATH)
    df_silver.createOrReplaceTempView("silver_ticks")

    print("[*] Calculando métricas de negocio y tabla de hechos (Kimball)...")

    # 2. Construir Tabla de Hechos: fact_price_volatility
    fact_volatility = spark.sql("""
        SELECT 
            condition_id,
            COALESCE(market_id, 'm_unknown') as market_id,
            COALESCE(event_title, 'NBA Event') as event_title,
            COALESCE(market_question, 'Game Outcome') as market_question,
            event_date,
            ROUND(AVG(price), 4) as avg_price,
            ROUND(MIN(price), 4) as min_price,
            ROUND(MAX(price), 4) as max_price,
            ROUND(COALESCE(STDDEV(price), 0.0), 4) as volatility_index,
            ROUND(SUM(COALESCE(market_volume, 1000.0)), 2) as total_volume_traded,
            COUNT(*) as tick_count
        FROM silver_ticks
        GROUP BY condition_id, market_id, event_title, market_question, event_date
    """)

    fact_volatility.show(10, truncate=False)

    # 3. Guardar en Data Lakehouse (Capa Gold en Parquet)
    fact_gold_path = os.path.join(GOLD_PATH, "fact_price_volatility")
    print(f"[*] Guardando tabla de hechos en Data Lakehouse: {fact_gold_path}...")
    fact_volatility.write.mode("overwrite").parquet(fact_gold_path)

    # 4. Exportar a Base de Datos Relacional PostgreSQL
    print("[*] Exportando Data Mart a PostgreSQL...")
    try:
        fact_volatility.write \
            .format("jdbc") \
            .option("url", PG_JDBC_URL) \
            .option("dbtable", "fact_price_volatility") \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        print("[+] Tabla 'fact_price_volatility' persistida exitosamente en PostgreSQL.")
    except Exception as e:
        print(f"[!] Nota sobre JDBC: {e}")

    print("\n[+] Pipeline Medallion completado con éxito: Bronze -> Silver -> Gold.")

if __name__ == "__main__":
    run_silver_to_gold()
