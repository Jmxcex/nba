"""
Spark Structured Streaming: Consumo de Kafka y Persistencia en Capa Bronze
Guarda los eventos crudos con metadatos técnicos en formato Parquet con Checkpointing.
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
HADOOP_DIR = os.path.join(BASE_DIR, "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze", "ticks")
CHECKPOINT_PATH = os.path.join(BASE_DIR, "data", "checkpoints", "bronze_ticks")

KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "nba.ticks.raw")

def run_bronze_stream():
    print("=" * 60)
    print("INICIANDO SPARK STRUCTURED STREAMING -> CAPA BRONZE")
    print(f"Broker: {KAFKA_BROKER} | Topic: {KAFKA_TOPIC}")
    print(f"Destino Bronze: {BRONZE_PATH}")
    print("=" * 60)

    # 1. Iniciar sesión de Spark con conector Kafka
    spark = SparkSession.builder \
        .appName("NBA-Bronze-Ingestion-Stream") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Definir esquema estricto de los eventos de Kafka (epinnock/polymarket-nba)
    schema = StructType([
        StructField("condition_id", StringType(), True),
        StructField("token_id", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("price", DoubleType(), True),
        StructField("source", StringType(), True)
    ])

    # 3. Leer stream desde Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 4. Parsear JSON y agregar metadatos técnicos (Auditoría)
    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("partition").alias("_kafka_partition"),
        col("offset").alias("_kafka_offset"),
        col("timestamp").alias("_kafka_msg_timestamp")
    ).select(
        "data.*",
        "_kafka_partition",
        "_kafka_offset",
        "_kafka_msg_timestamp",
        current_timestamp().alias("_ingestion_timestamp")
    )

    # 5. Escribir a la Capa Bronze (Parquet + Checkpointing)
    trigger_mode = {"availableNow": True} if "--once" in sys.argv else {"processingTime": "5 seconds"}
    print(f"[*] Iniciando escritura en Capa Bronze (Trigger: {trigger_mode})...")
    query = parsed_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", BRONZE_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(**trigger_mode) \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\n[!] Stream detenido por el usuario.")
        query.stop()

if __name__ == "__main__":
    run_bronze_stream()

