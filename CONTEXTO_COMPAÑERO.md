# 📋 Contexto del Proyecto — Para Continuar el Trabajo

## 🎯 Qué es este proyecto

Proyecto de la asignatura **Bases de Datos Avanzadas** que implementa un pipeline ETL de Big Data usando **Medallion Architecture** (Bronze → Silver → Gold) con Apache Kafka, Apache Spark y PostgreSQL.

**Dataset:** `epinnock/polymarket-nba` de Hugging Face — datos de mercados predictivos NBA con **82,197,177 registros de precios** + tablas dimensionales (66K markets, 3.6K events).

**Objetivo:** Obtener calificación **Excelente (5.0)** en la rúbrica de 7 criterios.

---

## 📁 Estructura del Proyecto

```
nba-medallion-streaming/
├── docker-compose.yml          # Kafka + Kafka UI + PostgreSQL
├── data/
│   ├── download_hf_data.py     # Script para descargar dataset de HuggingFace
│   └── raw/                    # (gitignored) Archivos .parquet descargados
├── hadoop/bin/                 # winutils.exe + hadoop.dll para Windows
├── src/
│   ├── producer/
│   │   └── kafka_producer.py   # Lee Parquet → produce a Kafka topic "nba-ticks"
│   ├── streaming/
│   │   └── spark_bronze_stream.py  # Spark Structured Streaming: Kafka → Bronze (Parquet)
│   ├── batch/
│   │   ├── bronze_to_silver.py     # Spark Batch: dedup + filtrado + broadcast join con markets
│   │   └── silver_to_gold.py       # Spark SQL: agregaciones → PostgreSQL (fact_price_volatility)
│   └── dashboard/
│       └── serve_dashboard.py      # Dashboard HTML con Plotly, servido en puerto 8501
├── docs/
│   ├── OBJETIVOS_Y_METODOLOGIA.md  # Criterios 1 y 2 de la rúbrica
│   ├── ARQUITECTURA_TECNICA.md     # Criterios 3, 4, 6, 7
│   └── TRAZABILIDAD_IA.md          # Criterio 5 (trazabilidad de uso de IA)
├── tests/
│   └── test_pipeline.py
└── README.md
```

---

## 🐳 Infraestructura Docker

```yaml
# Contenedores (docker-compose up -d):
nba-kafka       → puerto 9092 (Apache Kafka)
nba-kafka-ui    → puerto 8085 (UI web para ver topics)
nba-postgres    → puerto 5433 (PostgreSQL 16, mapeado desde 5432 interno)
```

**Credenciales PostgreSQL:** user=`postgres`, password=`postgres`, db=`nba_gold`

---

## 🔧 Stack Técnico

| Componente | Versión | Notas |
|---|---|---|
| Python | 3.15.0 | `C:\Users\josem\AppData\Local\Python\bin\python.exe` |
| PySpark | 4.3.0 | Usa Scala **2.13** |
| Kafka Connector | `spark-sql-kafka-0-10_2.13:4.3.0` | ⚠️ DEBE ser 2.13, NO 2.12 |
| Docker | - | Kafka + PostgreSQL |
| Hadoop (Windows) | winutils 3.3.6 | En `hadoop/bin/` del proyecto |

---

## ✅ Qué está HECHO

1. **Descarga de datos** — `data/download_hf_data.py` descarga los 4 parquet de HuggingFace (~555 MB total)
2. **Kafka Producer** — Lee `prices-2024-25.parquet` y envía mensajes JSON al topic `nba-ticks`. Configurable con `STREAM_LIMIT` (por defecto 200,000 msgs)
3. **Capa Bronze** — Spark Structured Streaming consume de Kafka y escribe Parquet crudo en `data/bronze/ticks/`
4. **Capa Silver** — Spark batch: deduplicación por hash, filtrado de outliers, **broadcast join** con `markets.parquet` para enriquecer con datos del mercado
5. **Capa Gold** — Spark SQL: modelo dimensional Kimball → tabla `fact_price_volatility` persistida en PostgreSQL
6. **Dashboard** — HTML estático con Plotly servido por HTTPServer en puerto 8501
7. **Documentación** — 3 documentos en `docs/` cubriendo los 7 criterios de la rúbrica
8. **README completo** con diagramas Mermaid de arquitectura

---

## ❌ Qué FALTA / Problemas Pendientes

### 🔴 Crítico: Volumen de datos procesados
- El pipeline actual solo procesó ~50,000 registros por Kafka (de los 82M disponibles)
- **Para la nota máxima se necesitan mínimo 50M registros procesados**
- Hay que aumentar `STREAM_LIMIT` en el producer y/o procesar ambos archivos de precios (`prices-2024-25.parquet` + `prices-2025-26.parquet`)
- Alternativa: hacer ingesta directa por Spark (leer Parquet directo sin pasar por Kafka para el volumen completo)

### 🟡 Mejoras necesarias
- **Dashboard** muestra datos de una ejecución parcial — regenerar después de procesar más volumen
- **Tests** (`tests/test_pipeline.py`) pueden necesitar actualización para el schema real
- **Streamlit** no se pudo instalar (falta Visual C++ Build Tools) — el dashboard usa HTTP + Plotly como workaround
- **GitHub** — el repo aún no está conectado a un remoto

### 🟢 Nice to have
- Añadir más gráficas al dashboard (tendencias temporales, distribuciones)
- Implementar ventanas de tiempo (tumbling/sliding windows) en Spark Streaming para más puntos en el criterio 6
- Añadir más documentación de trazabilidad IA

---

## 🚀 Cómo Ejecutar (paso a paso)

```powershell
# 1. Levantar infraestructura
cd nba-medallion-streaming
docker-compose up -d

# 2. Descargar datos (si no están en data/raw/)
python data/download_hf_data.py

# 3. Producir mensajes a Kafka
$env:STREAM_LIMIT = "500000"   # o más
python src/producer/kafka_producer.py

# 4. Consumir con Spark Bronze
python src/streaming/spark_bronze_stream.py --once

# 5. Bronze → Silver
python src/batch/bronze_to_silver.py

# 6. Silver → Gold (escribe a PostgreSQL)
python src/batch/silver_to_gold.py

# 7. Lanzar dashboard
python src/dashboard/serve_dashboard.py
# Abrir http://localhost:8501
```

---

## ⚠️ Notas Importantes para Windows

1. **HADOOP_HOME**: Cada script de Spark configura `os.environ["HADOOP_HOME"]` automáticamente apuntando a `hadoop/bin/` del proyecto. No hace falta configurar nada global.
2. **Puerto PostgreSQL**: Es **5433** (no 5432) porque la máquina ya tenía un Postgres local.
3. **PySpark 4.3.0 = Scala 2.13**: Si descargas algún JAR adicional, debe ser versión `_2.13`.
4. **El .gitignore** excluye `data/raw/`, `data/bronze/`, `data/silver/`, `data/gold/` y `data/checkpoints/`. Los datos se regeneran con los scripts.

---

## 📊 Rúbrica (archivo en docs/)

| # | Criterio | Estado |
|---|---|---|
| 1 | Objetivos y preguntas clave | ✅ Documentado |
| 2 | Metodología Medallion | ✅ Implementado + documentado |
| 3 | Arquitectura y diagramas | ✅ Documentado con Mermaid |
| 4 | Justificación de herramientas | ✅ Documentado |
| 5 | Trazabilidad IA | ✅ 5 etapas documentadas |
| 6 | Uso avanzado de Spark | ⚠️ Funciona pero falta volumen |
| 7 | Uso avanzado de Kafka | ⚠️ Funciona pero falta volumen |

---

*Documento generado el 18/Nov/2026. Proyecto en: `C:\Users\josem\.gemini\antigravity\scratch\nba-medallion-streaming`*
