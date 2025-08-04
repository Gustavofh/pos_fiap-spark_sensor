"""Job Spark de carga variável – gera métricas reais e garante execução > 10 s"""
import os, random, time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt, rand

MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

rows = random.randint(10_000_000_000, 20_000_000_000)
operation = random.choice(["sum", "sqrt"])

spark = (SparkSession.builder
         .master(MASTER)
         .appName(f"Dummy-{operation}-{rows}")
         .getOrCreate())

# 1) Cria DataFrame grande
start = time.time()
df = (
    spark.range(rows)
         .withColumn("value", col("id") * (rand() * 1_000_000 + 0.001))
)

# 2) Ação pesada
if operation == "sum":
    df.agg({"value": "sum"}).collect()
else:
    df.select(sqrt(col("value"))).agg({"sqrt(value)": "avg"}).collect()

# 3) Garante duração mínima (>10 s) para dar tempo ao collector
elapsed = time.time() - start
if elapsed < 10:
    time.sleep(10 - elapsed)

spark.stop()