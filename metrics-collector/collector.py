"""Coleta métricas **apenas do job mais recente** e grava em tabelas separadas."""
import os, time, json, requests, psycopg2
from datetime import datetime, timezone

MASTER_API  = "http://spark-master:8080/api/v1"
HISTORY_API = "http://spark-history:18080/api/v1"
INTERVAL    = int(os.getenv("POLL_INTERVAL", 10))

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "postgres"),
    user=os.getenv("PGUSER", "spark"),
    password=os.getenv("PGPASS", "spark"),
    dbname=os.getenv("PGDB", "spark_metrics"),
)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS spark_jobs (
  collected_at timestamptz,
  app_id       text,
  job_id       int,
  payload      jsonb
);
CREATE TABLE IF NOT EXISTS spark_stages (
  collected_at timestamptz,
  app_id       text,
  stage_id     int,
  payload      jsonb
);
CREATE TABLE IF NOT EXISTS spark_executors (
  collected_at timestamptz,
  app_id       text,
  executor_id  text,
  payload      jsonb
);
""")
conn.commit()


def fetch(url: str):
    try:
        return requests.get(url, timeout=3).json()
    except Exception:
        return []

def latest_app(api_base: str):
    """Retorna o app mais recente (running ou concluído)"""
    apps = fetch(f"{api_base}/applications?limit=1")
    return apps[0]["id"] if apps else None

def insert(ts, app_id, table, key, data):
    cur.execute(
        f"INSERT INTO {table} VALUES (%s,%s,%s,%s)",
        (ts, app_id, key, json.dumps(data)),
    )

while True:
    ts = datetime.now(timezone.utc)

    # 1) Verifica app mais recente no history‑server; se não houver, no master
    app_id = latest_app(HISTORY_API) or latest_app(MASTER_API)
    if not app_id:
        time.sleep(INTERVAL)
        continue

    # --- JOB‑LEVEL ---
    for job in fetch(f"{HISTORY_API}/applications/{app_id}/jobs") or \
               fetch(f"{MASTER_API}/applications/{app_id}/jobs"):
        insert(ts, app_id, "spark_jobs", job.get("jobId"), job)

    # --- STAGE‑LEVEL ---
    for stage in fetch(f"{HISTORY_API}/applications/{app_id}/stages") or \
                 fetch(f"{MASTER_API}/applications/{app_id}/stages"):
        insert(ts, app_id, "spark_stages", stage.get("stageId"), stage)

    # --- EXECUTOR‑LEVEL ---
    for exe in fetch(f"{HISTORY_API}/applications/{app_id}/executors") or \
               fetch(f"{MASTER_API}/applications/{app_id}/executors"):
        insert(ts, app_id, "spark_executors", exe.get("id"), exe)

    conn.commit()
    time.sleep(INTERVAL)