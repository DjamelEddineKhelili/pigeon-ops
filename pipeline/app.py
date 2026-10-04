"""
THE DATA ENGINEERING TEAM'S API — the "service window" other teams (and n8n) knock on.

n8n doesn't run our Python. It sends HTTP requests to this API, like a manager sending orders:
    POST /ingest/velib_status   → "go fetch Vélib' now"
    POST /build                 → "rebuild the warehouse"
    GET  /stats                 → "how are we doing?"

Why an API instead of scripts? Because a service can be called by anyone (n8n, another team, an agent),
restarted, monitored, and later... duplicated behind a load balancer (bonus level 😉).

Run locally:   uvicorn pipeline.app:app --reload        then open http://localhost:8000/docs
"""

import threading
from datetime import datetime, timezone

import duckdb
from fastapi import FastAPI, HTTPException

from pipeline.ingest import ingest
from pipeline.sources import SOURCES
from pipeline.warehouse import WAREHOUSE, build

app = FastAPI(title="Data Engineering team", version="level-1")

# DuckDB allows only ONE writer at a time. If n8n fires /build while a build is running,
# the second one must be refused politely instead of crashing into the first.
build_lock = threading.Lock()


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/ingest/{source}")
def ingest_source(source: str):
    """
    TODO(you): 3 rules, in this order.
      1. unknown source → raise HTTPException(404, "some helpful message listing the known SOURCES")
      2. run ingest(source) and keep the result
      3. if result["status"] != "ok" → raise HTTPException(502, result)   (502 = "the upstream server failed")
         else return result
    Why raise on failure? So n8n SEES the failure (red execution, retry, error workflow).
    A 200 response with {"status": "error"} inside would look like a success to n8n.
    """
    raise NotImplementedError("ingest endpoint")


@app.post("/build")
def build_warehouse():
    if not build_lock.acquire(blocking=False):          # don't wait: if busy, say so immediately
        raise HTTPException(409, "a build is already running")
    try:
        report = build()
    finally:
        build_lock.release()                             # ALWAYS release, even if build() crashed
    if any(t["status"] != "ok" for t in report.values()):
        raise HTTPException(500, report)
    return report


@app.get("/stats")
def stats():
    """Row count of every silver table + how many minutes ago each source last succeeded."""
    if not WAREHOUSE.exists():
        raise HTTPException(404, "warehouse not built yet: call POST /build first")
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'silver'").fetchall()]
        rows = {t: con.execute(f"SELECT count(*) FROM silver.{t}").fetchone()[0] for t in tables}
        freshness = {}
        if "ingestion_log" in tables:
            for source, last_ok, minutes in con.execute("""
                SELECT source, max(fetched_at)::VARCHAR,
                       round(date_diff('second', max(fetched_at), now() AT TIME ZONE 'UTC') / 60, 1)
                FROM silver.ingestion_log WHERE status = 'ok' GROUP BY source
            """).fetchall():
                freshness[source] = {"last_ok_utc": last_ok, "minutes_ago": minutes}
    return {"rows": rows, "freshness": freshness}
