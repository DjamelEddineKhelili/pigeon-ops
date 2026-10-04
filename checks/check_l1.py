"""
LEVEL 1 checks. Two modes:

    python -m checks.check_l1          offline: tests YOUR code on a fake mini-lake (no internet, no Docker)
    python -m checks.check_l1 --live   live: asks the running pipeline (Docker) if the real data is flowing

Every ✅ is one thing that works. A ❌ tells you what to look at.
"""

import gzip
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

# The offline tests run on a throw-away lake, so they never touch your real data.
# These env vars must be set BEFORE importing pipeline.* (the paths are read at import time).
TMP = Path(tempfile.mkdtemp(prefix="l1check_"))
if "--live" not in sys.argv:
    os.environ["LAKE_DIR"] = str(TMP / "lake")
    os.environ["WAREHOUSE_PATH"] = str(TMP / "warehouse" / "test.duckdb")

passed, failed = 0, 0


def check(name, fn):
    global passed, failed
    try:
        fn()
        print(f"✅ {name}")
        passed += 1
    except NotImplementedError as e:
        print(f"⬜ {name}  (not written yet: {e})")
        failed += 1
    except Exception as e:
        print(f"❌ {name}\n     {type(e).__name__}: {e}")
        failed += 1


# ─────────────────────────────── offline: the lake ───────────────────────────────

def t_bronze_path():
    from pipeline.lake import LAKE, bronze_path
    p = bronze_path("velib_status", datetime(2026, 10, 3, 7, 5, 9, tzinfo=timezone.utc), "json")
    expected = (LAKE / "bronze" / "source=velib_status" / "date=2026-10-03" / "hour=07"
                / "velib_status_20261003T070509Z.json.gz")
    assert Path(p) == expected, f"got {p}\n     expected {expected}"


def t_write_atomic():
    from pipeline.lake import LAKE, write_atomic
    target = LAKE / "test" / "deep" / "folder" / "x.json.gz"
    size = write_atomic(target, b'{"hello": "lake"}')
    assert target.exists(), "file was not created (did you create the parent folders?)"
    assert gzip.open(target).read() == b'{"hello": "lake"}', "content is not the gzip of the input bytes"
    assert size == target.stat().st_size, "must return the size of the stored (compressed) file"
    leftovers = list(target.parent.glob("*.tmp"))
    assert not leftovers, f"temporary file left behind: {leftovers}"


def t_log():
    from pipeline.lake import LOG_FILE, log_ingestion
    log_ingestion({"source": "a", "status": "ok"})
    log_ingestion({"source": "b", "status": "error"})
    lines = LOG_FILE.read_text(encoding="utf-8").strip().splitlines()
    assert [json.loads(l)["source"] for l in lines[-2:]] == ["a", "b"], "the log must APPEND one JSON object per line"


# ─────────────────────────────── offline: ingestion + API (with a fake internet) ───────────────────────────────

class FakeResponse:
    def __init__(self, status, content):
        self.status_code, self.content = status, content

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


def fake_internet(status, content):
    import pipeline.ingest
    pipeline.ingest.httpx.get = lambda *args, **kwargs: FakeResponse(status, content)


def t_ingest_ok():
    from pipeline.ingest import ingest
    fake_internet(200, b'{"data": {"stations": []}}')
    e = ingest("velib_status")
    assert e["status"] == "ok", f"expected status ok, got {e}"
    assert e["raw_bytes"] == 26 and e["stored_bytes"] > 0, f"sizes not filled: {e}"
    assert e["path"] and Path(e["path"]).exists(), f"the bronze file does not exist: {e['path']}"


def t_ingest_fail():
    from pipeline.ingest import ingest
    fake_internet(503, b"down")
    e = ingest("velib_status")
    assert e["status"] == "error" and "503" in (e["error"] or ""), f"a 503 must be logged as an error: {e}"
    fake_internet(200, b"")
    e = ingest("velib_status")
    assert e["status"] == "error", f"an empty body must be an error: {e}"


def t_api():
    from fastapi.testclient import TestClient
    from pipeline.app import app
    client = TestClient(app)
    assert client.get("/health").status_code == 200
    code = client.post("/ingest/not_a_source").status_code
    assert code == 404, f"unknown source must give 404, got {code}"
    fake_internet(503, b"down")
    code = client.post("/ingest/weather").status_code
    assert code == 502, f"a failed ingestion must give 502 (so n8n sees it), got {code}"
    fake_internet(200, b'{"ok": true}')
    code = client.post("/ingest/weather").status_code
    assert code == 200, f"a successful ingestion must give 200, got {code}"


def reset_fake_lake():
    from pipeline.lake import LAKE
    shutil.rmtree(LAKE, ignore_errors=True)


# ─────────────────────────────── offline: the warehouse ───────────────────────────────

def station(sid, code, bikes, mech, ebike, docks, reported):
    return {"station_id": sid, "num_bikes_available": bikes, "numBikesAvailable": bikes,
            "num_bikes_available_types": [{"mechanical": mech}, {"ebike": ebike}],
            "num_docks_available": docks, "numDocksAvailable": docks, "is_installed": 1,
            "is_returning": 1, "is_renting": 1, "last_reported": reported, "stationCode": code,
            "station_opening_hours": None}


FUEL_HEADER = ("id;latitude;longitude;Code postal;Adresse;Ville;Département;Automate 24-24 (oui/non);"
               "Prix Gazole mis à jour le;Prix Gazole;Prix SP95 mis à jour le;Prix SP95;"
               "Prix E85 mis à jour le;Prix E85;Prix GPLc mis à jour le;Prix GPLc;"
               "Prix E10 mis à jour le;Prix E10;Prix SP98 mis à jour le;Prix SP98")
FUEL_ROWS = [
    "75011001;4885900;237800;75011;1 rue de la Roquette;Paris;Paris;Oui;"
    "2026-10-03T08:00:00+00:00;2.199;;;;;;;2026-10-03T08:00:00+00:00;1.999;;",
    "89100001;4818300;330900;89100;84 route de Maillot;Sens;Yonne;Non;"
    "2026-09-28T11:45:27+00:00;2.429;;;2026-08-25T10:02:31+00:00;0.859;;;;;2026-09-28T11:42:00+00:00;2.289",
]


def put(source, when, ext, data: bytes):
    """Writes a bronze file WITHOUT using your lake.py, so the warehouse tests don't depend on it."""
    from pipeline.lake import LAKE
    path = (LAKE / "bronze" / f"source={source}" / f"date={when:%Y-%m-%d}" / f"hour={when:%H}"
            / f"{source}_{when:%Y%m%dT%H%M%SZ}.{ext}.gz")
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as f:
        f.write(data)
    return path


def make_fake_lake():
    """3 Vélib snapshots, 1 station list, 2 weather calls for the SAME hour, the same fuel file twice."""
    from pipeline.lake import LAKE, LOG_FILE
    t = lambda m: datetime(2026, 10, 3, 7, m, tzinfo=timezone.utc)
    snapshots = [
        # station 1 does NOT report again between snapshot 1 and 2 → same reading twice in the lake
        [station(1, "16107", 3, 2, 1, 30, 1790900000), station(2, "40001", 0, 0, 0, 25, 1790900010)],
        [station(1, "16107", 3, 2, 1, 30, 1790900000), station(2, "40001", 1, 1, 0, 24, 1790900310)],
        [station(1, "16107", 5, 3, 2, 28, 1790900600), station(2, "40001", 1, 1, 0, 24, 1790900310)],
    ]
    LAKE.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "w", encoding="utf-8") as log:
        for i, stations in enumerate(snapshots):
            body = {"lastUpdatedOther": 1790900000, "ttl": 3600, "data": {"stations": stations}}
            p = put("velib_status", t(5 * i), "json", json.dumps(body).encode())
            log.write(json.dumps({"source": "velib_status", "fetched_at": t(5 * i).isoformat(), "status": "ok",
                                  "path": str(p), "raw_bytes": 1, "stored_bytes": 1, "seconds": 0.1,
                                  "error": None}) + "\n")
    info = {"lastUpdatedOther": 1, "ttl": 3600, "data": {"stations": [
        {"station_id": 1, "stationCode": "16107", "name": "Benjamin Godard", "lat": 48.86, "lon": 2.27,
         "capacity": 35, "rental_methods": ["CREDITCARD"]},
        {"station_id": 2, "stationCode": "40001", "name": "Hôpital Mondor", "lat": 48.79, "lon": 2.45,
         "capacity": 28}]}}
    put("velib_info", t(0), "json", json.dumps(info).encode())
    for m in (0, 30):
        w = {"latitude": 48.86, "longitude": 2.35, "current": {"time": "2026-10-03T07:00", "interval": 900,
             "temperature_2m": 14.2, "precipitation": 0.4, "rain": 0.4, "wind_speed_10m": 9.1, "weather_code": 61}}
        put("weather", t(m), "json", json.dumps(w).encode())
    csv = "\n".join([FUEL_HEADER, *FUEL_ROWS]) + "\n"
    for m in (0, 40):
        put("fuel", t(m), "csv", csv.encode("utf-8"))


def built():
    from pipeline.warehouse import build
    if not getattr(built, "report", None):
        make_fake_lake()
        built.report = build()
    return built.report


def ready(table):
    """Fails cleanly if the build of `table` failed: ⬜ if it's still a gap, ❌ with the real error otherwise."""
    result = built()[table]
    if result["status"] != "ok":
        if "NotImplementedError" in result["error"]:
            raise NotImplementedError(f"build_{table}")
        raise AssertionError(result["error"])


def query(sql):
    import duckdb
    from pipeline.warehouse import WAREHOUSE
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        return con.execute(sql).fetchall()


def t_build_ok():
    errors = {k: v["error"] for k, v in built().items() if v["status"] != "ok"}
    if errors and all("NotImplementedError" in e for e in errors.values()):
        raise NotImplementedError(", ".join(errors))
    assert not errors, json.dumps(errors, indent=2)


def t_status_dedup():
    ready("station_status")
    n = query("SELECT count(*) FROM silver.station_status")[0][0]
    assert n == 4, f"6 raw rows contain 4 DIFFERENT (station_id, reported_at) pairs; you kept {n}"


def t_status_types():
    ready("station_status")
    rows = query("SELECT mechanical, ebike, bikes FROM silver.station_status "
                 "WHERE station_id = 1 ORDER BY reported_at DESC LIMIT 1")
    assert rows == [(3, 2, 5)], f"expected (mechanical=3, ebike=2, bikes=5), got {rows}"


def t_weather_dedup():
    ready("weather_hourly")
    n = query("SELECT count(*) FROM silver.weather_hourly")[0][0]
    assert n == 1, f"2 calls in the same hour describe ONE observation; you kept {n} rows"


def t_fuel_long():
    ready("fuel_prices")
    rows = query("SELECT station_id, fuel, price FROM silver.fuel_prices ORDER BY station_id, fuel")
    expected = [("75011001", "e10", 1.999), ("75011001", "gazole", 2.199),
                ("89100001", "e85", 0.859), ("89100001", "gazole", 2.429), ("89100001", "sp98", 2.289)]
    assert rows == expected, f"\n     got      {rows}\n     expected {expected}"


def t_fuel_coords():
    ready("fuel_stations")
    rows = query("SELECT station_id, lat, lon FROM silver.fuel_stations ORDER BY station_id")
    assert rows == [("75011001", 48.859, 2.378), ("89100001", 48.183, 3.309)], (
        f"got {rows}. Hint: the raw latitude 4885900 is not a latitude yet…")


def t_idempotent():
    from pipeline.warehouse import build
    ready("station_status")
    ready("fuel_prices")
    before = query("SELECT (SELECT count(*) FROM silver.station_status), (SELECT count(*) FROM silver.fuel_prices)")
    build()
    after = query("SELECT (SELECT count(*) FROM silver.station_status), (SELECT count(*) FROM silver.fuel_prices)")
    assert before == after, f"rebuilding changed the counts {before} → {after}: the build is not idempotent"


# ─────────────────────────────── live: the running system ───────────────────────────────

API = os.environ.get("PIPELINE_URL", "http://localhost:8000")


def t_live_health():
    import httpx
    assert httpx.get(f"{API}/health", timeout=5).json()["status"] == "ok"


def t_live_fresh():
    import httpx
    fresh = httpx.get(f"{API}/stats", timeout=30).json()["freshness"]
    limits = {"velib_status": 15, "weather": 90, "fuel": 200, "velib_info": 1500}
    for source, limit in limits.items():
        assert source in fresh, f"no successful ingestion of '{source}' yet"
        age = fresh[source]["minutes_ago"]
        assert age <= limit, f"'{source}' last succeeded {age} min ago (limit {limit}): is its n8n workflow active?"


def t_live_volume():
    import httpx
    rows = httpx.get(f"{API}/stats", timeout=30).json()["rows"]
    assert rows.get("station_status", 0) > 1000, f"station_status has {rows.get('station_status')} rows"
    assert rows.get("fuel_prices", 0) > 10000, f"fuel_prices has {rows.get('fuel_prices')} rows"


if __name__ == "__main__":
    if "--live" in sys.argv:
        print(f"LIVE checks against {API}\n")
        check("the pipeline API answers", t_live_health)
        check("every source is fresh", t_live_fresh)
        check("the warehouse holds real volumes", t_live_volume)
    else:
        print("OFFLINE checks on a fake mini-lake\n")
        check("bronze_path builds the partitioned path", t_bronze_path)
        check("write_atomic stores gzip, safely", t_write_atomic)
        check("log_ingestion appends JSON lines", t_log)
        check("ingest: success path", t_ingest_ok)
        check("ingest: failures are logged, never crash", t_ingest_fail)
        check("API: 404 / 502 / 200", t_api)
        reset_fake_lake()
        check("warehouse build runs without error", t_build_ok)
        check("station_status: duplicates removed", t_status_dedup)
        check("station_status: mechanical/ebike extracted", t_status_types)
        check("weather_hourly: one row per hour", t_weather_dedup)
        check("fuel_prices: wide → long", t_fuel_long)
        check("fuel_stations: real coordinates", t_fuel_coords)
        check("the build is idempotent", t_idempotent)
    shutil.rmtree(TMP, ignore_errors=True)
    print(f"\n{passed}/{passed + failed} passed")
    sys.exit(0 if failed == 0 else 1)
