"""
THE WAREHOUSE (silver layer) — the shop with labelled shelves.

The lake keeps raw boxes. The warehouse keeps clean, typed, de-duplicated TABLES that people can query.
We use DuckDB: a database that lives in ONE file (warehouse/warehouse.duckdb), needs no server,
and can read JSON / CSV / gzip files from the lake directly with SQL. Think "SQLite, but for analytics".

How a build works (Level 1 strategy: "full rebuild"):
    every table is re-created from ALL the raw files, every time.
    Simple, and IDEMPOTENT: running the build twice gives exactly the same tables.
    (Later levels: "incremental" builds that only read new files. Full rebuild is fine while the lake is small.)

Run by hand:
    python -m pipeline.warehouse

In the SQL below, every  ???  is a gap for you. The build refuses to run a query that still contains one.
"""

import json
import os
import time
from pathlib import Path

import duckdb

from pipeline.lake import LAKE, LOG_FILE

WAREHOUSE = Path(os.environ.get("WAREHOUSE_PATH", "warehouse/warehouse.duckdb"))

# our name → the label used in the French fuel CSV column names ("Prix Gazole", "Prix SP95", ...)
FUELS = {"gazole": "Gazole", "sp95": "SP95", "sp98": "SP98", "e10": "E10", "e85": "E85", "gplc": "GPLc"}

# The download time is written in every bronze file name (…_20261003T070509Z.json.gz). This SQL pulls it out.
FETCHED_AT = r"strptime(regexp_extract(filename, '(\d{8}T\d{6}Z)', 1), '%Y%m%dT%H%M%SZ')"


def files(source: str, ext: str) -> str:
    """A glob pattern matching EVERY bronze file of a source. ** = any sub-folders (all dates, all hours)."""
    return (LAKE / "bronze" / f"source={source}" / "**" / f"*.{ext}.gz").as_posix()


def latest_file(source: str, ext: str) -> str:
    """The most recent bronze file of a source (the partitioned names sort chronologically)."""
    found = sorted((LAKE / "bronze" / f"source={source}").rglob(f"*.{ext}.gz"))
    if not found:
        raise FileNotFoundError(f"no bronze file yet for '{source}': run its ingestion first")
    return found[-1].as_posix()


def run(con, sql: str):
    if "???" in sql:
        raise NotImplementedError("this query still has a ??? gap")
    con.execute(sql)


# ─────────────────────────────────────────────────────────────────────────────
# VÉLIB' STATUS — the big one. One raw file = one photo of ~1,500 stations.
# ─────────────────────────────────────────────────────────────────────────────

def build_station_status(con):
    """
    PROBLEM 1 — the raw shape. A file looks like:
        {"lastUpdatedOther": ..., "data": {"stations": [ {station}, {station}, ... ]}}
    `unnest(data.stations)` turns that ONE row holding a list into ONE ROW PER STATION.

    PROBLEM 2 — duplicates. We take a photo every 5 minutes, but a quiet station doesn't report every
    5 minutes: its `last_reported` stays the same, so the SAME reading appears in many photos.
    The true identity of a reading is (station_id, reported_at). Keep one row per pair:
    the first time we saw it (smallest fetched_at).

    PROBLEM 3 — a weird format. Bike types come as a LIST of one-key objects:
        "num_bikes_available_types": [{"mechanical": 2}, {"ebike": 1}]
    """
    run(con, f"""
        CREATE OR REPLACE TABLE silver.station_status AS
        WITH raw AS (
            SELECT {FETCHED_AT} AS fetched_at, unnest(data.stations) AS s
            FROM read_json('{files("velib_status", "json")}', filename = true, union_by_name = true)
        ),
        flat AS (
            SELECT
                s.station_id                                       AS station_id,
                s.stationCode                                      AS station_code,
                to_timestamp(s.last_reported) AT TIME ZONE 'UTC'   AS reported_at,
                s.num_bikes_available                              AS bikes,

                -- TODO(you) PROBLEM 3: extract the number of mechanical bikes and of e-bikes.
                -- DuckDB reads the list as  [{{mechanical: 2, ebike: NULL}}, {{mechanical: NULL, ebike: 1}}]
                -- Tool 1: list_transform(the_list, lambda x: x.mechanical)  → [2, NULL]
                -- Tool 2: list_sum([2, NULL])                                → 2   (NULLs are ignored)
                ???                                                AS mechanical,
                ???                                                AS ebike,

                s.num_docks_available                              AS docks,
                s.is_installed = 1                                 AS is_installed,
                s.is_renting = 1                                   AS is_renting,
                s.is_returning = 1                                 AS is_returning,
                fetched_at
            FROM raw
        )
        SELECT * EXCLUDE (fetched_at), fetched_at AS first_seen_at
        FROM flat
        -- TODO(you) PROBLEM 2: keep ONE row per (station_id, reported_at), the earliest fetched_at.
        -- Tool: QUALIFY filters on a window function, like WHERE does on a column:
        --   QUALIFY row_number() OVER (PARTITION BY <the identity columns> ORDER BY <which one wins>) = 1
        -- row_number() numbers the rows 1, 2, 3... inside each group; "= 1" keeps the winner.
        ???
    """)


def build_station_info(con):
    """Station names, GPS, capacity. Reference data: only the LATEST file matters."""
    run(con, f"""
        CREATE OR REPLACE TABLE silver.station_info AS
        SELECT s.station_id AS station_id, s.stationCode AS station_code, s.name AS name,
               s.lat AS lat, s.lon AS lon, s.capacity AS capacity
        FROM (SELECT unnest(data.stations) AS s FROM read_json('{latest_file("velib_info", "json")}'))
    """)


def build_weather(con):
    """
    Open-Meteo's "current" block is the latest observation, at the hour. We call it every hour,
    but if a call is retried (or you call it by hand), the SAME observation is stored twice.
    """
    run(con, f"""
        CREATE OR REPLACE TABLE silver.weather_hourly AS
        SELECT
            strptime(current.time, '%Y-%m-%dT%H:%M') AS observed_at,
            current.temperature_2m                   AS temperature_c,
            current.precipitation                    AS precipitation_mm,
            current.rain                             AS rain_mm,
            current.wind_speed_10m                   AS wind_kmh,
            current.weather_code                     AS weather_code
        FROM read_json('{files("weather", "json")}', union_by_name = true)
        -- TODO(you): one row per observed_at. Same tool as PROBLEM 2 above.
        ???
    """)


# ─────────────────────────────────────────────────────────────────────────────
# FUEL — 10k stations, French column names with spaces and accents, prices as text.
# ─────────────────────────────────────────────────────────────────────────────

def build_fuel_prices(con):
    """
    The CSV is WIDE: one row per station, one pair of columns per fuel:
        id | ... | Prix Gazole | Prix Gazole mis à jour le | Prix SP95 | Prix SP95 mis à jour le | ...
    Analysis wants it LONG: one row per (station, fuel):
        station_id | fuel   | price | updated_at
        89100001   | gazole | 2.429 | 2026-09-28 11:45:27
        89100001   | sp98   | 2.289 | 2026-09-28 11:42:00

    Plan: for EACH fuel, write one small SELECT, then glue them with UNION ALL.

    TODO(you): fill `parts` with one SQL string per fuel (loop over FUELS.items()). Each one:
        SELECT id AS station_id,
               '<fuel>' AS fuel,
               TRY_CAST("Prix <Label>" AS DOUBLE) AS price,
               TRY_CAST("Prix <Label> mis à jour le" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS updated_at
        FROM raw
        WHERE "Prix <Label>" IS NOT NULL
    Notes:
      - column names with spaces/accents need "double quotes" in SQL;
        inside a Python f-string, write them as f'..."Prix {label}"...' (single quotes outside)
      - TRY_CAST returns NULL instead of crashing on garbage text. Garbage WILL come.
      - the dates look like 2026-09-28T11:45:27+00:00 → TIMESTAMPTZ, then AT TIME ZONE 'UTC' = plain UTC
    """
    parts = []   # TODO(you)
    if not parts:
        raise NotImplementedError("fuel parts")
    run(con, f"""
        CREATE OR REPLACE TABLE silver.fuel_prices AS
        WITH raw AS (
            SELECT * FROM read_csv('{files("fuel", "csv")}', union_by_name = true, all_varchar = true)
        ),
        long AS ({" UNION ALL ".join(parts)})
        SELECT DISTINCT * FROM long   -- the same file is downloaded every 3 h: identical rows collapse here
    """)


def build_fuel_stations(con):
    """
    Station reference data from the latest fuel file.
    Dirty detail: run `python -m pipeline.explore fuel` and look at the `latitude` column. 4818300?
    Compare with the `geom` column of the same row. Fix it below.
    """
    run(con, f"""
        CREATE OR REPLACE TABLE silver.fuel_stations AS
        SELECT id                                   AS station_id,
               "Adresse"                            AS address,
               "Ville"                              AS city,
               "Code postal"                        AS postcode,
               ???                                  AS lat,     -- TODO(you): from latitude (it's TEXT: TRY_CAST it)
               ???                                  AS lon,     -- TODO(you): from longitude
               "Département"                        AS department,
               "Automate 24-24 (oui/non)" = 'Oui'   AS is_24h
        FROM read_csv('{latest_file("fuel", "csv")}', all_varchar = true)
    """)


def build_ingestion_log(con):
    """The ingestion log becomes a table too: the department can query its own health."""
    if not LOG_FILE.exists():
        raise FileNotFoundError("no ingestion log yet: run an ingestion first")
    run(con, f"""
        CREATE OR REPLACE TABLE silver.ingestion_log AS
        SELECT * FROM read_json('{LOG_FILE.as_posix()}', format = 'newline_delimited')
    """)


BUILDERS = {
    "station_status": build_station_status,
    "station_info": build_station_info,
    "weather_hourly": build_weather,
    "fuel_prices": build_fuel_prices,
    "fuel_stations": build_fuel_stations,
    "ingestion_log": build_ingestion_log,
}


def build() -> dict:
    """Rebuild every silver table. One failing table doesn't stop the others; the report says who failed."""
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    report = {}
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        for name, builder in BUILDERS.items():
            start = time.monotonic()
            try:
                builder(con)
                rows = con.execute(f"SELECT count(*) FROM silver.{name}").fetchone()[0]
                report[name] = {"status": "ok", "rows": rows, "seconds": round(time.monotonic() - start, 2)}
            except Exception as e:
                report[name] = {"status": "error", "error": f"{type(e).__name__}: {e}"}
    return report


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
