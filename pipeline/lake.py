"""
THE DATA LAKE (bronze layer) — the company's "garage".

Rule #1 of a lake: store what you received EXACTLY as you received it. Never clean here.
Why? Because if your cleaning code has a bug, you can fix it and rebuild everything from the raw files.
If you cleaned before storing, the original is gone forever.

Layout on disk (this naming style is called "Hive partitioning", every big data tool understands it):

    lake/
    ├── _ingestion_log.jsonl                     ← one line per download attempt (success or failure)
    └── bronze/
        └── source=velib_status/
            └── date=2026-10-03/
                └── hour=07/
                    └── velib_status_20261003T070509Z.json.gz

Analogy: the garage has one shelf per supplier, one box per day, one drawer per hour.
To find "Vélib' data from Oct 3rd, 7am", you walk straight to the drawer instead of opening every box.
"""

import gzip
import json
import os
from datetime import datetime
from pathlib import Path

LAKE = Path(os.environ.get("LAKE_DIR", "lake"))     # in Docker: /data/lake (see docker-compose.yml)
LOG_FILE = LAKE / "_ingestion_log.jsonl"


def bronze_path(source: str, fetched_at: datetime, ext: str) -> Path:
    """Where the raw file for `source`, downloaded at `fetched_at` (UTC), must be stored.

    Example: bronze_path("velib_status", 2026-10-03 07:05:09 UTC, "json")
        → LAKE / "bronze" / "source=velib_status" / "date=2026-10-03" / "hour=07"
               / "velib_status_20261003T070509Z.json.gz"

    TODO(you): build and return that Path.
    Hints:
      - Path objects can be joined with /  :  LAKE / "bronze" / "something"
      - f-strings can format dates directly :  f"{fetched_at:%Y-%m-%d}"  →  "2026-10-03"
      - the file stamp format is "%Y%m%dT%H%M%SZ"  (T separates date and time, Z means UTC)
      - the extension is ALWAYS followed by .gz (we compress everything)
    """
    raise NotImplementedError("bronze_path")


def write_atomic(path: Path, data: bytes) -> int:
    """Gzip `data` into `path`, ATOMICALLY. Return the size in bytes of the stored file.

    "Atomic" = the file appears complete or not at all, never half-written.
    Why? The warehouse build reads the lake every 15 minutes. If it reads a file while we're still
    writing it, it reads garbage. Trick: write to "x.json.gz.tmp" first, then RENAME it to "x.json.gz".
    A rename is instant: readers see the full file or nothing.

    TODO(you):
      1. create the parent folders              → path.parent.mkdir(parents=True, exist_ok=True)
      2. tmp = path with ".tmp" added at the end → path.with_name(path.name + ".tmp")
      3. write the bytes compressed             → with gzip.open(tmp, "wb") as f: ...
      4. rename tmp → path                      → os.replace(tmp, path)   (works on Windows AND Linux)
      5. return the stored size                 → path.stat().st_size
    """
    raise NotImplementedError("write_atomic")


def log_ingestion(entry: dict) -> None:
    """Append `entry` as ONE line of JSON at the end of LOG_FILE (the "JSON Lines" format).

    This log is the memory of the department: every attempt, its status, its size, its error.
    Level 2's Quality team will read it to answer "is a source sick?".

    TODO(you): make sure LAKE exists, open LOG_FILE in APPEND mode ("a", encoding="utf-8"),
               write json.dumps(entry) followed by "\\n".
    """
    raise NotImplementedError("log_ingestion")
