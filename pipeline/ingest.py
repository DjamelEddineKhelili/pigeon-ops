"""
INGESTION — the delivery driver. Goes to a source, brings back the raw data, drops it in the lake, signs the log.

Run one by hand:
    python -m pipeline.ingest velib_status
"""

import sys
import time
from datetime import datetime, timezone

import httpx

from pipeline.lake import bronze_path, log_ingestion, write_atomic
from pipeline.sources import SOURCES

HEADERS = {"User-Agent": "paris-data-dept/0.1 (student project)"}   # polite: say who you are to public APIs


def ingest(source: str) -> dict:
    """Download one source into the lake. NEVER raises: failures are logged, not crashed.

    Returns the log entry (a dict) so the API / n8n can see what happened.
    """
    src = SOURCES[source]
    fetched_at = datetime.now(timezone.utc)          # ALWAYS UTC in a data system. Paris time is for humans only.
    start = time.monotonic()                          # a stopwatch (not affected by clock changes)
    entry = {
        "source": source,
        "fetched_at": fetched_at.isoformat(),
        "status": "error",          # pessimistic default: only set to "ok" at the very end of the success path
        "path": None,
        "raw_bytes": 0,             # size of what the API sent
        "stored_bytes": 0,          # size after gzip (compare the two: that's your compression ratio)
        "seconds": 0.0,
        "error": None,
    }
    try:
        # TODO(you): the success path, 5 small steps.
        #  1. download:  r = httpx.get(src["url"], timeout=60, follow_redirects=True, headers=HEADERS)
        #     (follow_redirects matters: the fuel URL redirects to the real file)
        #  2. r.raise_for_status()  → turns HTTP 404/500/... into an exception (so it lands in `except`)
        #  3. an empty body is also a failure:  if not r.content: raise ValueError("empty response body")
        #  4. path = bronze_path(...) then write_atomic(path, r.content)  (it returns the stored size)
        #  5. fill entry: raw_bytes, stored_bytes, path (as str), and status = "ok"
        raise NotImplementedError("ingest success path")
    except NotImplementedError:
        raise
    except Exception as e:
        entry["error"] = f"{type(e).__name__}: {e}"
    entry["seconds"] = round(time.monotonic() - start, 2)
    log_ingestion(entry)
    return entry


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCES:
        sys.exit(f"usage: python -m pipeline.ingest <source>   sources: {', '.join(SOURCES)}")
    print(ingest(sys.argv[1]))
