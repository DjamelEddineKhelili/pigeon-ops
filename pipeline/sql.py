"""
Ask the warehouse a question (read-only).

    python -m pipeline.sql "SELECT count(*) FROM silver.station_status"

Once everything runs in Docker, ask from INSIDE the container (the container owns the database file):
    docker compose exec pipeline python -m pipeline.sql "SELECT ..."
"""

import sys

import duckdb

from pipeline.warehouse import WAREHOUSE

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit('usage: python -m pipeline.sql "SELECT ..."')
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        con.sql(sys.argv[1]).show(max_rows=50, max_width=200)
