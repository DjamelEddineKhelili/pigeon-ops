"""
LOOK BEFORE YOU CLEAN. Prints the shape of the latest raw file of a source.

    python -m pipeline.explore velib_status
    python -m pipeline.explore fuel

Every data engineer's first move on a new source: open it and stare at it. You will spot weird
things (duplicated fields, numbers stored as text, coordinates ×100000...) that no documentation mentions.
"""

import gzip
import json
import sys

import duckdb

from pipeline.lake import LAKE
from pipeline.sources import SOURCES


def shape(value, indent=0, key="(root)"):
    """Print a JSON tree: dicts show their keys, lists show their length and their FIRST element only."""
    pad = "  " * indent
    if isinstance(value, dict):
        print(f"{pad}{key}: object with {len(value)} keys")
        for k, v in value.items():
            shape(v, indent + 1, k)
    elif isinstance(value, list):
        print(f"{pad}{key}: list of {len(value)}" + (", first element:" if value else ""))
        if value:
            shape(value[0], indent + 1, "[0]")
    else:
        print(f"{pad}{key} = {value!r}")


def main(source):
    ext = SOURCES[source]["ext"]
    found = sorted((LAKE / "bronze" / f"source={source}").rglob(f"*.{ext}.gz"))
    if not found:
        sys.exit(f"no file for '{source}' yet. Run: python -m pipeline.ingest {source}")
    path = found[-1]
    print(f"{path}  ({path.stat().st_size / 1024:.0f} KB compressed, {len(found)} files in total)\n")
    if ext == "json":
        shape(json.load(gzip.open(path)))
    else:
        rel = duckdb.sql(f"SELECT * FROM read_csv('{path.as_posix()}', all_varchar = true)")
        print(f"{rel.shape[0]} rows × {rel.shape[1]} columns. First row, one column per line:\n")
        for col, val in zip(rel.columns, rel.limit(1).fetchone()):
            print(f"  {col!r:45} = {(val or '')[:70]!r}")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCES:
        sys.exit(f"usage: python -m pipeline.explore <source>   sources: {', '.join(SOURCES)}")
    main(sys.argv[1])
