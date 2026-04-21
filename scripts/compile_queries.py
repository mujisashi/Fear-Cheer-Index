"""Compile keyword buckets + composition registry into Starscape query JSON.

Keywords live one-per-line in queries/keywords/*.txt. Structure lives in
queries/composition.json. This script expands the composition DSL and writes
queries/<name>.json (2-space indent, trailing newline) to match the hand-edited
files byte-for-byte.

Usage:
    python3 scripts/compile_queries.py           # regenerate all
    python3 scripts/compile_queries.py --check   # fail if any file would change

Run --check from CI or a pre-commit hook to catch drift between the registry
and the materialized JSON.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUERIES_DIR = ROOT / "queries"
KEYWORDS_DIR = QUERIES_DIR / "keywords"
COMPOSITION_FILE = QUERIES_DIR / "composition.json"


def load_bucket(name):
    path = KEYWORDS_DIR / f"{name}.txt"
    with open(path) as f:
        terms = [line.rstrip("\n") for line in f if line.strip()]
    return terms


def expand(node, buckets):
    if "bucket" in node:
        return {
            "op": "contains",
            "fields": ["body", "title"],
            "values": buckets[node["bucket"]],
        }
    if "themes" in node:
        return {"op": "contains", "field": "themes", "values": node["themes"]}
    if "taxonomies" in node:
        return {"op": "contains", "field": "taxonomies", "values": node["taxonomies"]}
    if node.get("op") in ("and", "or"):
        return {
            "op": node["op"],
            "values": [expand(v, buckets) for v in node["values"]],
        }
    raise ValueError(f"Unrecognized composition node: {node}")


def serialize(obj):
    return json.dumps(obj, indent=2) + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="Fail if output would differ from on-disk files")
    args = ap.parse_args()

    with open(COMPOSITION_FILE) as f:
        composition = json.load(f)

    referenced = set()

    def collect(node):
        if "bucket" in node:
            referenced.add(node["bucket"])
        for child in node.get("values", []):
            if isinstance(child, dict):
                collect(child)

    for q in composition["queries"].values():
        collect(q)
    buckets = {name: load_bucket(name) for name in referenced}

    drift = []
    for name, spec in composition["queries"].items():
        out_path = QUERIES_DIR / f"{name}.json"
        rendered = serialize(expand(spec, buckets))
        if args.check:
            current = out_path.read_text() if out_path.exists() else ""
            if current != rendered:
                drift.append(name)
        else:
            out_path.write_text(rendered)
            print(f"wrote {out_path.relative_to(ROOT)}")

    if args.check:
        if drift:
            print(f"drift detected in: {', '.join(drift)}", file=sys.stderr)
            print("run `python3 scripts/compile_queries.py` to regenerate", file=sys.stderr)
            sys.exit(1)
        print("ok: all query files match composition + keywords")


if __name__ == "__main__":
    main()
