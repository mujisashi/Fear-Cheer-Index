"""Local-only data fetch.

Mirrors update_data.py's pipeline but skips the HubSpot upload and writes to
data.local.json instead of data.json. Use this to preview restructured queries
without touching the production dashboard payload.

Run from repo root:
    python3 scripts/fetch_data_local.py
"""
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_data import (
    QUERY_METRICS,
    calculate_growth_metrics,
    calculate_ratio_metric,
    fetch_infegy_data,
    process_query,
)

OUTPUT_FILE = "data.local.json"
OUTPUT_JS = "data.local.js"


def build_payload(metric_data):
    data = {"lastUpdated": "", "metrics": {}}
    for metric_name, (values, labels, _, net_sentiment) in metric_data.items():
        avg_net_sentiment = sum(net_sentiment) / len(net_sentiment) if net_sentiment else 0
        data["metrics"][metric_name] = {
            "current": values[-1],
            "trend": values,
            "labels": labels,
            "net_sentiment": round(avg_net_sentiment * 100, 1),
            "growth": calculate_growth_metrics(values),
        }
    data["lastUpdated"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return data


def print_summary(data):
    print(f"\nWrote {OUTPUT_FILE} ({len(data['metrics'])} metrics)")
    print(f"{'metric':<22} {'current':>10} {'trend_len':>10} {'net_sent':>10}")
    print("-" * 56)
    for name, m in data["metrics"].items():
        print(f"{name:<22} {m['current']:>10.3f} {len(m['trend']):>10} {m['net_sentiment']:>10.1f}")


def main():
    print("Fetching total volume...")
    total_volume_data = fetch_infegy_data("q_WoKuQA6Dr45")

    metric_data = {}
    raw_counts = {}
    for query_id, query_config in QUERY_METRICS.items():
        if query_id == "q_WoKuQA6Dr45":
            continue
        print(f"Fetching {query_config['metric_name']} ({query_id})...")
        values, labels, counts, net_sentiment = process_query(query_id, total_volume_data)
        if values and labels:
            metric_data[query_config["metric_name"]] = (values, labels, counts, net_sentiment)
            if counts is not None:
                raw_counts[query_config["metric_name"]] = counts

    if "hope" in raw_counts and "despair" in raw_counts:
        print("Calculating hope/despair ratio...")
        ratio_values, labels = calculate_ratio_metric(
            raw_counts["hope"], raw_counts["despair"], metric_data["hope"][1]
        )
        hope_ns = metric_data["hope"][3]
        despair_ns = metric_data["despair"][3]
        ratio_ns = [(h + d) / 2 for h, d in zip(hope_ns, despair_ns)]
        metric_data["hopeDespairRatio"] = (ratio_values, labels, None, ratio_ns)

    if not metric_data:
        print("No data fetched.")
        return

    data = build_payload(metric_data)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(data, f, indent=4)
    with open(OUTPUT_JS, "w") as f:
        f.write("window.FCI_DATA = ")
        json.dump(data, f)
        f.write(";\n")
    print_summary(data)


if __name__ == "__main__":
    main()
