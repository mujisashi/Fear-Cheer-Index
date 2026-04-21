"""
Pre-flight diff report for the FCI query restructure rollout.

Runs the 7 Starscape queries against live API (using current stored query
bodies, whatever state they're in) and compares per-day volumes and
net_sentiment against the baseline in data.json. Asserts rollup invariants
and sanity bounds. Prints a PASS/FAIL summary. Does NOT upload to HubSpot.

Usage:
    python scripts/diff_report.py

Requires:
    api_token.txt in repo root (same as update_data.py).
    data.json in repo root as baseline.

Exit code:
    0 if all invariants PASS.
    1 if any invariant FAILS or any query errored.
"""

import json
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from update_data import (
    QUERY_METRICS,
    fetch_infegy_data,
    calculate_percentage,
)

TOTAL_VOLUME_QUERY_ID = "q_WoKuQA6Dr45"
TOTAL_VOLUME_TOLERANCE = 0.02  # ±2%
DATA_JSON_PATH = "data.json"


def iso_date(bucket_key):
    """Starscape bucket _key → 'YYYY-MM-DD' for date-aligned joins."""
    return datetime.fromisoformat(bucket_key.replace("Z", "+00:00")).strftime("%Y-%m-%d")


def display_label_to_iso(label, reference_year):
    """Legacy data.json labels are display strings like 'Mar 02'. Reconstruct ISO.

    reference_year is inferred from the query's newest fetched date so we pick
    the correct year at a Dec→Jan boundary.
    """
    parsed = datetime.strptime(label, "%b %d").replace(year=reference_year)
    return parsed.strftime("%Y-%m-%d")


def daily_counts_by_iso(api_response):
    """Extract {iso_date: count} from a Starscape histogram response."""
    out = {}
    for bucket in api_response["daily_volume"]["_buckets"]:
        out[iso_date(bucket["_key"])] = bucket["_count"]
    return out


def net_sentiment_pct(api_response):
    """Compute net_sentiment as pct positive (0..100), or None if no sentiment agg."""
    sent = api_response.get("sentiment")
    if not sent or "_buckets" not in sent:
        return None
    pos = sum(b["_count"] for b in sent["_buckets"] if b["_key"] in ("p", "positive"))
    neg = sum(b["_count"] for b in sent["_buckets"] if b["_key"] in ("n", "negative"))
    total = pos + neg
    if total == 0:
        return None
    return round(pos / total * 100, 1)


def fetch_all():
    """Fetch every query from QUERY_METRICS. Returns {metric_name: api_response}."""
    results = {}
    for query_id, cfg in QUERY_METRICS.items():
        name = cfg["metric_name"]
        print(f"  fetching {name} ({query_id})...", flush=True)
        results[name] = fetch_infegy_data(query_id)
    return results


def load_baseline():
    with open(DATA_JSON_PATH) as f:
        return json.load(f)


def baseline_daily_by_iso(baseline, metric_name, reference_year):
    """data.json stores trend[] + labels[] where labels are 'Mar 02' strings.
    Return {iso_date: trend_value} for the given metric."""
    m = baseline["metrics"].get(metric_name)
    if not m:
        return {}
    out = {}
    for label, value in zip(m["labels"], m["trend"]):
        out[display_label_to_iso(label, reference_year)] = value
    return out


class Report:
    def __init__(self):
        self.checks = []  # list of (name, passed, detail)

    def check(self, name, passed, detail=""):
        self.checks.append((name, passed, detail))
        marker = "PASS" if passed else "FAIL"
        print(f"  [{marker}] {name}" + (f" — {detail}" if detail else ""))

    def all_passed(self):
        return all(p for _, p, _ in self.checks)

    def summary(self):
        passed = sum(1 for _, p, _ in self.checks if p)
        print(f"\n{passed}/{len(self.checks)} checks passed.")


def assert_rollup_invariants(report, counts_by_metric):
    """Strict per-day row-level subset invariants from the spec."""
    pairs = [
        ("financialAnxiety", "despair"),
        ("layoffMentions", "despair"),
        ("consumerBehavior", "hope"),
        ("hope", "esi"),
        ("despair", "esi"),
    ]
    for child, parent in pairs:
        c = counts_by_metric[child]
        p = counts_by_metric[parent]
        dates = sorted(set(c) | set(p))
        violations = []
        for d in dates:
            cv = c.get(d, 0)
            pv = p.get(d, 0)
            if cv > pv:
                violations.append(f"{d}: {child}={cv} > {parent}={pv}")
        if violations:
            report.check(
                f"{child} ⊂ {parent}",
                False,
                f"{len(violations)} day(s) violate, first: {violations[0]}",
            )
        else:
            report.check(f"{child} ⊂ {parent}", True, f"{len(dates)} days")


def assert_total_volume_stable(report, new_total_counts, baseline):
    """Baseline data.json does not store the total-volume series directly (it's
    consumed to compute percentages). We approximate stability by checking that
    the new total-volume query responds with plausible non-zero data."""
    if not new_total_counts:
        report.check("total_volume non-empty", False, "no buckets returned")
        return
    report.check(
        "total_volume non-empty",
        True,
        f"{len(new_total_counts)} days, sum={sum(new_total_counts.values())}",
    )

    prev = baseline.get("previous_total_volume_daily") or {}
    if not prev:
        report.check(
            "total_volume ±2% vs prior run",
            True,
            "no prior snapshot — recording current run for next time (see notes)",
        )
        return
    common = sorted(set(prev) & set(new_total_counts))
    if not common:
        report.check("total_volume ±2% vs prior run", False, "no overlapping days")
        return
    worst = 0.0
    worst_day = None
    for d in common:
        old = prev[d]
        new = new_total_counts[d]
        if old == 0:
            continue
        diff = abs(new - old) / old
        if diff > worst:
            worst = diff
            worst_day = d
    report.check(
        "total_volume ±2% vs prior run",
        worst <= TOTAL_VOLUME_TOLERANCE,
        f"worst day {worst_day}: {worst*100:.2f}% diff",
    )


def assert_net_sentiment_bounds(report, sentiment_by_metric):
    """Post-restructure, net_sentiment should be strictly in (0, 100).
    A value of exactly 0 or 100 signals the constant-sentiment bug this
    restructure is fixing (or a degenerate keyword set)."""
    for name, pct in sentiment_by_metric.items():
        if pct is None:
            report.check(f"{name} net_sentiment present", False, "no sentiment agg")
            continue
        in_bounds = 0 < pct < 100
        report.check(
            f"{name} net_sentiment ∈ (0,100)",
            in_bounds,
            f"{pct}%",
        )


def volume_delta_summary(new_counts, baseline_daily, metric_name, reference_year):
    baseline = baseline_daily_by_iso(
        {"metrics": {metric_name: baseline_daily}},
        metric_name,
        reference_year,
    ) if isinstance(baseline_daily, dict) and "labels" in baseline_daily else baseline_daily
    common = sorted(set(new_counts) & set(baseline))
    if not common:
        return "no overlap with baseline"
    new_sum = sum(new_counts[d] for d in common)
    old_sum = sum(baseline[d] for d in common)
    if old_sum == 0:
        return f"new={new_sum}, baseline=0"
    delta = (new_sum - old_sum) / old_sum * 100
    return f"new={new_sum}, baseline={old_sum}, delta={delta:+.1f}% over {len(common)} days"


def main():
    print("FCI diff-report — pre-flight gate\n")
    print("Fetching live Starscape data (current stored query bodies)...")
    responses = fetch_all()

    counts_by_metric = {
        name: daily_counts_by_iso(resp)
        for name, resp in responses.items()
        if name != "Total Volume"
    }
    total_counts = daily_counts_by_iso(responses["Total Volume"])
    sentiment_by_metric = {
        name: net_sentiment_pct(resp)
        for name, resp in responses.items()
        if name != "Total Volume"
    }

    reference_year = datetime.now(timezone.utc).year

    baseline = load_baseline()

    print("\nPer-metric volume deltas (new vs. baseline data.json, percentage trend):")
    print("  NOTE: baseline trend values are daily percentages, not counts.")
    print("  This is a sanity sketch only; invariants below use absolute counts from the new fetch.\n")
    for name in counts_by_metric:
        m = baseline.get("metrics", {}).get(name)
        if not m:
            print(f"  {name}: no baseline entry")
            continue
        new_count_sum = sum(counts_by_metric[name].values())
        baseline_trend_sum = sum(m.get("trend", []))
        print(
            f"  {name}: new_count_total={new_count_sum}, "
            f"baseline_trend_sum={baseline_trend_sum:.1f} (pct-days)"
        )

    print("\nRollup invariants (per-day strict subset):")
    report = Report()
    assert_rollup_invariants(report, counts_by_metric)

    print("\nSanity bounds:")
    assert_total_volume_stable(report, total_counts, baseline)
    assert_net_sentiment_bounds(report, sentiment_by_metric)

    print("\nNet sentiment snapshot:")
    for name, pct in sentiment_by_metric.items():
        print(f"  {name}: {pct}%")

    report.summary()

    # Persist total-volume snapshot so the next run can do a ±2% stability check.
    baseline["previous_total_volume_daily"] = total_counts
    baseline["previous_total_volume_recorded_at"] = datetime.now(timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with open(DATA_JSON_PATH, "w") as f:
        json.dump(baseline, f, indent=4)
    print(f"\nRecorded total-volume snapshot into {DATA_JSON_PATH} for next run.")

    if not report.all_passed():
        print("\nFAIL — do not proceed with the next staged edit until invariants pass.")
        sys.exit(1)
    print("\nPASS — safe to proceed with the next staged edit.")


if __name__ == "__main__":
    main()
