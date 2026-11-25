"""Validate all runs, then print per-run p99 and paired effects."""
import csv
import sys
from collections import defaultdict
from statistics import median

rows = defaultdict(dict)
identities = defaultdict(set)
for row in csv.DictReader(open(sys.argv[1])):
    repeat = int(row["round"])
    op, arm = row["class"], row["arm"]
    order = ["A1", "A2", "V4", "B0", "B1"]
    if repeat % 2 == 0:
        order.reverse()
    if not 1 <= repeat <= 5 or arm != order[int(row["position"]) - 1]:
        raise ValueError("unexpected run order")
    if int(row["seed"]) != 20261126 + repeat * 1021:
        raise ValueError("trace seed mismatch")
    count, p99, wall = (int(row[k]) for k in ("queries", "p99_ns", "wall_ns"))
    if op not in ("hit", "miss", "scan") or count != (10000 if op == "scan" else 60000):
        raise ValueError("missing completed queries")
    if p99 <= 0 or wall <= 0 or arm in rows[repeat, op] or row["cpu"] != "4":
        raise ValueError("invalid timing, affinity or duplicate observation")
    rows[repeat, op][arm] = (p99, 1_000_000_000 * count / wall)
    identity = {"B1": "candidate", "V4": "v4"}.get(arm, "control")
    identities[identity].add(row["binary_sha256"])
digests = [next(iter(values)) for values in identities.values() if len(values) == 1]
if len(set(digests)) != 3 or any(len(d) != 64 for d in digests):
    raise ValueError("expected three separate, stable binary fingerprints")
for op in ("hit", "miss", "scan"):
    groups = [rows[i, op] for i in range(1, 6)]
    if any(set(g) != {"A1", "A2", "B0", "B1", "V4"} for g in groups):
        raise ValueError("incomplete binary comparison")
    def gains(a, b, metric):
        return [100 * ((1 - g[b][metric] / g[a][metric]) if metric == 0
                       else (g[b][metric] / g[a][metric] - 1)) for g in groups]
    effects = gains("B0", "B1", 0)
    spread = max(map(abs, gains("A1", "A2", 0)))
    print(op, "v5 scalar/candidate p99 ns", *(round(median(g[k][0] for g in groups)) for k in ("B0", "B1")),
          "paired gain %, min/max", *(round(x, 2) for x in (median(effects), min(effects), max(effects))),
          "max A/A %", round(spread, 2), "positive", sum(x > 0 for x in effects),
          "paired throughput gain %", round(median(gains("B0", "B1", 1)), 2))
    print(op, "v4/v5 scalar p99 ns", *(round(median(g[k][0] for g in groups)) for k in ("V4", "B0")),
          "paired p99 gain %", round(median(gains("V4", "B0", 0)), 2),
          "paired throughput gain %", round(median(gains("V4", "B0", 1)), 2))
