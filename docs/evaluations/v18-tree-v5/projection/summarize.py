"""Validate every independent process and report projection and upgrade guards."""
import argparse
import csv
from collections import defaultdict
from statistics import median

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("raw")
parser.add_argument("--extended", action="store_true")
args = parser.parse_args()
classes = ["hit", "miss", "contains-hit", "contains-miss"]
if not args.extended:
    classes.append("scan")
rows = defaultdict(dict)
identities = defaultdict(set)
with open(args.raw) as raw:
    for row in csv.DictReader(raw):
        repeat, cls, arm = int(row["round"]), row["class"], row["arm"]
        order = ["A1", "A2", "V1", "V2", "B0", "B1"]
        if repeat % 2 == 0:
            order.reverse()
        position = int(row["position"])
        if repeat not in range(1, 6) or not 1 <= position <= 6 or arm != order[position - 1]:
            raise ValueError("unmatched round, arm or order")
        if int(row["seed"]) != (20284197 if args.extended else 20274197) + repeat * 1021:
            raise ValueError("unmatched trace seed")
        count, p99, wall = (int(row[k]) for k in ("queries", "p99_ns", "wall_ns"))
        if cls not in classes or count != (10000 if cls == "scan" else (1000000 if args.extended else 250000)):
            raise ValueError("incomplete class")
        if p99 <= 0 or wall <= 0 or row["cpu"] != "4" or arm in rows[repeat, cls]:
            raise ValueError("invalid measurement or duplicate")
        rows[repeat, cls][arm] = (p99, 1e9 * count / wall)
        identity = {"B1": "candidate", "V1": "v4", "V2": "v4"}.get(arm, "control")
        identities[identity].add(row["binary_sha256"])
digests = [next(iter(v)) for v in identities.values() if len(v) == 1]
if len(set(digests)) != 3 or any(len(d) != 64 for d in digests):
    raise ValueError("expected three distinct, stable executable digests")
for cls in classes:
    trials = [rows[i, cls] for i in range(1, 6)]
    if any(set(t) != {"A1", "A2", "V1", "V2", "B0", "B1"} for t in trials):
        raise ValueError("incomplete pairs")
    def deltas(a, b, metric):
        return [100 * (1 - t[b][metric] / t[a][metric]) * (1 if metric == 0 else -1)
                for t in trials]
    ab = deltas("B0", "B1", 0)
    aa = max(map(abs, deltas("A1", "A2", 0)))
    v4_noise = max(map(abs, deltas("V1", "V2", 0)))
    print(cls, "p99_ns_v4/v5_control/candidate", *(round(median(t[k][0] for t in trials)) for k in ("V1","B0","B1")),
          "gain%,range", *(round(v,2) for v in (median(ab),min(ab),max(ab))), "positive",sum(x>0 for x in ab),
          "AA%_v5/v4",round(aa,2),round(v4_noise,2),"throughput_gain%",round(median(deltas("B0","B1",1)),2),
          "v4_to_v5_control/candidate_p99_gain%",*(round(median(deltas("V1",k,0)),2) for k in ("B0","B1")),
          "v4_to_v5_control/candidate_throughput_gain%",*(round(median(deltas("V1",k,1)),2) for k in ("B0","B1")))
