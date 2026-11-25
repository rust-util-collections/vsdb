"""Verify complete matched VecDex query traces and report within-pair p99 changes."""
import argparse
from collections import defaultdict
import csv
from pathlib import Path
from statistics import median

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("folder", type=Path)
p.add_argument("--seed", type=int, default=261126)
args = p.parse_args()
all_trace = {}
for regime in ("warm", "device", "singleton", "secondary", "device-width8", "landed", "distance-cache", "width8-repeat"):
    path = args.folder/(regime+".csv")
    rows = list(csv.DictReader(path.open()))
    pairs = ({"diagnostic-width8": ("8",)} if regime=="device-width8"
             else {"v4": ("v4","1"), "width8": ("1","8")} if regime=="secondary"
             else {"AA": ("1","1"),"AB": ("1","32")})
    if regime == "width8-repeat":
        pairs = {"AA": ("shipped","shipped"), "AB": ("shipped","8")}
    if regime == "distance-cache":
        pairs = {"AA": ("shipped","shipped"), "AB": ("shipped","cached")}
    if regime == "landed":
        pairs = {"AA": ("1","1"), "AB": ("1","shipped"),
                 "v4AA": ("v4","v4"), "upgrade": ("v4","shipped")}
    wanted = {(r,pair): list(binaries) for pair,binaries in pairs.items() for r in range(1,6)}
    groups = defaultdict(list)
    for row in rows:
        n = int(row["round"])
        seed = int(row["seed"])
        samples = int(row["samples"])
        expected_seed = args.seed + n + (100_000 if regime == "width8-repeat" else 0)
        if row["regime"] != regime or not 1<=n<=5 or seed!=expected_seed or samples<10_000:
            raise ValueError(f"unexpected classification or trace in {path}: {row}")
        if (not 0 < int(row["p50_ns"]) <= int(row["p99_ns"])
                or not 0 <= int(row["nearest"]) <= samples
                or int(row["logical_bytes"]) <= 0 or int(row["blocks"]) <= 0):
            raise ValueError(f"unusable query observations: {row}")
        if regime=="width8-repeat" and samples!=30_000:
            raise ValueError("width-eight confirmation requires 30,000 completed queries")
        if regime=="singleton" and samples!=100_000:
            raise ValueError("the singleton control requires 100,000 timed queries")
        if regime.startswith("device"):
            coverage=int(row["io_positive"])
            if not 0 < coverage <= samples or 100*coverage<95*samples or int(row["storage_bytes"]) < 4096*coverage:
                raise ValueError(f"unqualified file reads: {row}")
        elif int(row["storage_bytes"]) or int(row["io_positive"]):
            raise ValueError(f"warm query issued storage reads: {row}")
        trace_key = ("singleton" if regime=="singleton" else "graph",seed,samples)
        observable = (row["digest"],row["nearest"])
        if all_trace.setdefault(trace_key,observable)!=observable:
            raise ValueError(f"changed query result for {trace_key}")
        groups[n,row["pair"]].append(row)
    if set(wanted)!=set(groups):
        raise ValueError(f"incomplete rounds for {path}")
    for key,binaries in wanted.items():
        arms=sorted(groups[key],key=lambda row: row["arm"], reverse=True)
        roles=["observation"] if len(binaries)==1 else ["reference","comparison"]
        if len({r["samples"] for r in arms})!=1 or [r["binary"] for r in arms]!=binaries or [r["arm"] for r in arms]!=roles or sorted(int(r["order"]) for r in arms)!=list(range(1,len(binaries)+1)):
            raise ValueError(f"wrong pairing {regime} {key}")
    for pair in pairs:
        matches = [sorted(groups[n,pair],key=lambda r:r["arm"],reverse=True) for n in range(1,6)]
        if pair=="diagnostic-width8":
            print(regime,"median p99 (us)",round(median(int(r[0]["p99_ns"]) for r in matches)/1000,2))
            continue
        a,b=zip(*matches)
        gains=[100*(1-int(j["p99_ns"])/int(i["p99_ns"])) for i,j in matches]
        print(regime,pair,"median_p99_us",*[round(median(int(r["p99_ns"]) for r in arm)/1000,2) for arm in (a,b)],
              "median_gain",round(median(gains),2),"gain_range",[round(min(gains),2),round(max(gains),2)],
              "max_abs_gain",round(max(map(abs,gains)),2),"positive",sum(g>0 for g in gains),
              "median_blocks_per_query",*[round(median(int(r["blocks"])/int(r["samples"]) for r in arm),2) for arm in (a,b)])
