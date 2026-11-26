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
for regime in ("warm", "device", "singleton", "device-width8", "distance-cache", "width8-repeat", "policy"):
    path = args.folder/(regime+".csv")
    rows = list(csv.DictReader(path.open()))
    pairs = ({"diagnostic-width8": ("8",)} if regime=="device-width8"
             else {"AA": ("1","1"),"AB": ("1","32")})
    if regime == "width8-repeat":
        pairs = {"AA": ("shipped","shipped"), "AB": ("shipped","8")}
    if regime == "distance-cache":
        pairs = {"AA": ("shipped","shipped"), "AB": ("shipped","cached")}
    if regime == "policy":
        pairs = {"AA": ("policy","policy"), "AB": ("final","policy"),
                 "v4AA": ("v4","v4"), "upgrade": ("v4","policy")}
    wanted = {(r,pair): list(binaries) for pair,binaries in pairs.items() for r in range(1,6)}
    groups = defaultdict(list)
    for row in rows:
        n = int(row["round"])
        seed = int(row["seed"])
        samples = int(row["samples"])
        expected_seed = args.seed + n + {"width8-repeat":100_000, "policy":300_000}.get(regime,0)
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

# RSS is a shorter, independent diagnostic. These rows never enter a p99
# comparison. Each arm must complete all three seeds in each measured regime.
resource_digests = {}
for filename, arms in (("rss.csv", ["1","8","32","shipped","v4"]), ("rss-policy.csv", ["policy"])):
    resource = defaultdict(list)
    expected_keys = {(i, regime, arm) for i in range(1,4)
                     for regime in (("warm",) if filename=="rss-policy.csv" else ("warm","device")) for arm in arms
                     if (regime,arm) != ("device","v4")}
    with (args.folder/filename).open() as f:
        for row in csv.DictReader(f):
            i, regime, arm = int(row["round"]), row["regime"], row["arm"]
            key = i, regime, arm
            if key not in expected_keys:
                raise ValueError(f"duplicate or unexpected RSS observation: {key}")
            expected_keys.remove(key)
            samples = 1000 if regime=="warm" else 500
            seed = 741126+i
            peak = int(row["gnu_time_peak_kib"])
            observed = int(row["sampled_process_vmhwm_kib"])
            io, hits = int(row["storage_bytes"]), int(row["io_positive"])
            if int(row["seed"])!=seed or int(row["samples"])!=samples or min(peak,observed)<=0:
                raise ValueError(f"invalid RSS sample: {row}")
            if (regime=="warm" and (io or hits)) or (regime=="device" and
                    (not 0 < hits <= samples or hits*100 < samples*95 or io < hits*4096)):
                raise ValueError(f"unqualified RSS regime: {row}")
            result = row["digest"],row["nearest"]
            if resource_digests.setdefault((regime,seed),result)!=result:
                raise ValueError(f"different RSS query results on same trace: {row}")
            resource[regime,arm].append((peak,observed))
    if expected_keys:
        raise ValueError(f"missing RSS observations: {sorted(expected_keys)}")
    for (regime,arm), values in sorted(resource.items()):
        print("RSS",regime,arm,"max_gnu_peak_kib",max(a for a,b in values),
              "max_sampled_probe_vmhwm_kib",max(b for a,b in values))
