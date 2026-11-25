"""Validate the complete adapter profile and emit per-placement paired metrics."""
import argparse
from collections import defaultdict
import csv
import gzip
import json
from statistics import median
import sys

def require(condition, message):
    if not condition:
        raise ValueError(message)

def load_ok(r):
    if r["load"] == "saturation":
        return True
    return all(r[f"{op}_{when}_debt"] <= max(floor, 2, r[op]//100)
               for op, floor in (("small", 1), ("bulk", 16)) for when in ("half", "end"))

def allocation_table(records):
    runs = [row for row in records if row["section"] == "allocations"]
    expected = {(i, binary) for i in range(1, 6) for binary in ("v4-alloc", "v5-alloc")}
    require(len(runs) == 10 and {(r["round"], r["binary"]) for r in runs} == expected, "missing allocation trial")
    sample_sets = set()
    by_case = defaultdict(list)
    for r in runs:
        ops = {v["case"]: v for v in r["operations"]}
        # The original packet predates the extra live-map controls.
        required = {"exists", "scan", "owned-hit", "decode"}
        require(set(ops) in (required, required | {"live-exists", "live-decode"}), "missing operation")
        require(len(ops) == len(r["operations"]), "duplicate operation")
        sample_sets.add(frozenset(ops))
        checksums = {"exists": 2048, "owned-hit": 261120, "decode": 261120, "scan": 277504,
                     "live-exists": 2048, "live-decode": 261120}
        for key, op in ops.items():
            require(op["calls"] == 2048 and op["checksum"] == checksums[key], "changed allocation fixture")
            by_case[key, r["binary"]].append((op["count"] / op["calls"], op["bytes"] / op["calls"]))
    require(len(sample_sets) == 1, "source operations differ between allocation runs")
    return by_case

def display_allocations(path, output):
    with gzip.open(path, "rt") as f:
        rows = [json.loads(line) for line in f]
    require(rows[0]["section"] == "metadata" and all(r["section"] == "allocations" for r in rows[1:]),
            "unexpected allocation packet sections")
    data = allocation_table(rows[1:])
    require(("live-exists", "v5-alloc") in data, "expected the final live-map comparisons")
    table = csv.writer(output, lineterminator="\n")
    table.writerow(["operation", "binary", "processes", "calls_per_process", "allocations_per_call", "requested_bytes_per_call"])
    for (case, binary), samples in sorted(data.items()):
        table.writerow([case, binary, len(samples), 2048, round(median(x for x, _ in samples), 3),
                        round(median(x for _, x in samples), 1)])

def throughput(row):
    """Rates over the same worker window, including its last acknowledgment."""
    duration = row["wall_ns"] / 1e9
    small = row["small"] / duration
    bulk = row["bulk"] / duration
    # Only count complete shares of the offered mix (one scalar / two batches).
    # Extra batch traffic cannot make up for a shortage of foreground calls.
    matched = min(small, bulk / 2)
    return {"throughput_mib_s": row["logical_bytes"] / duration / (1 << 20),
            "successful_small_puts_s": small, "successful_batches_s": bulk,
            "matched_mix_mib_s": matched * (136 + 2 * 32 * 520) / (1 << 20)}

def summarize(path, output):
    with gzip.open(path, "rt") as f:
        meta=json.loads(next(f))
        records=[json.loads(s) for s in f]
    count=meta["repetitions"]
    require(meta["section"]=="metadata" and count>=5 and meta["ops"]>=10_000, "insufficient paired trace")
    require(set(meta["sha256"])=={"v4","v5","v5-16m","v4-alloc","v5-alloc"},"missing binary fingerprint")

    allocation_table(records)
    offers={r["placement"]:r for r in records if r["section"]=="offer"}
    require(set(offers)=={"all","hot"}, "missing calibrated placement")
    for place,o in offers.items():
        require(o["rate"]==max(1,int(0.70*o["verified_rate"])),"changed calibrated rate")
        controls=[r for r in records if r["section"]=="calibration" and r["placement"]==place and r["rate"]==o["verified_rate"]]
        require({r["binary"] for r in controls}=={"v5-16m","v5","v4"} and
                all(load_ok(dict(r,load="paced")) for r in controls),"uncertified calibration")

    groups=defaultdict(dict)
    for r in records:
        if r["section"]!="trial":
            continue
        cell=(r["placement"],r["load"],r["pair"],r["round"])
        require(r["arm"] not in groups[cell],"duplicate observation")
        groups[cell][r["arm"]]=r
        require(r["seed"]==meta["seed_base"]+r["round"] and 1<=r["round"]<=count, "incorrect trace seed")
        require(r["shards"]==16 and r["hot"]==str(r["placement"]=="hot").lower() and r["bulk_entries"]==32,
                "incorrect placement or batch geometry")
        require(r["ops"]==meta["ops"] and r["logical_bytes"]==r["small"]*136+r["bulk"]*(32*520)
                and r["small"]>0 and r["bulk"]>0 and r["peak_kib"]>0 and r["wall_ns"]>0,"invalid completions or RSS")
        if r["binary"]!="v4":
            require(r["before_buffer_size"]==r["after_buffer_size"]==(6 if r["binary"]=="v5" else 16)<<20,
                    "unexpected actual memory geometry")
        if r["load"]=="paced":
            require(r["small"]==meta["ops"] and r["bulk"]==2*r["ops"] and r["rate"]==offers[r["placement"]]["rate"]
                    and r["p99_ns"]>0,"incomplete offered trace")
        else:
            require(r["load"]=="saturation" and r["rate"]==0 and r["p99_ns"]==0,"unexpected saturated schedule")
    expected={(place,load,pair,i) for place in offers for load in ("paced","saturation")
              for pair in ("AA","AB","upgrade") for i in range(1,count+1)}
    require(set(groups)==expected,"incomplete matrix")
    for (_,_,pair,_),arms in groups.items():
        a,b=arms["reference"],arms["comparison"]
        want={"AA":("v5-16m","v5-16m"),"AB":("v5-16m","v5"),"upgrade":("v4","v5")}[pair]
        require((a["binary"],b["binary"])==want and {a["order"],b["order"]}=={1,2},"wrong pair roles")

    table=csv.writer(output,lineterminator="\n")
    table.writerow(["placement","load","pair","metric","reference_median","comparison_median","paired_gain_pct",
                    "low_gain_pct","high_gain_pct","favorable_pairs","v5_16m_aa_max_abs_pct","matched_load_pairs",
                    "reference_max_rss_kib","comparison_max_rss_kib"])
    for place in ("hot","all"):
        for load in ("paced","saturation"):
            metrics={"throughput_mib_s":lambda r:r["logical_bytes"]*(1e9/(1<<20))/r["wall_ns"],
                     "peak_rss_mib":lambda r:r["peak_kib"]/1024,
                     "end_L0":lambda r:r["after_l0"],
                     "flushes":lambda r:r["after_flushes"]-r["before_flushes"],
                     "compactions":lambda r:r["after_compactions"]-r["before_compactions"]}
            if load=="saturation":
                for key in ("successful_small_puts_s", "successful_batches_s", "matched_mix_mib_s"):
                    metrics[key] = lambda row, field=key: throughput(row)[field]
            if load=="paced":
                metrics["small_p99_ms"]=lambda r:r["p99_ns"]/1e6
            for pair in ("AB","upgrade"):
                trials=[groups[place,load,pair,i] for i in range(1,count+1)]
                noise=[groups[place,load,"AA",i] for i in range(1,count+1)]
                kept=sum(all(load_ok(x) for d in (v,n) for x in d.values()) for v,n in zip(trials,noise))
                peaks=[max(v[a]["peak_kib"] for v in trials) for a in ("reference","comparison")]
                for metric,measure in metrics.items():
                    a=[measure(v["reference"]) for v in trials]
                    b=[measure(v["comparison"]) for v in trials]
                    fields=[""]*5
                    if metric not in ("end_L0", "flushes", "compactions"):
                        sign = -1 if metric in ("small_p99_ms", "peak_rss_mib") else 1
                        gain=[sign*100*(y/x-1) for x,y in zip(a,b)]
                        spread=max(abs(100*(measure(v["comparison"])/measure(v["reference"])-1)) for v in noise)
                        fields=[round(median(gain),3),round(min(gain),3),round(max(gain),3),sum(v>0 for v in gain),round(spread,3)]
                    table.writerow([place,load,pair,metric,round(median(a),3),round(median(b),3),*fields,kept,*peaks])

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw")
    parser.add_argument("--allocations-only", action="store_true")
    args=parser.parse_args()
    if args.allocations_only:
        display_allocations(args.raw,sys.stdout)
    else:
        summarize(args.raw,sys.stdout)
