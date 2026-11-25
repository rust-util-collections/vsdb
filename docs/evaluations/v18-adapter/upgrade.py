"""Compare the adopted adapter with v4 using the original saved offers."""
import argparse
import csv
import fcntl
import json
import gzip
import hashlib
import os
from pathlib import Path
import random
from statistics import median
import subprocess
import tempfile

from summarize import load_ok, require, throughput

def read(path):
    with gzip.open(path, "rt") as f:
        return [json.loads(line) for line in f]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def collect(a):
    original=read(a.original)
    first=original[0]
    raw=[r for r in original if r["section"]=="trial"]
    require(len(raw)==first["repetitions"]*24, "original suite incomplete")
    offers={r["placement"]:r["rate"] for r in original if r["section"]=="offer"}
    bins={"reference":a.v4.resolve(),"comparison":a.selected.resolve()}
    require(digest(bins["reference"])==first["sha256"]["v4"], "v4 binary must match original")
    meta={**first, "suite":"selected-upgrade", "selected_revision":a.revision, "selected_buffer_mib":a.buffer,
          "sha256":{k:digest(b) for k,b in bins.items()},"original_packet_sha256":digest(a.original),"offers":offers}
    with gzip.open(a.raw,"xt") as out:
        def save(record):
            out.write(json.dumps(record,separators=(",",":"))+"\n")
            out.flush()
        save(meta)
        for i in range(1,6):
            cells=[("hot","paced"),("all","paced"),("hot","saturation"),("all","saturation")]
            random.Random(20261126+i).shuffle(cells)
            for placement,load in cells:
                pair_order = ("AA", "AB") if i % 2 else ("AB", "AA")
                for pair_kind in pair_order:
                    arms = list(bins)
                    if (i + int(pair_kind == "AB")) % 2:
                        arms.reverse()
                    for order,arm in enumerate(arms,1):
                        actual = "reference" if pair_kind == "AA" else arm
                        rate=offers[placement] if load=="paced" else 0
                        duration=0 if rate else first["seconds"]*1000
                        args=["taskset","-c",first["cpus"],str(bins[actual]),"memory",placement,
                            str(first["seed_base"]+i),str(rate),str(first["ops"]),str(duration)]
                        with a.lock.open("a") as lock:
                            fcntl.flock(lock,fcntl.LOCK_EX)
                            with tempfile.TemporaryDirectory(dir=a.scratch) as db:
                                result=subprocess.run(args,env={**os.environ,"VSDB_BASE_DIR":db},capture_output=True,text=True)
                                require(result.returncode==0,result.stderr)
                        metrics={k:int(v) if v.isdigit() else v for k,v in (x.split("=",1) for x in result.stdout.split()[1:])}
                        save({"round":i,"placement":placement,"load":load,"arm":arm,"pair":pair_kind,"order":order,
                              "binary_sha256":meta["sha256"][actual], **metrics})
            print("Finished selected upgrade round",i,flush=True)

def summarize(a):
    original=read(a.original)
    rows=read(a.raw)
    meta=rows[0]
    require(meta["suite"]=="selected-upgrade" and meta["original_packet_sha256"]==digest(a.original),"wrong source matrix")
    require(meta["sha256"]["reference"] == original[0]["sha256"]["v4"], "v4 reference changed")
    pairs={(p,l,i,k):{} for p in ("hot","all") for l in ("paced","saturation")
           for i in range(1,6) for k in ("AA","AB")}
    for r in rows[1:]:
        pair=pairs[r["placement"],r["load"],r["round"],r["pair"]]
        require(r["arm"] not in pair,"duplicate selected-upgrade arm")
        pair[r["arm"]]=r
        actual = "reference" if r["pair"] == "AA" else r["arm"]
        require(r["binary_sha256"] == meta["sha256"][actual], "wrong executable for pair role")
        require(r["seed"]==meta["seed_base"]+r["round"] and r["shards"]==16 and r["bulk_entries"]==32
                and r["hot"]==str(r["placement"]=="hot").lower() and r["ops"]==meta["ops"]
                and r["small"]>0 and r["bulk"]>0 and r["logical_bytes"]==r["small"]*136+r["bulk"]*(32*520)
                and r["peak_kib"]>0 and r["wall_ns"]>0,"wrong selected-upgrade workload")
        if actual=="comparison":
            require(r["after_buffer_size"]==r["before_buffer_size"]==meta["selected_buffer_mib"]<<20,"wrong selected buffer")
        else:
            require("after_buffer_size" not in r, "v4 control emitted v5-only gauges")
        if r["load"]=="paced":
            require(r["rate"]==meta["offers"][r["placement"]] and r["small"]==meta["ops"] and
                    r["bulk"]==2*meta["ops"] and r["p99_ns"]>0,"incomplete paced workload")
        else:
            require(r["rate"]==0 and r["p99_ns"]==0,"unexpected paced saturation")
    for pair in pairs.values():
        require(set(pair)=={"reference","comparison"} and {r["order"] for r in pair.values()}=={1,2},"incomplete pair")
    with a.summary.open("w") as f:
        writer=csv.writer(f,lineterminator="\n")
        writer.writerow(["placement","load","metric","v4_median","selected_median","paired_gain_pct","low_gain_pct",
                         "high_gain_pct","positive_pairs","v4_aa_max_pct","load_kept_pairs",
                         "v4_peak_max_kib","selected_peak_max_kib","v4_end_l0_median","selected_end_l0_median"])
        for placement in ("hot","all"):
            for load in ("paced","saturation"):
                observed=[pairs[placement,load,i,"AB"] for i in range(1,6)]
                noise=[pairs[placement,load,i,"AA"] for i in range(1,6)]
                v4=[r["reference"] for r in observed]
                sel=[r["comparison"] for r in observed]
                eligible=sum(all(load_ok(r) for r in (*ab.values(), *aa.values())) for ab,aa in zip(observed,noise))
                metrics = ["small_p99_ms"] if load=="paced" else list(throughput(v4[0]))
                for metric in metrics:
                    def value(r):
                        return r["p99_ns"]/1e6 if metric=="small_p99_ms" else throughput(r)[metric]
                    aa=max(abs(100*(value(n["comparison"])/value(n["reference"])-1)) for n in noise)
                    gains=[(-1 if load=="paced" else 1)*100*(value(y)/value(x)-1) for x,y in zip(v4,sel)]
                    vals=[median(map(value,v4)),median(map(value,sel)),median(gains),min(gains),max(gains),
                          sum(g>0 for g in gains),aa,eligible,max(r["peak_kib"] for r in v4+[
                              r for n in noise for r in n.values()]),max(r["peak_kib"] for r in sel),
                          median(r["after_l0"] for r in v4),median(r["after_l0"] for r in sel)]
                    writer.writerow([placement,load,metric,*[round(v,3) for v in vals]])

if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",choices=["run","summarize"])
    parser.add_argument("original",type=Path)
    parser.add_argument("raw",type=Path)
    parser.add_argument("--v4",type=Path)
    parser.add_argument("--selected",type=Path)
    parser.add_argument("--buffer",type=int)
    parser.add_argument("--revision")
    parser.add_argument("--lock",type=Path)
    parser.add_argument("--scratch",type=Path)
    parser.add_argument("--summary",type=Path)
    a=parser.parse_args()
    if a.action=="run":
        if not all((a.v4,a.selected,a.buffer,a.revision,a.lock,a.scratch)):
            parser.error("run requires --v4, --selected, --buffer, --revision, --lock and --scratch")
        collect(a)
    else:
        if a.summary is None:
            parser.error("summarize requires --summary")
        summarize(a)
