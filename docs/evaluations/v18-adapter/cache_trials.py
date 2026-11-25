"""Exercise real VSDB read/write pressure after populating its shared store."""
import argparse
import csv
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from summarize import load_ok, require

def measure(a):
    bins = {"6MiB": a.small.resolve(), "16MiB": a.large.resolve()}
    hashes = {key: hashlib.sha256(exe.read_bytes()).hexdigest() for key, exe in bins.items()}
    metadata = dict(suite="warm-cache-pressure", cpus=a.cpus, rate=a.rate, operations=10000,
                    sha256=hashes, source=a.revision, seed_base=20361126, rounds=3,
                    cache_target_bytes=128 << 20, rss_budget_kib=1 << 20)
    with gzip.open(a.raw,"xt") as output:
        output.write(json.dumps(metadata)+"\n")
        for repeat in range(1,4):
            order=list(bins)
            if repeat%2: order.reverse()
            for index, arm in enumerate(order,1):
                with a.lock.open("a") as lock:
                    fcntl.flock(lock,fcntl.LOCK_EX)
                    with tempfile.TemporaryDirectory(dir=a.scratch) as root:
                        cmd=["taskset","-c",a.cpus,str(bins[arm]),"memory-cache",str(20361126+repeat),str(a.rate)]
                        r=subprocess.run(cmd,env={**os.environ,"VSDB_BASE_DIR":root},text=True,capture_output=True)
                        require(r.returncode==0,r.stderr+" "+r.stdout)
                row = {key: int(value) if value.isdigit() else value
                       for key,value in (field.split("=",1) for field in r.stdout.split()[1:])}
                row.update(round=repeat,arm=arm,order=index,binary_sha256=hashes[arm],load="paced")
                output.write(json.dumps(row,separators=(",",":"))+"\n")
                output.flush()
            print("Finished warm-cache resource pair",repeat,flush=True)

def summarize(raw, dest):
    with gzip.open(raw,"rt") as f:
        meta=json.loads(next(f))
        records=[json.loads(s) for s in f]
    require(meta["suite"]=="warm-cache-pressure" and meta["rounds"]==3 and len(records)==6,"incomplete cache pressure series")
    require({(r["round"],r["arm"]) for r in records}=={(i,a) for i in range(1,4) for a in ("6MiB","16MiB")},
            "unmatched warm-cache arms")
    with dest.open("w") as out:
        w=csv.writer(out,lineterminator="\n")
        w.writerow(["pair","buffer_mib","seed","warm_segmented_mib","min_segmented_mib","warm_peak_mib","peak_rss_mib","within_1gib",
                    "timed_max_active_mib","timed_max_immutable_mib","timed_max_immutable_count","timed_max_segmented_mib",
                    "timed_max_overflow_mib","timed_max_pins_mib","start_l0","end_l0","end_immutables","puts","batches","reads",
                    "writer_load_kept","put_p99_ms","read_p99_ms"])
        for r in records:
            buf = 6 if r["arm"]=="6MiB" else 16
            require(r["reader"]==r["small"]==r["ops"]==meta["operations"]==10000 and r["bulk"]==20000 and r["rate"]==meta["rate"]
                    and r["binary_sha256"]==meta["sha256"][r["arm"]] and r["seed"]==meta["seed_base"]+r["round"]
                    and r["shards"]==16 and r["hot"]=="false" and r["bulk_entries"]==32
                    and r["before_buffer_size"]==r["after_buffer_size"]==buf<<20 and r["warm_rows_each"]==32768,
                    "wrong measured workload or binary")
            require(r["before_segmented"]>=meta["cache_target_bytes"] and r["sampled_segmented"]>=r["before_segmented"]
                    and r["observations"]>0 and min(r["small_p99_ns"],r["reader_p99_ns"],r["peak_kib"])>0,"missing cache/latency evidence")
            for stage in ("before","after"):
                members=[[int(v) for v in m.split(":")] for m in r[stage+"_members"].split(",")]
                require(len(members)==16 and all(len(m)==4 for m in members),"missing per-shard memory observation")
            mib=lambda key:round(r[key]/(1<<20),3)
            w.writerow([r["round"],buf,r["seed"],mib("before_segmented"),mib("min_segmented"),round(r["warm_peak_kib"]/1024,3),
                        round(r["peak_kib"]/1024,3),r["peak_kib"]<=meta["rss_budget_kib"],mib("sampled_active"),mib("sampled_immutable"),
                        r["sampled_generations"],mib("sampled_segmented"),mib("sampled_overflow"),mib("sampled_pins"),r["before_l0"],
                        r["after_l0"],r["after_immutable_count"],r["small"],r["bulk"],r["reader"],load_ok(r),
                        round(r["small_p99_ns"]/1e6,3),round(r["reader_p99_ns"]/1e6,3)])

if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("action",choices=("run","summarize"))
    p.add_argument("raw",type=Path)
    p.add_argument("--small",type=Path)
    p.add_argument("--large",type=Path)
    p.add_argument("--rate",type=int)
    p.add_argument("--revision")
    p.add_argument("--lock",type=Path)
    p.add_argument("--scratch",type=Path)
    p.add_argument("--cpus",default="4-7")
    p.add_argument("--summary",type=Path)
    a=p.parse_args()
    if a.action=="run":
        if not all((a.small,a.large,a.rate,a.revision,a.lock,a.scratch)):p.error("run requires two binaries, rate, revision, lock and scratch")
        measure(a)
    else:
        if a.summary is None:p.error("summarize requires --summary")
        summarize(a.raw,a.summary)
