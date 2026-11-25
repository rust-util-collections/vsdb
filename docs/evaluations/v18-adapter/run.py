"""Measure a complete paired series in isolated VSDB processes on Linux."""
import argparse
import contextlib
import fcntl
import gzip
import hashlib
import itertools
import json
import os
from pathlib import Path
import random
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("bin", type=Path, help="v4, v5-16m, v5, v4-alloc and v5-alloc executables")
p.add_argument("output", type=Path)
p.add_argument("--scratch", type=Path, required=True)
p.add_argument("--lock", type=Path, required=True)
p.add_argument("--cpus", default="4-7")
p.add_argument("--repetitions", type=int, default=5)
p.add_argument("--ops", type=int, default=10_000)
p.add_argument("--seconds", type=int, default=4)
a = p.parse_args()
if a.repetitions < 5 or a.ops < 10_000 or a.ops % 8 or a.seconds < 1:
    p.error("use at least five independent repetitions, 10,000 foreground operations (multiple of eight) and a positive duration")
a.scratch.mkdir(parents=True, exist_ok=True)
bin_dir = a.bin.resolve()

def output(cmd):
    return subprocess.check_output(cmd, text=True).strip()

def measurements(line):
    return {k: int(v) if v.isdigit() else v for k, v in (s.split("=", 1) for s in line.split()[1:])}

@contextlib.contextmanager
def cpu_lock():
    with a.lock.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        yield

def trial(binary, args, section, **fields):
    with cpu_lock(), tempfile.TemporaryDirectory(dir=a.scratch) as dbdir:
        run = subprocess.run(["taskset","-c",a.cpus,str(bin_dir/binary),*map(str,args)],
            env={**os.environ,"VSDB_BASE_DIR":dbdir},text=True,capture_output=True)
        if run.returncode:
            raise RuntimeError(f"{binary} {args}: {run.stderr} {run.stdout}")
    record = {"section":section, "binary":binary, **fields}
    if args[0] == "memory":
        record.update(measurements(run.stdout.strip()))
    else:
        record["operations"] = [measurements(line) for line in run.stdout.splitlines()]
    write(record)
    return record

def write(record):
    out.write(json.dumps(record, separators=(",",":"))+"\n")
    out.flush()

def qualified(row):
    def kept(prefix):
        completed = row[prefix]
        streams = 16 if prefix=="bulk" else 1
        return all(row[f"{prefix}_{when}_debt"] <= max(streams,2,completed//100) for when in ("half","end"))
    return row["small"]==row["ops"] and row["bulk"]==row["ops"]//8*16 and kept("small") and kept("bulk")

with gzip.open(a.output, "xt") as out:
    binaries = ["v4","v5-16m","v5","v4-alloc","v5-alloc"]
    write({"section":"metadata","cpus":a.cpus,"kernel":output(["uname","-srmo"]),
           "processor":next(line.split(":",1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")),
           "rustc":output(["rustc","+stable","-Vv"]),"filesystem":output(["findmnt","-T",a.scratch,"-n","-o","SOURCE,FSTYPE,OPTIONS"]),
           "compression":output(["btrfs","property","get",a.scratch,"compression"]),"seed_base":20261126,
           "repetitions":a.repetitions,"ops":a.ops,"seconds":a.seconds,
           "sha256":{b:hashlib.sha256((bin_dir/b).read_bytes()).hexdigest() for b in binaries}})
    for i in range(5):
        for binary in (["v4-alloc","v5-alloc"] if i%2==0 else ["v5-alloc","v4-alloc"]):
            trial(binary,["allocations"],"allocations",round=i+1)
    rates={}
    for placement in ["hot","all"]:
        pilot=[trial(b,["memory",placement,20261126,0,4096,2000],"calibration",placement=placement) for b in ["v5-16m","v5","v4"]]
        # Each scalar arrival carries two 32-entry bulk submissions across
        # the sixteen streams. A capacity estimate is only an initial guess.
        estimate=int(min(r["logical_bytes"]*1e9/r["wall_ns"]/(136+2*32*520) for r in pilot))
        rate=max(1,estimate)
        for _ in range(8):
            verified=[trial(b,["memory",placement,20261126,rate,4096,0],"calibration",placement=placement)
                      for b in ["v5-16m","v5","v4"]]
            if all(qualified(r) for r in verified):
                break
            rate=max(1,int(rate*0.72))
        else:
            raise ValueError("No stable matched offer")
        rates[placement]=max(1,int(0.70*rate))
        write({"section":"offer","placement":placement,"verified_rate":rate,"rate":rates[placement]})
        print("Calibrated",placement,rates[placement],"from",rate,flush=True)
    for i in range(1,a.repetitions+1):
        cells=list(itertools.product(["hot","all"],["paced","saturation"]))
        random.Random(20261126+i).shuffle(cells)
        for placement,load in cells:
            rate=rates[placement] if load=="paced" else 0
            duration=a.seconds*1000 if load=="saturation" else 0
            pairs=[("AA",["v5-16m","v5-16m"]),("AB",["v5-16m","v5"]),("upgrade",["v4","v5"])]
            random.Random(20261126+i+cells.index((placement,load))).shuffle(pairs)
            for kind,bins in pairs:
                arms=list(zip(["reference","comparison"],bins))
                if (i+(kind=="AB"))%2:
                    arms.reverse()
                for order,(arm,binary) in enumerate(arms):
                    r=trial(binary,["memory",placement,20261126+i,rate,a.ops,duration],"trial",round=i,
                            placement=placement,load=load,pair=kind,arm=arm,order=order+1)
                    if load=="paced":
                        assert r["small"]==a.ops and r["bulk"]==a.ops//8*16
        print("Finished round",i,flush=True)
