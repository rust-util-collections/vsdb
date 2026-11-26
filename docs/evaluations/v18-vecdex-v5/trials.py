"""Interleaved, fresh-process VecDex comparisons on one captured graph."""
import argparse
import csv
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("bin_dir", type=Path)
p.add_argument("fixtures", type=Path)
p.add_argument("output", type=Path)
p.add_argument("regime", choices=["warm", "device", "singleton", "secondary", "device-width8", "landed", "distance-cache", "width8-repeat", "final", "policy"])
p.add_argument("--cpus", required=True)
p.add_argument("--lock", type=Path)
p.add_argument("--samples", type=int, default=10_000)
p.add_argument("--seed", type=int, default=261126)
args = p.parse_args()
if args.samples < 10_000:
    p.error("latency qualification requires at least 10,000 queries per process")
args.bin_dir = args.bin_dir.resolve()
args.fixtures = args.fixtures.resolve()
raw_fields = ["samples","p50_ns","p99_ns","digest","nearest","blocks","logical_bytes","storage_bytes","io_positive"]
fields = ["round","pair","arm","order","seed","binary","regime",*raw_fields]
reference = {}
args.output.parent.mkdir(parents=True,exist_ok=True)
with args.output.open("x") as out:
    writer = csv.DictWriter(out,fieldnames=fields,lineterminator="\n")
    writer.writeheader()
    out.flush()
    for n in range(1,6):
        pairings=([("v4", "v4", "1"),("width8","1","8")] if args.regime=="secondary"
                  else [("AA","1","1"),("AB","1","32")])
        if args.regime == "width8-repeat":
            pairings = [("AA", "shipped", "shipped"), ("AB", "shipped", "8")]
        if args.regime == "distance-cache":
            pairings = [("AA", "shipped", "shipped"), ("AB", "shipped", "cached")]
        if args.regime == "policy":
            pairings = [("AA", "policy", "policy"), ("AB", "final", "policy"),
                        ("v4AA", "v4", "v4"), ("upgrade", "v4", "policy")]
        if args.regime == "final":
            pairings = [("AA", "final", "final"), ("AB", "shipped", "final"),
                        ("v4AA", "v4", "v4"), ("upgrade", "v4", "final")]
        if args.regime == "landed":
            pairings = [("AA", "1", "1"), ("AB", "1", "shipped"),
                        ("v4AA", "v4", "v4"), ("upgrade", "v4", "shipped")]
        if args.regime == "device-width8":
            pairings = [("diagnostic-width8", "8", "8")]
        if n%2==0: pairings.reverse()
        for pair, one, two in pairings:
            arms=[("reference",one),("comparison",two)]
            if (n + (pair in ("AB","width8"))) % 2: arms.reverse()
            if args.regime == "device-width8":
                arms = [("observation","8")]
            for order,(arm,binary) in enumerate(arms,1):
                regime = "device" if args.regime in ("device", "device-width8") else "warm"
                version="v4" if binary=="v4" else "v5"
                fixture=args.fixtures / ("v5-singleton" if args.regime=="singleton" else version+"-"+regime)
                command=["taskset","-c",args.cpus,str(args.bin_dir/binary),"run",str(fixture),
                         regime,str(args.seed+n),str(args.samples)]
                if args.lock: command=["flock",str(args.lock),*command]
                completed=subprocess.run(command,text=True,capture_output=True)
                if completed.returncode:
                    raise RuntimeError(completed.stderr)
                parsed=dict(part.split("=",1) for part in completed.stdout.split())
                if set(parsed)!=set(raw_fields) or int(parsed["samples"])!=args.samples:
                    raise ValueError("incomplete query output")
                result=parsed["digest"],parsed["nearest"]
                if reference.setdefault(n,result)!=result:
                    raise ValueError("ordered IDs, distances or known nearest-neighbor results differ on the same trace")
                if regime=="device" and int(parsed["io_positive"])*100 < args.samples*95:
                    raise ValueError(f"insufficient per-query disk I/O: {parsed}")
                if regime=="warm" and int(parsed["storage_bytes"]):
                    raise ValueError("warm run accessed storage")
                writer.writerow(dict(round=n,pair=pair,arm=arm,order=order,seed=args.seed+n,binary=binary,
                                     regime=args.regime,**parsed))
                out.flush()
        print(args.regime,"round",n,flush=True)
