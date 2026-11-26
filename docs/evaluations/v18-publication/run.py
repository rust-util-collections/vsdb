"""Run matched VSDB operation profiles from optimized standalone binaries."""
import argparse
import csv
import fcntl
import hashlib
import os
from pathlib import Path
import platform
import random
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("binaries", type=Path)
parser.add_argument("output", type=Path)
parser.add_argument("--scratch", required=True, type=Path)
parser.add_argument("--lock", required=True, type=Path)
parser.add_argument("--cpus", default="4-7")
parser.add_argument("--rate", type=int, default=3000)
parser.add_argument("--default-dag", action="store_true", help="repeat only the child comparisons in the default 16-shard namespace")
parser.add_argument("--final-edits", action="store_true", help="compare the retained checked working edit with ordinary and precomputed operations")
args = parser.parse_args()
if args.default_dag and args.final_edits:
    parser.error("choose one supplemental workload")
# Each tuple selects the control executable and operation, then the comparison.
cases = {
    "placement": ("v5-pinned", "mixed-hot", "v5-pinned", "mixed-split"),
    "v4-hot": ("v4", "mixed-hot", "v5-pinned", "mixed-hot"),
    "v4-split": ("v4", "mixed-split", "v5-pinned", "mixed-split"),
}
diagnostics = []
if args.default_dag:
    diagnostics = [("diag-previous", "dag16"), ("diag-pinned", "dag16")]
    cases = {
        "default-child": ("v5-previous", "dag16", "v5-pinned", "dag16"),
        "default-v4-child": ("v4", "dag16", "v5-pinned", "dag16"),
    }
if args.final_edits:
    diagnostics = [("diag-final", op) for op in ("individual", "overlay", "batch", "empty")]
    cases = {
        "final-dependent": ("v5-final", "individual", "v5-final", "overlay"),
        "final-precomputed": ("v5-final", "batch", "v5-final", "overlay"),
    }
columns = ("case round pair order arm binary algorithm ops seed rate p99_ns p50_ns service_ns "
           "bulk_batches bulk_rows elapsed_ns pending_half pending_end bulk_half bulk_end "
           "before_l0 after_l0 submissions counted rss_kib prefix_advance").split()
args.scratch.mkdir(parents=True, exist_ok=True)
args.output.mkdir(parents=True, exist_ok=True)
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()
with (args.output / "host.txt").open("x") as info:
    info.write(f"{platform.platform()}\nCPU affinity {args.cpus}\n")
    for cmd in (["rustc", "+stable", "-Vv"], ["findmnt", "-T", str(args.scratch), "-no", "SOURCE,FSTYPE,OPTIONS"]):
        info.write(subprocess.check_output(cmd, text=True))
    labels = {label for control, _, compared, _ in cases.values() for label in (control, compared)}
    labels.update(label for label, _ in diagnostics)
    for label in sorted(labels):
        info.write(f"{label} {digest(args.binaries / label)}\n")
def run(label, mode, n, seed):
    with tempfile.TemporaryDirectory(dir=args.scratch) as d:
        env = dict(os.environ, VSDB_BASE_DIR=d)
        output = subprocess.check_output(["taskset","-c",args.cpus,str((args.binaries/label).resolve()),mode,
                                         str(n),str(seed),str(args.rate)],text=True,env=env)
    row=dict(kv.split("=",1) for kv in output.strip().split())
    assert int(row["ops"])==n and int(row["seed"])==seed and int(row["p99_ns"])>0
    assert row.pop("mode")==("mixed" if mode.startswith("mixed-") else mode)
    if mode.startswith("mixed-"):
        assert row.pop("split")==str(mode=="mixed-split").lower()
        assert int(row["rate"])==args.rate and int(row["bulk_batches"])==(n+127)//128
        assert int(row["bulk_rows"])==1024*int(row["bulk_batches"])
    return dict(row, binary=label, algorithm=mode)
with args.lock.open("a") as lock, (args.output/"trials.csv").open("x") as f:
    writer=csv.DictWriter(f,fieldnames=columns,lineterminator="\n")
    writer.writeheader()
    def observe(arm, spec, n, seed, **settings):
        row=run(*spec,n,seed)
        writer.writerow(dict(row,arm=arm,**settings));f.flush()
    seed_base = 20661126 if args.final_edits else 20261126
    for iteration in range(1,6):
        rng=random.Random(seed_base+iteration)
        tasks=[(name,pair) for name in cases for pair in ("AA","AB")]
        rng.shuffle(tasks)
        for name,pair in tasks:
            a,am,b,bm=cases[name]
            choices=[("control",(a,am)),("comparison",(a,am) if pair=="AA" else (b,bm))]
            if (iteration+(pair=="AB"))%2:choices.reverse()
            # Keep the two fresh processes of one pair adjacent on the host.
            fcntl.flock(lock,fcntl.LOCK_EX)
            try:
                for order,(arm,spec) in enumerate(choices,1):
                    observe(arm,spec,10000,seed_base+iteration,case=name,round=iteration,pair=pair,order=order)
            finally:fcntl.flock(lock,fcntl.LOCK_UN)
        print("completed round",iteration,flush=True)
    for label, mode in diagnostics:
        fcntl.flock(lock,fcntl.LOCK_EX)
        try:observe("diagnostic",(label,mode),1000,20261127,case="submissions",round=0,pair="diagnostic",order=1)
        finally:fcntl.flock(lock,fcntl.LOCK_UN)
