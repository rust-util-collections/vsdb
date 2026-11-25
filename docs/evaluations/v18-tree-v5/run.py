"""Run five matched A/A and A/B pairs plus one v4 reference per seed."""
import argparse
import csv
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("control", type=Path)
p.add_argument("candidate", type=Path)
p.add_argument("v4", type=Path)
p.add_argument("output", type=Path)
p.add_argument("--scratch", type=Path, required=True)
p.add_argument("--cpu", default="4")
p.add_argument("--lock", type=Path)
args = p.parse_args()
binaries = {"control": args.control.resolve(), "candidate": args.candidate.resolve(), "v4": args.v4.resolve()}
hashes = {arm: hashlib.sha256(path.read_bytes()).hexdigest() for arm, path in binaries.items()}
if len(set(hashes.values())) != 3:
    p.error("candidate, control and v4 binaries must have distinct fingerprints")
args.scratch.mkdir(parents=True, exist_ok=True)
with args.output.open("x") as output:
    table = csv.writer(output, lineterminator="\n")
    table.writerow(("round", "seed", "position", "arm", "binary_sha256", "class",
                    "queries", "p99_ns", "wall_ns", "cpu"))
    for repeat in range(1, 6):
        seed = 20261126 + repeat * 1021
        order = ["A1", "A2", "V4", "B0", "B1"]
        if repeat % 2 == 0:
            order.reverse()
        for position, arm in enumerate(order, 1):
            name = {"V4": "v4", "B1": "candidate"}.get(arm, "control")
            with tempfile.TemporaryDirectory(dir=args.scratch) as tmp:
                cmd = ["taskset", "-c", args.cpu, str(binaries[name]), tmp, str(seed)]
                if args.lock:
                    cmd = ["flock", str(args.lock), *cmd]
                rows = subprocess.check_output(cmd, text=True, env={**os.environ, "VSDB_BASE_DIR": tmp})
            measured = set()
            for line in rows.splitlines():
                op, samples, p99, wall = line.split()
                assert op in ("hit", "miss", "scan") and op not in measured
                assert int(samples) == (10000 if op == "scan" else 60000)
                assert int(p99) > 0 and int(wall) > 0
                measured.add(op)
                table.writerow((repeat, seed, position, arm, hashes[name], op,
                                samples, p99, wall, args.cpu))
            assert measured == {"hit", "miss", "scan"}
            output.flush()
        print("completed round", repeat, flush=True)
