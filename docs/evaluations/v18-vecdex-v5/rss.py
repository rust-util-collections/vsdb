"""Report Linux OS process high-water RSS on shorter, independent diagnostic runs."""
import argparse
import csv
import fcntl
from pathlib import Path
import subprocess
import tempfile
import time


def high_water(pid):
    try:
        lines = (Path("/proc") / pid / "status").read_text().splitlines()
        return max((int(line.split()[1]) for line in lines if line.startswith("VmHWM:")), default=0)
    except (OSError, ValueError):
        return 0


def measure(binary, fixture, regime, seed, samples, cpus):
    with tempfile.TemporaryDirectory() as tmp:
        maximum = Path(tmp) / "max.txt"
        command = ["/usr/bin/time", "-f", "%M", "-o", maximum, "taskset", "-c", cpus,
                   str(binary), "run", str(fixture), regime, str(seed), str(samples)]
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        children = Path(f"/proc/{proc.pid}/task/{proc.pid}/children")
        process_peak = 0
        while proc.poll() is None:
            try:
                for pid in children.read_text().split():
                    process_peak = max(process_peak, high_water(pid))
            except OSError:
                pass
            time.sleep(0.004)
        out, err = proc.communicate()
        if proc.returncode:
            raise RuntimeError(err)
        kernel_peak = int(maximum.read_text())
    if min(kernel_peak, process_peak) <= 0:
        raise ValueError("missing process RSS")
    return kernel_peak, process_peak, dict(field.split("=") for field in out.split())


p = argparse.ArgumentParser(description=__doc__)
p.add_argument("bin_dir", type=Path)
p.add_argument("fixtures", type=Path)
p.add_argument("output", type=Path)
p.add_argument("--lock", required=True, type=Path)
p.add_argument("--cpus", required=True)
p.add_argument("--policy-warm-only", action="store_true")
args = p.parse_args()
bins = args.bin_dir.resolve()
fixtures = args.fixtures.resolve()
fields = ["round", "regime", "arm", "seed", "samples", "gnu_time_peak_kib",
          "sampled_process_vmhwm_kib", "storage_bytes", "io_positive", "digest", "nearest"]
expected = {}
with args.output.open("x") as out, args.lock.open("a") as lock:
    writer = csv.DictWriter(out, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    out.flush()
    for i in range(1, 4):
        for regime in (("warm",) if args.policy_warm_only else ("warm", "device")):
            arms = ["1", "8", "32", "shipped"] + (["v4"] if regime == "warm" else [])
            if args.policy_warm_only:
                arms = ["policy"]
            if i % 2 == 0:
                arms.reverse()
            samples = 500 if regime == "device" else 1000
            seed = 741126 + i
            for arm in arms:
                version = "v4" if arm == "v4" else "v5"
                fcntl.flock(lock, fcntl.LOCK_EX)
                try:
                    peak, seen_peak, obs = measure(bins/arm, fixtures/(version+"-"+regime),
                                                   regime, seed, samples, args.cpus)
                finally:
                    fcntl.flock(lock, fcntl.LOCK_UN)
                if int(obs["samples"]) != samples:
                    raise ValueError("incomplete trace")
                if regime == "warm" and int(obs["storage_bytes"]):
                    raise ValueError("warm resource run touched storage")
                if regime == "device" and int(obs["io_positive"])*100 < samples*95:
                    raise ValueError("disk resource run did not achieve the I/O coverage floor")
                observable = obs["digest"], obs["nearest"]
                if expected.setdefault((regime, seed), observable) != observable:
                    raise ValueError("different public search results")
                writer.writerow(dict(round=i, regime=regime, arm=arm, seed=seed, samples=samples,
                                     gnu_time_peak_kib=peak, sampled_process_vmhwm_kib=seen_peak,
                                     **{k: obs[k] for k in ("storage_bytes","io_positive","digest","nearest")}))
                out.flush()
        print("RSS round", i, flush=True)
