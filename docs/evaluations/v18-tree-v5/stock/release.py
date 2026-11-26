"""Verify the archived stock v18 tree comparison on the two MMDB backends."""
import argparse
import csv
import json
from pathlib import Path
from statistics import median

SAMPLES = {"hit": 60_000, "miss": 60_000, "scan": 10_000}
ROLES = {"AA1": "v4", "AA2": "v4", "AB1": "v4", "AB2": "v5"}


def schedule():
    for repeat in range(1, 6):
        order = list(ROLES)
        if repeat % 2 == 0:
            order.reverse()
        for position, arm in enumerate(order, 1):
            yield repeat, 20290113 + repeat * 1021, position, arm


def summarize(args):
    meta = json.loads(args.host.read_text())
    hashes = meta["binary_sha256"]
    if set(hashes) != {"v4", "v5"} or len(set(hashes.values())) != 2:
        raise ValueError("expected distinct v4 and final v5 binaries")
    if any(len(hashes[k]) != 64 for k in hashes):
        raise ValueError("invalid SHA-256 in host receipt")
    expected = {(i, op, arm): (seed, position) for i, seed, position, arm in schedule() for op in SAMPLES}
    rows = {}
    with args.raw.open() as src:
        for r in csv.DictReader(src):
            i, op, arm = int(r["round"]), r["operation"], r["arm"]
            key = i, op, arm
            count, p99, wall = (int(r[k]) for k in ("completed", "p99_ns", "wall_ns"))
            if (key in rows or expected.get(key) != (int(r["seed"]), int(r["position"]))
                    or count != SAMPLES.get(op) or min(wall, p99) <= 0
                    or r["binary"] != ROLES.get(arm) or r["sha256"] != hashes.get(r["binary"])
                    or r["cpu"] != meta["cpu"]):
                raise ValueError(f"unmatched observation: {key}")
            rows[key] = p99, count * 1e9 / wall
    if rows.keys() != expected.keys():
        raise ValueError("incomplete five-pair matrix")
    for op in SAMPLES:
        samples = {arm: [rows[i, op, arm] for i in range(1, 6)] for arm in ROLES}
        def gains(left, right, index):
            return [100 * (1 - b[index] / a[index]) * (1 if index == 0 else -1)
                    for a, b in zip(samples[left], samples[right], strict=True)]
        latency = gains("AB1", "AB2", 0)
        noise = max(map(abs, gains("AA1", "AA2", 0)))
        print(op, "median p99 ns v4/v5", *(round(median(v[0] for v in samples[k])) for k in ("AB1", "AB2")),
              "paired gain %, min/max", *(round(v, 3) for v in (median(latency), min(latency), max(latency))),
              "favorable", sum(v > 0 for v in latency), "v4 max AA %", round(noise, 3),
              "paired throughput gain %", round(median(gains("AB1", "AB2", 1)), 3),
              "v4 max throughput AA %", round(max(map(abs, gains("AA1", "AA2", 1))), 3))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["summarize"])
    parser.add_argument("raw", type=Path)
    parser.add_argument("host", type=Path)
    summarize(parser.parse_args())
