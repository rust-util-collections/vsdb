#!/usr/bin/env bash
# Build the stock adapter revisions in independent Cargo target directories.
set -euo pipefail
repo=$(cd "$1" && pwd -P)
work=$(cd "$2" && pwd -P)
evidence="$repo/docs/evaluations/v18-tree-v5"
mkdir "$work/data"
btrfs property set "$work/data" compression none
for arm in v4 v5; do
    rev=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["source"][sys.argv[2]]["vsdb"])' "$evidence/stock/host.json" "$arm")
    git -C "$repo" worktree add --detach "$work/$arm" "$rev"
    python3 - "$evidence/stock/locks.json.gz" "$arm" "$work/$arm/Cargo.lock" <<'PYLOCK'
import gzip, json, sys
from pathlib import Path
with gzip.open(sys.argv[1], "rt") as stream:
    Path(sys.argv[3]).write_text(json.load(stream)[sys.argv[2]])
PYLOCK
    examples="$work/$arm/strata/examples"
    mkdir -p "$examples"
    cp "$evidence/probe.rs" "$examples/profile_tree_leaf.rs"
    (cd "$work/$arm" && CARGO_TARGET_DIR="$work/target-$arm" cargo +stable build --locked --release -p vsdb --example profile_tree_leaf)
    cp "$work/target-$arm/release/examples/profile_tree_leaf" "$work/$arm-probe"
done
cp "$evidence/stock/geometry.rs" "$work/v5/strata/examples/profile_tree_release_geometry.rs"
(cd "$work/v5" && CARGO_TARGET_DIR="$work/target-v5" cargo +stable run --locked --release -p vsdb --example profile_tree_release_geometry -- "$work/config-check")
python3 -B "$evidence/stock/release.py" run "$work/v4-probe" "$work/v5-probe" "$work/data" "$work/trials.csv" --cpu 4
python3 -B "$evidence/stock/release.py" summarize "$work/trials.csv" "$work/trials.builds.json"
