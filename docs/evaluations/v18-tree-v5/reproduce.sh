#!/usr/bin/env bash
# Build immutable baseline, prototype and historical v4 controls separately.
# Usage: bash reproduce.sh /absolute/path/to/vsdb /absolute/empty/scratch
set -euo pipefail
repo=$(cd "$1" && pwd -P)
work=$(cd "$2" && pwd -P)
evidence="$repo/docs/evaluations/v18-tree-v5"
for arm in v4 control candidate; do
    rev=0ecb57dc61028ad741a24885249075fa4a3cdd0e
    if [[ $arm == v4 ]]; then rev=6ddaed901901910537d0cf280141218380c5665d; fi
    git -C "$repo" worktree add --detach "$work/$arm" "$rev"
    if [[ $arm != v4 ]]; then git -C "$work/$arm" apply "$evidence/oracle.patch"; fi
    if [[ $arm == candidate ]]; then git -C "$work/$arm" apply "$evidence/leaf-header.patch"; fi
    mkdir -p "$work/$arm/strata/examples"
    cp "$evidence/probe.rs" "$work/$arm/strata/examples/profile_tree_leaf.rs"
    (cd "$work/$arm" && CARGO_TARGET_DIR="$work/target-$arm" cargo +stable build --release -p vsdb --example profile_tree_leaf)
    cp "$work/target-$arm/release/examples/profile_tree_leaf" "$work/$arm-probe"
done
python3 -B "$evidence/run.py" "$work/control-probe" "$work/candidate-probe" "$work/v4-probe" "$work/trials.csv" --scratch "$work/dbs" --cpu 4
python3 -B "$evidence/summarize.py" "$work/trials.csv"
