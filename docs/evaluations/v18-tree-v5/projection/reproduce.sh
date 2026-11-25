#!/usr/bin/env bash
# Build each immutable checkout with its own Cargo target directory.
set -euo pipefail
repo=$(cd "$1" && pwd -P)
work=$(cd "$2" && pwd -P)
evidence="$repo/docs/evaluations/v18-tree-v5/projection"
for arm in v4 control candidate; do
    rev=8ce01f07ad48503f20ceba15b90ba8c4116b6747
    if [[ $arm == v4 ]]; then rev=6ddaed901901910537d0cf280141218380c5665d; fi
    git -C "$repo" worktree add --detach "$work/$arm" "$rev"
    if [[ $arm == candidate ]]; then git -C "$work/$arm" apply "$evidence/point-projection.patch"; fi
    examples="$work/$arm/strata/examples"
    mkdir -p "$examples"
    cp "$evidence/probe.rs" "$examples/profile_tree_projection.rs"
    sed -e '/^pub(super) fn run/,$d' -e '/^use super::/d' "$repo/docs/evaluations/v18-adapter/alloc.rs" > "$examples/profile_tree_projection_alloc.rs"
    cat "$evidence/alloc_case.rs" >> "$examples/profile_tree_projection_alloc.rs"
    (cd "$work/$arm" && CARGO_TARGET_DIR="$work/target-$arm" cargo +stable build --release -p vsdb --example profile_tree_projection --example profile_tree_projection_alloc)
    cp "$work/target-$arm/release/examples/profile_tree_projection" "$work/$arm-probe"
    mkdir "$work/alloc-db-$arm"
    taskset -c 4 "$work/target-$arm/release/examples/profile_tree_projection_alloc" "$work/alloc-db-$arm" > "$work/alloc-$arm.txt"
done
python3 -B "$evidence/run.py" "$work/control-probe" "$work/candidate-probe" "$work/v4-probe" "$work/trials.csv" --scratch "$work/dbs"
python3 -B "$evidence/summarize.py" "$work/trials.csv"
