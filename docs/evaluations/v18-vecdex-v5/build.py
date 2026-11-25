"""Build one query arm in a fresh detached worktree, without editing the caller's checkout."""
import argparse
from pathlib import Path
import os
import shutil
import subprocess

here = Path(__file__).resolve().parent
p = argparse.ArgumentParser(description=__doc__)
p.add_argument("repository", type=Path)
p.add_argument("scratch", type=Path)
p.add_argument("revision")
p.add_argument("arm", choices=["v4", "1", "8", "32", "shipped", "cached", "final"])
p.add_argument("--pin-diagnostics", action="store_true")
args = p.parse_args()
store = args.scratch.resolve()
store.mkdir(parents=True, exist_ok=True)
name = args.arm + ("-pins" if args.pin_diagnostics else "")
work = store / ("checkout-" + name)
subprocess.run(["git", "-C", args.repository, "worktree", "add", "--detach", work, args.revision], check=True)
examples = work / "strata/examples"
examples.mkdir(exist_ok=True)
shutil.copyfile(here / "probe.rs", examples / "v18_vecdex_probe.rs")


def replace(path, old, new):
    text = path.read_text()
    if text.count(old) != 1:
        raise ValueError(f"unexpected source for {path.name}: {old}")
    path.write_text(text.replace(old, new))


if args.arm == "v4":
    # Only the graph builder draws levels. Every measured query reads the same
    # immutable graph rows, including the exact saved adjacency and state.
    replace(work / "strata/src/vecdex/hnsw.rs", "    let r: f64 = rand::random();", """    static HEIGHT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(472001);
    let mut v = HEIGHT.fetch_add(0x9e37_79b9_7f4a_7c15, std::sync::atomic::Ordering::Relaxed);
    v = (v ^ (v >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    v = (v ^ (v >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    let r = ((v ^ (v >> 31)) >> 11) as f64 / (1u64 << 53) as f64;""")
elif args.arm == "cached":
    subprocess.run(["git", "-C", work, "apply", "--", here/"distance-cache.patch"], check=True)
elif args.arm in ("1", "8"):
    replace(work / "strata/src/vecdex/mod.rs",
            "self.run_with_width::<D, MAX_GROUPED_READ_KEYS>(metric)",
            f"self.run_with_width::<D, {args.arm}>(metric)")

if args.pin_diagnostics:
    if args.arm == "v4":
        raise ValueError("pin diagnostics require v5")
    core = work / "core/src/common/engine/mmdb.rs"
    replace(work / "core/Cargo.toml", "mmdb = { workspace = true }",
            'mmdb = { workspace = true, features = ["test-utils"] }')
    marker = "impl MmdbReadView<'_> {"
    tracker = """static PROFILE_MAX_PIN: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
static PROFILE_VECTOR_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
fn profile_pins<'a>(pins: impl Iterator<Item=&'a mmdb::PinnedSlice>) {
    let mut charges=std::collections::BTreeMap::new();
    for value in pins {
        PROFILE_VECTOR_BYTES.fetch_add(value.len(), std::sync::atomic::Ordering::Relaxed);
        if let Some((id,n))=value.sst_backing_charge() { charges.insert(id,n); }
    }
    PROFILE_MAX_PIN.fetch_max(charges.values().sum(), std::sync::atomic::Ordering::Relaxed);
}
"""
    replace(core, marker, tracker + marker)
    start = core.read_text().index(marker)
    original = core.read_text()
    rest = original[start:]
    needle = "        pin.as_deref().map(decode).transpose()"
    assert rest.count(needle) == 1
    core.write_text(original[:start] + rest.replace(needle,
        "        if key.len() == 9 && key[0] == 0 { profile_pins(pin.iter()); }\n" + needle))
    replace(core, "        for (slot, pin) in pins.iter().enumerate() {",
            "        profile_pins(pins.iter().flatten());\n        for (slot, pin) in pins.iter().enumerate() {")
    marker = '    pub(crate) fn shard_properties(&self, name: &str) -> Vec<Option<String>> {'
    replace(core, marker, marker + '''
        match name {
            "profile.max_pin" => return vec![Some(PROFILE_MAX_PIN.load(std::sync::atomic::Ordering::Relaxed).to_string())],
            "profile.vector_bytes" => return vec![Some(PROFILE_VECTOR_BYTES.load(std::sync::atomic::Ordering::Relaxed).to_string())],
            _ => {}
        }''')
    probe = examples / "v18_vecdex_probe.rs"
    replace(probe, "    latencies.sort_unstable();", '''
    println!("peak_retained_sst_bytes={} projected_vector_bytes={}",property(&ns,"profile.max_pin"),property(&ns,"profile.vector_bytes"));
    latencies.sort_unstable();''')

env = os.environ.copy()
env.setdefault("CARGO_TARGET_DIR", str(store / "build"))
subprocess.run(["cargo", "+stable", "build", "--release", "-p", "vsdb", "--example", "v18_vecdex_probe"],
               cwd=work, env=env, check=True)
shutil.copy2(Path(env["CARGO_TARGET_DIR"]) / "release/examples/v18_vecdex_probe", store / name)
