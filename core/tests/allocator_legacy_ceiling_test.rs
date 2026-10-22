use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use vsdb_core::{MapxRaw, Namespace, VsdbOptions, vsdb_configure};

const PHASE: &str = "VSDB_ALLOC_LEGACY_PHASE";
const BASE: &str = "VSDB_ALLOC_LEGACY_BASE";
const TEST: &str = "legacy_ceiling_folds_before_non_default_allocation";

fn child(phase: &str, base: &Path) -> Output {
    Command::new(env::current_exe().unwrap())
        .args(["--exact", TEST, "--nocapture"])
        .env(PHASE, phase)
        .env(BASE, base)
        .output()
        .unwrap()
}

#[test]
fn legacy_ceiling_folds_before_non_default_allocation() {
    if let Ok(phase) = env::var(PHASE) {
        let base = PathBuf::from(env::var(BASE).unwrap());
        match phase.as_str() {
            "default" => {
                vsdb_configure(VsdbOptions::new(&base)).unwrap();
                let _ = Namespace::default_ns();
            }
            "allocate" => {
                vsdb_configure(VsdbOptions::new(&base)).unwrap();
                let ns = Namespace::create().unwrap();
                let mut namespaced = MapxRaw::new_in(&ns);
                namespaced.insert(b"namespaced", b"value");
                let mut default = MapxRaw::new();
                default.insert(b"default", b"value");
                println!("PREFIX={}", default.instance_id().map_id);
            }
            other => panic!("unknown phase {other}"),
        }
        return;
    }

    let scratch = env::temp_dir().join(format!(
        "vsdb_alloc_legacy_{}_{}",
        std::process::id(),
        rand::random::<u128>()
    ));
    let base = scratch.join("base");
    fs::create_dir_all(&base).unwrap();

    let output = child("default", &base);
    assert!(output.status.success(), "default phase failed: {output:?}");

    let ceiling_path = base.join("__SYSTEM__/__prefix_ceiling__");
    let floor = u64::from_le_bytes(fs::read(&ceiling_path).unwrap().try_into().unwrap());
    let legacy = floor + 8192;

    {
        // Simulate a pre-tripwire v15 binary that advanced the legacy
        // shard-0 ceiling after the v16 file was last folded.
        let db = mmdb::DB::open(
            mmdb::DbOptions {
                create_if_missing: false,
                prefix_len: 8,
                ..Default::default()
            },
            base.join("mmdb/shard_00"),
        )
        .unwrap();
        db.put(&[0u8], &legacy.to_le_bytes()).unwrap();
        db.flush().unwrap();
    }

    let output = child("allocate", &base);
    assert!(output.status.success(), "allocate phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let prefix = stdout
        .lines()
        .find_map(|line| line.strip_prefix("PREFIX="))
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or_else(|| panic!("missing PREFIX in stdout: {stdout}"));
    assert!(
        prefix >= legacy,
        "allocator issued {prefix} below the legacy ceiling {legacy}"
    );

    fs::remove_dir_all(&scratch).unwrap();
}
