use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output, id as process_id},
    thread,
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
        process_id(),
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

const MISSING_PHASE: &str = "VSDB_ALLOC_MISSING_PHASE";
const MISSING_BASE: &str = "VSDB_ALLOC_MISSING_BASE";
const MISSING_TEST: &str = "missing_ceiling_does_not_reuse_live_prefix";

fn missing_child(phase: &str, base: &Path) -> Output {
    Command::new(env::current_exe().unwrap())
        .args(["--exact", MISSING_TEST, "--nocapture"])
        .env(MISSING_PHASE, phase)
        .env(MISSING_BASE, base)
        .output()
        .unwrap()
}

fn missing_line<'a>(stdout: &'a str, key: &str) -> &'a str {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .unwrap_or_else(|| panic!("missing {key} in stdout: {stdout}"))
        .trim()
}

/// Deleting the v16 ceiling file must not restart allocation at the
/// floor. The only live map is in a non-default namespace, so recovery
/// has to see that dataset; the floor is the first prefix ever issued.
#[test]
fn missing_ceiling_does_not_reuse_live_prefix() {
    if let Ok(phase) = env::var(MISSING_PHASE) {
        let base = PathBuf::from(env::var(MISSING_BASE).unwrap());
        vsdb_configure(VsdbOptions::new(&base)).unwrap();
        match phase.as_str() {
            "seed" => {
                let ns = Namespace::create().unwrap();
                let mut map = MapxRaw::new_in(&ns);
                map.insert(b"k", b"v");
                let info = Namespace::list().unwrap().pop().unwrap();
                println!("PREFIX={}", map.instance_id().map_id);
                println!("ROOT={}", info.path.display());
                println!("SHARDS={}", info.shards);
            }
            "reopen" => {
                let mut map = MapxRaw::new();
                map.insert(b"k", b"new");
                println!("PREFIX={}", map.instance_id().map_id);
            }
            other => panic!("unknown phase {other}"),
        }
        return;
    }

    let scratch = env::temp_dir().join(format!(
        "vsdb_alloc_missing_{}_{}",
        process_id(),
        rand::random::<u128>()
    ));
    let base = scratch.join("base");
    fs::create_dir_all(&base).unwrap();

    let output = missing_child("seed", &base);
    assert!(output.status.success(), "seed phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let prefix: u64 = missing_line(&stdout, "PREFIX=").parse().unwrap();
    let root = PathBuf::from(missing_line(&stdout, "ROOT="));
    let shards: u64 = missing_line(&stdout, "SHARDS=").parse().unwrap();

    fs::remove_file(base.join("__SYSTEM__/__prefix_ceiling__")).unwrap();

    let output = missing_child("reopen", &base);
    assert!(output.status.success(), "reopen phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let reissued: u64 = missing_line(&stdout, "PREFIX=").parse().unwrap();
    assert!(
        reissued > prefix,
        "reissued {reissued} which is not above the live prefix {prefix}"
    );

    let shard = root
        .join("mmdb")
        .join(format!("shard_{:02}", prefix % shards));
    let db = mmdb::DB::open_read_only_with_options(
        mmdb::DbOptions {
            prefix_len: 8,
            ..Default::default()
        },
        &shard,
    )
    .unwrap();
    let mut key = prefix.to_le_bytes().to_vec();
    key.extend_from_slice(b"k");
    assert_eq!(
        db.get(&key).unwrap().as_deref(),
        Some(&b"v"[..]),
        "live key was overwritten after the ceiling file was removed"
    );

    fs::remove_dir_all(&scratch).unwrap();
}

const KEYLESS_PHASE: &str = "VSDB_ALLOC_KEYLESS_PHASE";
const KEYLESS_BASE: &str = "VSDB_ALLOC_KEYLESS_BASE";
const KEYLESS_PARENT: &str = "VSDB_ALLOC_KEYLESS_PARENT";
const KEYLESS_VALUE_TEST: &str = "missing_ceiling_does_not_reuse_handle_in_value";
const KEYLESS_CUSTOM_TEST: &str = "missing_ceiling_does_not_reuse_handle_in_custom_dir";

fn keyless_child(test: &str, phase: &str, base: &Path, parent_file: &Path) -> Output {
    Command::new(env::current_exe().unwrap())
        .args(["--exact", test, "--nocapture"])
        .env(KEYLESS_PHASE, phase)
        .env(KEYLESS_BASE, base)
        .env(KEYLESS_PARENT, parent_file)
        .output()
        .unwrap()
}

fn keyless_line<'a>(stdout: &'a str, key: &str) -> &'a str {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix(key))
        .unwrap_or_else(|| panic!("missing {key} in stdout: {stdout}"))
        .trim()
}

/// An empty map whose only durable copy is a `VSMAPX01` value inside
/// another map has no key of its own. Deleting the ceiling file must
/// not hand that prefix to the next `MapxRaw::new`.
#[test]
fn missing_ceiling_does_not_reuse_handle_in_value() {
    if let Ok(phase) = env::var(KEYLESS_PHASE) {
        let base = PathBuf::from(env::var(KEYLESS_BASE).unwrap());
        vsdb_configure(VsdbOptions::new(&base)).unwrap();
        match phase.as_str() {
            "seed" => {
                // `new` defers allocation until first use. Give the keeper
                // a key first so the empty child is the highest prefix and
                // is not covered by the keeper's key prefix.
                let mut keeper = MapxRaw::new();
                keeper.insert(b"anchor", b"1");
                let child = MapxRaw::new();
                let child_bytes = postcard::to_allocvec(&child).unwrap();
                keeper.insert(b"child", &child_bytes);
                let parent_file = PathBuf::from(env::var(KEYLESS_PARENT).unwrap());
                fs::write(&parent_file, postcard::to_allocvec(&keeper).unwrap())
                    .unwrap();
                println!("CHILD={}", child.instance_id().map_id);
            }
            "reopen" => {
                // Allocate before restoring. A same-run reservation would
                // hide a prefix the scan never observed.
                let mut fresh = MapxRaw::new();
                fresh.insert(b"k", b"fresh");
                let parent_file = PathBuf::from(env::var(KEYLESS_PARENT).unwrap());
                let parent: MapxRaw =
                    postcard::from_bytes(&fs::read(&parent_file).unwrap()).unwrap();
                let child_bytes = parent.get(b"child").unwrap();
                let mut child: MapxRaw = postcard::from_bytes(&child_bytes).unwrap();
                child.insert(b"k", b"child");
                println!("FRESH={}", fresh.instance_id().map_id);
                println!(
                    "FRESH_SEES={}",
                    fresh.get(b"k").as_deref().unwrap().escape_ascii()
                );
                println!(
                    "CHILD_SEES={}",
                    child.get(b"k").as_deref().unwrap().escape_ascii()
                );
            }
            other => panic!("unknown phase {other}"),
        }
        return;
    }

    let scratch = env::temp_dir().join(format!(
        "vsdb_alloc_keyless_value_{}_{}",
        process_id(),
        rand::random::<u128>()
    ));
    let base = scratch.join("base");
    let parent_file = scratch.join("keeper.handle");
    fs::create_dir_all(&base).unwrap();

    let output = keyless_child(KEYLESS_VALUE_TEST, "seed", &base, &parent_file);
    assert!(output.status.success(), "seed phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let child: u64 = keyless_line(&stdout, "CHILD=").parse().unwrap();

    fs::remove_file(base.join("__SYSTEM__/__prefix_ceiling__")).unwrap();

    let output = keyless_child(KEYLESS_VALUE_TEST, "reopen", &base, &parent_file);
    assert!(output.status.success(), "reopen phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let fresh: u64 = keyless_line(&stdout, "FRESH=").parse().unwrap();
    assert!(
        fresh > child,
        "reissued {fresh}, which is not above the keyless handle {child}"
    );
    assert_eq!(keyless_line(&stdout, "FRESH_SEES="), "fresh");
    assert_eq!(keyless_line(&stdout, "CHILD_SEES="), "child");

    fs::remove_dir_all(&scratch).unwrap();
}

/// The custom directory is the documented bootstrap for top-level
/// handles. An empty map saved only there must survive ceiling loss.
#[test]
fn missing_ceiling_does_not_reuse_handle_in_custom_dir() {
    if let Ok(phase) = env::var(KEYLESS_PHASE) {
        let base = PathBuf::from(env::var(KEYLESS_BASE).unwrap());
        vsdb_configure(VsdbOptions::new(&base)).unwrap();
        match phase.as_str() {
            "seed" => {
                let child = MapxRaw::new();
                let child_bytes = postcard::to_allocvec(&child).unwrap();
                let custom = vsdb_core::vsdb_get_custom_dir();
                fs::create_dir_all(custom).unwrap();
                fs::write(custom.join("root"), &child_bytes).unwrap();
                println!("CHILD={}", child.instance_id().map_id);
            }
            "reopen" => {
                // Allocate before restoring. A same-run reservation would
                // hide a prefix the scan never observed.
                let mut fresh = MapxRaw::new();
                fresh.insert(b"k", b"fresh");
                let custom = vsdb_core::vsdb_get_custom_dir();
                let mut child: MapxRaw =
                    postcard::from_bytes(&fs::read(custom.join("root")).unwrap())
                        .unwrap();
                child.insert(b"k", b"child");
                println!("FRESH={}", fresh.instance_id().map_id);
                println!(
                    "FRESH_SEES={}",
                    fresh.get(b"k").as_deref().unwrap().escape_ascii()
                );
                println!(
                    "CHILD_SEES={}",
                    child.get(b"k").as_deref().unwrap().escape_ascii()
                );
            }
            other => panic!("unknown phase {other}"),
        }
        return;
    }

    let scratch = env::temp_dir().join(format!(
        "vsdb_alloc_keyless_custom_{}_{}",
        process_id(),
        rand::random::<u128>()
    ));
    let base = scratch.join("base");
    fs::create_dir_all(&base).unwrap();

    let output = keyless_child(KEYLESS_CUSTOM_TEST, "seed", &base, &base);
    assert!(output.status.success(), "seed phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let child: u64 = keyless_line(&stdout, "CHILD=").parse().unwrap();

    fs::remove_file(base.join("__SYSTEM__/__prefix_ceiling__")).unwrap();

    let output = keyless_child(KEYLESS_CUSTOM_TEST, "reopen", &base, &base);
    assert!(output.status.success(), "reopen phase failed: {output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let fresh: u64 = keyless_line(&stdout, "FRESH=").parse().unwrap();
    assert!(
        fresh > child,
        "reissued {fresh}, which is not above the custom-dir handle {child}"
    );
    assert_eq!(keyless_line(&stdout, "FRESH_SEES="), "fresh");
    assert_eq!(keyless_line(&stdout, "CHILD_SEES="), "child");

    fs::remove_dir_all(&scratch).unwrap();
}

const NEAR_MAX_PHASE: &str = "VSDB_ALLOC_NEAR_MAX_PHASE";
const NEAR_MAX_BASE: &str = "VSDB_ALLOC_NEAR_MAX_BASE";
const NEAR_MAX_ARG: &str = "VSDB_ALLOC_NEAR_MAX_ARG";
const NEAR_MAX_TEST: &str = "near_u64_max_ceiling_does_not_wrap_or_reissue_prefixes";

fn near_max_child(phase: &str, base: &Path, arg: &str) -> Output {
    Command::new(env::current_exe().unwrap())
        .args(["--exact", NEAR_MAX_TEST, "--nocapture"])
        .env(NEAR_MAX_PHASE, phase)
        .env(NEAR_MAX_BASE, base)
        .env(NEAR_MAX_ARG, arg)
        .output()
        .unwrap()
}

/// Regression: ceilings near `u64::MAX` must saturate at `u64::MAX`
/// without wrapping `GLOBAL_CEILING` or `GLOBAL_COUNTER`, must keep
/// existing handles restorable, must persist a ceiling strictly above
/// every issued prefix across multi-thread windows, and must panic
/// cleanly once `u64::MAX - 1` is exhausted instead of wrapping.
#[test]
fn near_u64_max_ceiling_does_not_wrap_or_reissue_prefixes() {
    if let Ok(phase) = env::var(NEAR_MAX_PHASE) {
        let base = PathBuf::from(env::var(NEAR_MAX_BASE).unwrap());
        let arg = env::var(NEAR_MAX_ARG).unwrap();
        vsdb_configure(VsdbOptions::new(&base)).unwrap();
        match phase.as_str() {
            "seed" => {
                let mut early = MapxRaw::new();
                early.insert(b"k", b"early");
                let id = early.save_meta().unwrap();
                Namespace::default_ns().flush();
                println!("EARLY={}", id.map_id);
            }
            "two_windows" => {
                let early_id: u64 = arg.parse().unwrap();
                let early = MapxRaw::from_meta(early_id).unwrap();
                assert_eq!(early.get(b"k").as_deref(), Some(&b"early"[..]));

                let t1 = thread::spawn(|| {
                    let mut m = MapxRaw::new();
                    m.insert(b"k", b"t1");
                    let id = m.save_meta().unwrap();
                    assert_eq!(
                        MapxRaw::from_meta(id).unwrap().get(b"k").as_deref(),
                        Some(&b"t1"[..])
                    );
                    id.map_id
                });
                let p1 = t1.join().unwrap();
                let t2 = thread::spawn(|| {
                    let mut m = MapxRaw::new();
                    m.insert(b"k", b"t2");
                    let id = m.save_meta().unwrap();
                    assert_eq!(
                        MapxRaw::from_meta(id).unwrap().get(b"k").as_deref(),
                        Some(&b"t2"[..])
                    );
                    id.map_id
                });
                let p2 = t2.join().unwrap();
                Namespace::default_ns().flush();
                println!("P1={p1}");
                println!("P2={p2}");
            }
            "after_two_windows" => {
                let (early_s, rest) = arg.split_once(',').unwrap();
                let (p1_s, p2_s) = rest.split_once(',').unwrap();
                let early_id: u64 = early_s.parse().unwrap();
                let p1: u64 = p1_s.parse().unwrap();
                let p2: u64 = p2_s.parse().unwrap();

                let early = MapxRaw::from_meta(early_id).unwrap();
                let m1 = MapxRaw::from_meta(p1).unwrap();
                let m2 = MapxRaw::from_meta(p2).unwrap();
                assert_eq!(early.get(b"k").as_deref(), Some(&b"early"[..]));
                assert_eq!(m1.get(b"k").as_deref(), Some(&b"t1"[..]));
                assert_eq!(m2.get(b"k").as_deref(), Some(&b"t2"[..]));

                // The persisted ceiling saturated at `u64::MAX`, so the
                // restarted process must refuse further allocation rather
                // than re-issuing `p2` or wrapping to reserved ids.
                let res = std::panic::catch_unwind(|| MapxRaw::new().instance_id());
                assert!(
                    res.is_err(),
                    "restarted process at u64::MAX ceiling must panic"
                );
            }
            "exhaust" => {
                let early_id: u64 = arg.parse().unwrap();
                let early = MapxRaw::from_meta(early_id).unwrap();
                assert_eq!(early.get(b"k").as_deref(), Some(&b"early"[..]));

                // Ceiling was seeded at u64::MAX - 2: prefixes u64::MAX - 2
                // and u64::MAX - 1 are the final two valid prefixes; u64::MAX
                // itself is the exclusive ceiling sentinel and must not be issued.
                let a = MapxRaw::new();
                let b = MapxRaw::new();
                assert_eq!(a.instance_id().map_id, u64::MAX - 2);
                assert_eq!(b.instance_id().map_id, u64::MAX - 1);
                let _ = a.save_meta().unwrap();
                let _ = b.save_meta().unwrap();
                assert!(MapxRaw::from_meta(u64::MAX - 2).is_ok());
                assert!(MapxRaw::from_meta(u64::MAX - 1).is_ok());

                let res = std::panic::catch_unwind(|| MapxRaw::new().instance_id());
                assert!(res.is_err(), "allocating at u64::MAX must panic");
            }
            other => panic!("unknown phase {other}"),
        }
        return;
    }

    let scratch = env::temp_dir().join(format!(
        "vsdb_alloc_near_max_{}_{}",
        process_id(),
        rand::random::<u128>()
    ));
    let base = scratch.join("base");
    fs::create_dir_all(&base).unwrap();

    let out = near_max_child("seed", &base, "");
    assert!(out.status.success(), "seed failed: {out:?}");
    let early = keyless_line(&String::from_utf8_lossy(&out.stdout), "EARLY=").to_owned();

    let ceiling_path = base.join("__SYSTEM__/__prefix_ceiling__");
    fs::write(&ceiling_path, (u64::MAX - 20_000).to_le_bytes()).unwrap();

    let out = near_max_child("two_windows", &base, &early);
    assert!(out.status.success(), "two_windows failed: {out:?}");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let p1: u64 = keyless_line(&stdout, "P1=").parse().unwrap();
    let p2: u64 = keyless_line(&stdout, "P2=").parse().unwrap();
    let persisted =
        u64::from_le_bytes(fs::read(&ceiling_path).unwrap().try_into().unwrap());
    assert!(
        persisted > p1 && persisted > p2,
        "persisted ceiling {persisted} did not advance above issued {p1}, {p2}"
    );

    let out = near_max_child("after_two_windows", &base, &format!("{early},{p1},{p2}"));
    assert!(out.status.success(), "after_two_windows failed: {out:?}");

    fs::write(&ceiling_path, (u64::MAX - 2).to_le_bytes()).unwrap();
    let out = near_max_child("exhaust", &base, &early);
    assert!(out.status.success(), "exhaust failed: {out:?}");

    fs::remove_dir_all(&scratch).unwrap();
}
