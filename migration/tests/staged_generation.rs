//! Frozen v18 envelopes with a real MMDB v4 SST and a synced residual WAL.
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
use vsdb::{InstanceId, MapxRaw, Namespace, VsdbOptions, vsdb_configure};
use vsdb_migrate::stage_universe;

const PREFIX: u64 = 50_000_000;
const FLOOR: u64 = PREFIX + 2048;
const ID: u128 = 0xd1fd_a294_1c74_414f_86e6_8cf5_693d_5382;

#[derive(Serialize)]
enum Lifecycle {
    Pending,
    Established,
}

fn physical(suffix: &[u8]) -> Vec<u8> {
    [PREFIX.to_le_bytes().as_slice(), suffix].concat()
}

fn child(phase: &str, path: &Path) {
    let result = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "generation_process"])
        .env("VSDB_FIXTURE_PHASE", phase)
        .env("VSDB_FIXTURE_PATH", path)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{phase}: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn generation_process() {
    let Ok(phase) = std::env::var("VSDB_FIXTURE_PHASE") else {
        return;
    };
    let path = PathBuf::from(std::env::var_os("VSDB_FIXTURE_PATH").unwrap());
    if phase == "legacy" {
        for i in [1, 0] {
            let db = mmdb_v4::DB::open(
                mmdb_v4::DbOptions {
                    prefix_len: 8,
                    ..Default::default()
                },
                path.join(format!("mmdb/shard_{i:02}")),
            )
            .unwrap();
            if i == 0 {
                db.put(&physical(b"sst"), b"old").unwrap();
                db.put(&physical(b"flushed-only"), b"from immutable table")
                    .unwrap();
                db.put(&physical(b"removed"), b"hidden").unwrap();
                db.flush().unwrap();
                let mut batch = mmdb_v4::WriteBatch::new();
                batch.delete(&physical(b"removed"));
                batch.put(&physical(b"sst"), b"new");
                batch.put(&physical(b"empty"), b"");
                db.write_with_options(
                    &mmdb_v4::WriteOptions {
                        sync: true,
                        ..Default::default()
                    },
                    batch,
                )
                .unwrap();
                std::process::exit(0);
            }
            db.close().unwrap();
        }
        unreachable!();
    }
    vsdb_configure(VsdbOptions::new(path.join("base"))).unwrap();
    let ns = Namespace::open(1).unwrap();
    assert_eq!(
        ns.path(),
        path.join("placements/0000000000000001")
            .canonicalize()
            .unwrap()
    );
    let mut map = MapxRaw::from_meta(InstanceId::new(PREFIX, 1)).unwrap();
    assert_eq!(map.get(b"sst").as_deref(), Some(b"new".as_slice()));
    assert_eq!(map.get(b"empty"), Some(Vec::new()));
    assert_eq!(
        map.get(b"flushed-only").as_deref(),
        Some(b"from immutable table".as_slice())
    );
    assert_eq!(map.get(b"removed"), None);
    if phase == "first" {
        assert_eq!(map.iter().count(), 3);
        let next = MapxRaw::new_in(&ns);
        assert!(next.instance_id().map_id >= FLOOR);
        map.insert(b"after", b"v5");
        map.try_sync_wal().unwrap();
    } else {
        assert_eq!(map.get(b"after").as_deref(), Some(b"v5".as_slice()));
    }
}

fn fixture(root: &Path) -> (PathBuf, PathBuf) {
    let base = root.join("source");
    let ext = root.join("old-placement");
    let sys = base.join("__SYSTEM__");
    fs::create_dir_all(&sys).unwrap();
    fs::create_dir_all(base.join("__CUSTOM__")).unwrap();
    fs::write(base.join("__CUSTOM__/attachment"), b"application-owned").unwrap();
    fs::write(
        sys.join("universe_header"),
        [
            b"VSUNI018".as_slice(),
            &18u32.to_le_bytes(),
            &ID.to_le_bytes(),
        ]
        .concat(),
    )
    .unwrap();
    fs::write(sys.join("format_version"), b"18").unwrap();
    fs::write(sys.join("__prefix_ceiling__"), FLOOR.to_le_bytes()).unwrap();
    let reg = (
        3u64,
        vec![
            (
                (1u64, Some(ext.to_str().unwrap()), 2u32, Some(64u64), 17u64),
                Lifecycle::Established,
            ),
            ((2u64, None, 2u32, None, 18u64), Lifecycle::Pending),
        ],
    );
    let mut bytes = b"VSNREG02".to_vec();
    postcard::to_io(&reg, &mut bytes).unwrap();
    fs::write(sys.join("__namespaces__"), bytes).unwrap();
    let system = ext.join("__SYSTEM__");
    fs::create_dir_all(system.join("__instance_meta__")).unwrap();
    fs::write(system.join("format_version"), b"18").unwrap();
    fs::write(system.join(".shards_ready"), b"18").unwrap();
    fs::write(
        system.join("engine_header"),
        [
            b"VSENG018".as_slice(),
            &ID.to_le_bytes(),
            &1u64.to_le_bytes(),
            &2u32.to_le_bytes(),
        ]
        .concat(),
    )
    .unwrap();
    fs::write(
        system
            .join("__instance_meta__")
            .join(format!("{PREFIX:016x}")),
        [
            b"VSMAPX02".as_slice(),
            &PREFIX.to_le_bytes(),
            &1u64.to_le_bytes(),
        ]
        .concat(),
    )
    .unwrap();
    fs::create_dir_all(ext.join("mmdb")).unwrap();
    child("legacy", &ext);
    let rows = inventory(&ext.join("mmdb/shard_00"));
    for ext in ["sst", "wal"] {
        assert!(
            rows.iter().any(
                |(path, data)| path.extension().is_some_and(|name| name == ext)
                    && data.as_ref().is_some_and(|bytes| !bytes.is_empty())
            ),
            "missing populated {ext}"
        );
    }
    (base, ext)
}

fn inventory(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    let mut result = BTreeMap::new();
    let mut dirs = vec![root.to_owned()];
    while let Some(dir) = dirs.pop() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            let value = if path.is_dir() {
                dirs.push(path.clone());
                None
            } else {
                Some(fs::read(&path).unwrap())
            };
            result.insert(path.strip_prefix(root).unwrap().to_owned(), value);
        }
    }
    result
}

#[test]
fn external_shards_publish_together_and_the_original_generation_is_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let (base, ext) = fixture(temp.path());
    let original = (inventory(&base), inventory(&ext));
    let output = temp.path().join("v5");
    stage_universe(&base, &output).unwrap();
    assert_eq!(
        fs::read(output.join("base/__CUSTOM__/attachment")).unwrap(),
        b"application-owned"
    );
    assert!(!output.join("base/mmdb/shard_00/CURRENT").exists());
    assert_eq!(
        fs::read(output.join("base/__SYSTEM__/__prefix_ceiling__")).unwrap(),
        FLOOR.to_le_bytes()
    );
    assert!(stage_universe(&base, &output).is_err());
    child("first", &output);
    child("reopen", &output);
    assert_eq!(original, (inventory(&base), inventory(&ext)));
}

#[test]
fn an_unhealthy_established_generation_cannot_be_published() {
    let temp = tempfile::tempdir().unwrap();
    let (base, ext) = fixture(temp.path());
    let output = temp.path().join("v5");
    assert!(stage_universe(&base, ext.join("nested")).is_err());
    let shard = ext.join("mmdb/shard_01");
    let lock = fs::File::open(shard.join("LOCK")).unwrap();
    lock.try_lock().unwrap();
    assert!(stage_universe(&base, &output).is_err());
    assert!(!output.exists());
    drop(lock);
    let old = temp.path().join("protected");
    fs::rename(&shard, &old).unwrap();
    assert!(stage_universe(&base, &output).is_err());
    assert!(!output.exists());
    let different = mmdb::DB::open(mmdb::DbOptions::default().with_prefix_len(8), &shard).unwrap();
    different.close().unwrap();
    drop(different);
    assert!(stage_universe(&base, &output).is_err());
    assert!(!output.exists());
    fs::remove_dir_all(&shard).unwrap();
    fs::rename(old, &shard).unwrap();
    fs::write(shard.join("CURRENT"), b"damaged\n").unwrap();
    assert!(stage_universe(&base, &output).is_err());
    assert!(!output.exists());
}

#[cfg(unix)]
#[test]
fn destination_parent_alias_opens_the_same_registered_namespace() {
    let temp = tempfile::tempdir().unwrap();
    let (source, _) = fixture(temp.path());
    let real = temp.path().join("volume");
    let alias = temp.path().join("local-volume");
    fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    stage_universe(&source, alias.join("v5")).unwrap();
    child("first", &alias.join("v5"));
    child("reopen", &real.join("v5"));
}
