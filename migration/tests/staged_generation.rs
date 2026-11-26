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

#[test]
fn stale_ceiling_rejects_an_external_row_and_an_empty_saved_map() {
    let tmp = tempfile::tempdir().unwrap();
    let (source, external) = fixture(tmp.path());
    let output = tmp.path().join("generation");
    let ceiling = source.join("__SYSTEM__/__prefix_ceiling__");
    let meta_dir = external.join("__SYSTEM__/__instance_meta__");

    // The visible row prefix alone proves the exclusive boundary stale.
    fs::remove_file(meta_dir.join(format!("{PREFIX:016x}"))).unwrap();
    fs::write(&ceiling, PREFIX.to_le_bytes()).unwrap();
    let check = |expected_id: u64| {
        let old_source = inventory(&source);
        let old_external = inventory(&external);
        let error = stage_universe(&source, &output).unwrap_err();
        assert!(matches!(error, vsdb::VsdbError::Corrupt { .. }));
        assert!(
            error.to_string().contains(&expected_id.to_string()),
            "{error}"
        );
        assert!(!output.exists());
        assert_eq!(inventory(&source), old_source);
        assert_eq!(inventory(&external), old_external);
    };
    check(PREFIX);

    // This allocated but empty external map has no row to discover. Its
    // saved instance filename owns its ID, regardless of the payload schema.
    fs::write(&ceiling, FLOOR.to_le_bytes()).unwrap();
    fs::write(meta_dir.join(format!("{FLOOR:016x}")), b"opaque metadata").unwrap();
    check(FLOOR);
}

#[test]
fn stored_ceiling_does_not_treat_opaque_bytes_as_allocated_local_maps() {
    let tmp = tempfile::tempdir().unwrap();
    let (source, external) = fixture(tmp.path());
    let output = tmp.path().join("generation");
    let foreign = [
        b"VSMAPX02".as_slice(),
        &u64::MAX.to_le_bytes(),
        &1u64.to_le_bytes(),
    ]
    .concat();
    let raw = postcard::to_allocvec(&foreign).unwrap();
    fs::write(source.join("__CUSTOM__/foreign-handle"), &raw).unwrap();
    fs::write(
        external
            .join("__SYSTEM__/__instance_meta__")
            .join(format!("{:016x}", PREFIX + 2)),
        &raw,
    )
    .unwrap();
    let db = mmdb_v4::DB::open(
        mmdb_v4::DbOptions {
            prefix_len: 8,
            ..Default::default()
        },
        external.join("mmdb/shard_00"),
    )
    .unwrap();
    db.put(&physical(&raw), &raw).unwrap();
    db.close().unwrap();
    stage_universe(&source, &output).unwrap();
    let floor = fs::read(output.join("base/__SYSTEM__/__prefix_ceiling__")).unwrap();
    assert_eq!(floor, FLOOR.to_le_bytes());
    let db = mmdb::DB::open(
        vsdb_core::common::mmdb_shard_format_options().with_create_if_missing(false),
        output.join("placements/0000000000000001/mmdb/shard_00"),
    )
    .unwrap();
    assert_eq!(db.get(&physical(&raw)).unwrap(), Some(raw));
}

#[test]
fn pending_partial_engine_also_proves_a_stale_allocator_ceiling() {
    let tmp = tempfile::tempdir().unwrap();
    let (source, external) = fixture(tmp.path());
    let pending = source.join("__NAMESPACES__/0000000000000002");
    fs::create_dir_all(pending.join("__SYSTEM__")).unwrap();
    fs::write(
        pending.join("__SYSTEM__/__initializing__"),
        [
            b"VSENG018".as_slice(),
            &ID.to_le_bytes(),
            &2u64.to_le_bytes(),
            &2u32.to_le_bytes(),
        ]
        .concat(),
    )
    .unwrap();
    let db = mmdb_v4::DB::open(
        mmdb_v4::DbOptions {
            prefix_len: 8,
            ..Default::default()
        },
        pending.join("mmdb/shard_00"),
    )
    .unwrap();
    let key = [FLOOR.to_le_bytes().as_slice(), b"pending-row"].concat();
    db.put(&key, b"not yet published").unwrap();
    db.close().unwrap();
    assert!(!pending.join("mmdb/shard_01").exists());
    let source_before = inventory(&source);
    let ext_before = inventory(&external);
    let destination = tmp.path().join("generation");
    let error = stage_universe(&source, &destination).unwrap_err();
    assert!(matches!(error, vsdb::VsdbError::Corrupt { .. }));
    assert!(error.to_string().contains(&FLOOR.to_string()));
    assert!(!destination.exists());
    assert_eq!(inventory(&source), source_before);
    assert_eq!(inventory(&external), ext_before);
}

#[test]
fn legacy_allocator_fold_precedes_v17_ownership_checks_but_cannot_mask_a_stale_v18_file() {
    const NEW_MAP: u64 = 60_000_000;
    let key = [NEW_MAP.to_le_bytes().as_slice(), b"legacy-owner"].concat();
    if let Some(generation) = std::env::var_os("VSDB_LEGACY_FLOOR_REOPEN") {
        vsdb_configure(VsdbOptions::new(PathBuf::from(generation).join("base"))).unwrap();
        let next = MapxRaw::new().instance_id();
        assert_eq!(next.map_id, NEW_MAP + 1);
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("v17");
    let generation = tmp.path().join("v5");
    let files: Vec<(&str, &[u8])> =
        postcard::from_bytes(include_bytes!("fixtures/tree-v17.postcard")).unwrap();
    for (name, bytes) in files {
        let path = input.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }
    assert!(!input.join("__SYSTEM__/universe_header").exists());
    let stored = input.join("__SYSTEM__/__prefix_ceiling__");
    fs::write(&stored, NEW_MAP.to_le_bytes()).unwrap();
    let shard = input.join("mmdb/shard_00");
    let old = mmdb_v4::DB::open(
        mmdb_v4::DbOptions {
            prefix_len: 8,
            ..Default::default()
        },
        &shard,
    )
    .unwrap();
    old.put(&[0], &(NEW_MAP + 1).to_le_bytes()).unwrap();
    old.put(&key, b"preserved").unwrap();
    old.close().unwrap();
    drop(old);
    let before = inventory(&input);
    stage_universe(&input, &generation).unwrap();
    assert_eq!(inventory(&input), before);
    assert_eq!(
        fs::read(generation.join("base/__SYSTEM__/__prefix_ceiling__")).unwrap(),
        (NEW_MAP + 1).to_le_bytes()
    );
    let selected = mmdb::DB::open(
        vsdb_core::common::mmdb_shard_format_options().with_create_if_missing(false),
        generation.join("base/mmdb/shard_00"),
    )
    .unwrap();
    assert_eq!(
        selected.get(&key).unwrap().as_deref(),
        Some(&b"preserved"[..])
    );
    selected.close().unwrap();
    drop(selected);
    let reopened = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "legacy_allocator_fold_precedes_v17_ownership_checks_but_cannot_mask_a_stale_v18_file",
        ])
        .env("VSDB_LEGACY_FLOOR_REOPEN", &generation)
        .output()
        .unwrap();
    assert!(reopened.status.success(), "{reopened:?}");

    // The old physical shard can still contain the one-byte record after the
    // v18 metadata has been committed. The v18 file now owns the ceiling.
    vsdb_migrate::migrate_universe(&input).unwrap();
    assert_eq!(fs::read(&stored).unwrap(), (NEW_MAP + 1).to_le_bytes());
    fs::write(&stored, NEW_MAP.to_le_bytes()).unwrap();
    let before = inventory(&input);
    let rejected = tmp.path().join("rejected");
    assert!(matches!(
        stage_universe(&input, &rejected),
        Err(vsdb::VsdbError::Corrupt { .. })
    ));
    assert!(!rejected.exists());
    assert_eq!(inventory(&input), before);
}

#[test]
fn migrate_and_stage_reject_non_file_current_and_preserve_io_errors() {
    #[cfg(unix)]
    use std::os::unix::fs::{PermissionsExt, symlink};

    let tmp = tempfile::tempdir().unwrap();
    let input = tmp.path().join("v17");
    let files: Vec<(&str, &[u8])> =
        postcard::from_bytes(include_bytes!("fixtures/tree-v17.postcard")).unwrap();
    for (name, bytes) in files {
        let path = input.join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, bytes).unwrap();
    }

    let shard0 = input.join("mmdb/shard_00");
    let current = shard0.join("CURRENT");
    let saved_current = fs::read(&current).unwrap();

    // Symlink CURRENT must be rejected as Corrupt rather than followed.
    #[cfg(unix)]
    {
        let target = shard0.join("CURRENT.real");
        fs::write(&target, &saved_current).unwrap();
        fs::remove_file(&current).unwrap();
        symlink(&target, &current).unwrap();
        assert!(matches!(
            vsdb_migrate::migrate_universe(&input),
            Err(vsdb::VsdbError::Corrupt { .. })
        ));
        fs::remove_file(&current).unwrap();
        fs::remove_file(&target).unwrap();
        fs::write(&current, &saved_current).unwrap();

        // PermissionDenied on shard_00 must surface as VsdbError::Io, not Corrupt.
        let orig_mode = fs::metadata(&shard0).unwrap().permissions().mode();
        fs::set_permissions(&shard0, fs::Permissions::from_mode(0o000)).unwrap();
        if fs::symlink_metadata(&current).is_err() {
            let err = vsdb_migrate::migrate_universe(&input).unwrap_err();
            assert!(matches!(err, vsdb::VsdbError::Io(_)), "got {err:?}");
        }
        fs::set_permissions(&shard0, fs::Permissions::from_mode(orig_mode)).unwrap();
    }
}
