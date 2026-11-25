//! Offline physical conversion. The only public rename publishes a complete
//! directory containing the base and all explicitly placed namespace roots.
use super::{
    HEADER, PROGRESS, Plan, REG, Record, Registry, State, migrate_universe, mmdb_err,
    physical_path, read_id, registry,
};
use crate::corrupt;
use sha3::{Digest, Keccak256};
use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};
use vsdb::{
    Result, VsdbError,
    common::{atomic_write_file, error::StorageErrorKind},
};

fn v5_error(error: mmdb::Error) -> VsdbError {
    use mmdb::ErrorKind;
    let kind = match error.kind() {
        ErrorKind::Busy => StorageErrorKind::Busy,
        ErrorKind::Io => StorageErrorKind::Io,
        ErrorKind::Corruption => StorageErrorKind::Corruption,
        ErrorKind::ReadOnly => StorageErrorKind::ReadOnly,
        ErrorKind::InvalidArgument => StorageErrorKind::InvalidArgument,
        ErrorKind::DbClosed => StorageErrorKind::DbClosed,
        ErrorKind::Background => StorageErrorKind::Background,
        _ => StorageErrorKind::Other,
    };
    VsdbError::Storage {
        kind,
        source: Box::new(error),
    }
}

fn shard_options() -> mmdb::DbOptions {
    mmdb::DbOptions::default()
        .with_num_levels(7)
        .with_prefix_len(8)
}

// Hash the logical stream in sorted order. Separate lengths distinguish
// (ab, c) from (a, bc), and retain an independent check on the row count.
#[derive(Default)]
struct Rows {
    count: u64,
    hash: Keccak256,
}

impl Rows {
    fn observe(&mut self, key: &[u8], value: &[u8]) {
        self.count += 1;
        for bytes in [key, value] {
            self.hash.update((bytes.len() as u64).to_le_bytes());
            self.hash.update(bytes);
        }
    }
}

fn numbered(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit())
}

fn bootstrap_file(name: &str) -> bool {
    name == "LOCK"
        || name == "CURRENT.tmp"
        || name.strip_prefix("CURRENT.tmp.").is_some_and(numbered)
        || name.strip_prefix("MANIFEST-").is_some_and(numbered)
}

fn engine_file(name: &str) -> bool {
    name == "CURRENT"
        || bootstrap_file(name)
        || name.strip_suffix(".wal").is_some_and(numbered)
        || name.strip_suffix(".sst").is_some_and(numbered)
}

fn export_shard(source: &Path, staged_copy: &Path) -> Result<()> {
    let v4 = mmdb_v4::DB::open_read_only_with_options(
        mmdb_v4::DbOptions {
            num_levels: 7,
            prefix_len: 8,
            ..Default::default()
        },
        source,
    )
    .map_err(mmdb_err)?;
    let temp = tempfile::tempdir_in(staged_copy.parent().expect("a shard has a parent"))?;
    let target = mmdb::DB::open(shard_options(), temp.path()).map_err(v5_error)?;
    let mut expected = Rows::default();
    let mut stream = v4
        .iter_with_options(&mmdb_v4::ReadOptions {
            fill_cache: false,
            ..Default::default()
        })
        .map_err(mmdb_err)?;
    let ingested = target
        .ingest_sorted(stream.by_ref().inspect(|(k, v)| expected.observe(k, v)))
        .map_err(v5_error)?;
    if let Some(error) = stream.error() {
        return Err(corrupt(error));
    }
    drop(stream);
    v4.close().map_err(mmdb_err)?;
    target.close().map_err(v5_error)?;
    drop(target);

    // The read-only v5 opener adopts the geometry stored in the MANIFEST.
    // Writable reopen checks the levels and prefix actually used by VSDB.
    let reopened = mmdb::DB::open(shard_options().with_create_if_missing(false), temp.path())
        .map_err(v5_error)?;
    let mut actual = Rows::default();
    let mut check = reopened.iter().map_err(v5_error)?;
    for (key, value) in check.by_ref() {
        actual.observe(&key, &value);
    }
    if let Some(error) = check.error() {
        return Err(corrupt(error.to_string()));
    }
    drop(check);
    reopened.close().map_err(v5_error)?;
    if ingested != expected.count
        || actual.count != expected.count
        || actual.hash.finalize() != expected.hash.finalize()
    {
        return Err(corrupt(format!(
            "row verification failed at {}",
            source.display()
        )));
    }

    // This is a private copy. Until the whole generation is published,
    // failure or interruption leaves the source and final path untouched.
    // Preserve application sidecars even when they live next to a shard's
    // physical database files. Replacing engine files resets sequence history.
    for entry in fs::read_dir(staged_copy)? {
        let entry = entry?;
        let name = entry.file_name();
        if !name.to_str().is_some_and(engine_file) {
            renamest::rename_exclusive(entry.path(), temp.path().join(name))?;
        }
    }
    fs::remove_dir_all(staged_copy)?;
    fs::rename(temp.path(), staged_copy)?;
    let _ = temp.keep();
    Ok(())
}

fn namespace_path(base: &Path, record: &Record) -> PathBuf {
    record.path.as_ref().map(PathBuf::from).unwrap_or_else(|| {
        base.join("__NAMESPACES__")
            .join(format!("{:016x}", record.id))
    })
}

fn staged_placement(generation: &Path, id: u64) -> PathBuf {
    generation.join("placements").join(format!("{id:016x}"))
}

fn bind_placements(base: &Path, generation: &Path, plan: &mut Plan) -> Result<()> {
    if !plan.entries.iter().any(|(r, _)| r.path.is_some()) {
        return Ok(());
    }
    for (record, _) in &mut plan.entries {
        if record.path.is_some() {
            let path = staged_placement(generation, record.id);
            record.path = Some(
                path.into_os_string()
                    .into_string()
                    .map_err(|_| corrupt("explicit namespace placement requires a UTF-8 path"))?,
            );
        }
    }
    let path = base.join(REG);
    // An interrupted logical conversion can already have the v18 registry,
    // even if the universe header has yet to be installed.
    let mut bytes = Vec::new();
    if fs::read(&path)?.starts_with(b"VSNREG02") {
        bytes.extend_from_slice(b"VSNREG02");
        postcard::to_io(plan, &mut bytes)?;
    } else {
        postcard::to_io(
            &Registry {
                next_id: plan.next_id,
                entries: plan.entries.iter().map(|(r, _)| r).collect::<Vec<_>>(),
            },
            &mut bytes,
        )?;
    }
    atomic_write_file(&path, &bytes)
}

fn lock_existing(path: &Path, locks: &mut Vec<File>) -> Result<()> {
    let lock = match File::open(path) {
        Ok(lock) => lock,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    lock.try_lock_shared().map_err(io::Error::from)?;
    locks.push(lock);
    Ok(())
}

fn copy_tree(source: &Path, target: &Path) -> Result<()> {
    fs::create_dir(target)?;
    let mut dirs = vec![(source.to_owned(), target.to_owned())];
    while let Some((src, dst)) = dirs.pop() {
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            let path = dst.join(entry.file_name());
            if kind.is_dir() {
                fs::create_dir(&path)?;
                dirs.push((entry.path(), path));
            } else if kind.is_file() {
                fs::copy(entry.path(), path)?;
            } else {
                return Err(corrupt(format!(
                    "stage requires regular files and directories: {}",
                    entry.path().display()
                )));
            }
        }
    }
    Ok(())
}

fn sync_generation(root: &Path) -> Result<()> {
    let mut dirs = vec![root.to_owned()];
    let mut next = 0;
    while next < dirs.len() {
        for entry in fs::read_dir(&dirs[next])? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                dirs.push(entry.path());
            } else {
                File::open(entry.path())?.sync_all()?;
            }
        }
        next += 1;
    }
    for dir in dirs.iter().rev() {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

fn overlaps(a: &Path, b: &Path) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

fn reset_unanchored_copy(path: &Path) -> Result<()> {
    // A pending MMDB create can stop after writing LOCK, a MANIFEST, or the
    // CURRENT temporary. WAL/SST data without its anchor is ambiguous.
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !entry.file_type()?.is_file() || !bootstrap_file(&name) {
            return Err(corrupt(format!(
                "unanchored shard contains unexpected data: {}",
                entry.path().display()
            )));
        }
        fs::remove_file(entry.path())?;
    }
    Ok(())
}

/// Copy a quiesced v17/v4 or v18/v4 source into a new, verified generation.
///
/// Configure v18 with `destination.join("base")` and complete any
/// application-specific v17 handle conversions there before accepting writes. Explicit namespace IDs keep
/// their identity and use absolute paths under `destination/placements`.
/// The destination's parent must exist. It may be on a different filesystem
/// from the sources. Publication uses an exclusive rename on that filesystem.
/// A retry after an interrupted unpublished conversion can use the same final
/// name. A destination already published by any process is preserved.
pub fn stage_universe(source: impl AsRef<Path>, destination: impl AsRef<Path>) -> Result<()> {
    stage(source.as_ref(), destination.as_ref(), |_| Ok(()))
}

#[derive(Clone, Copy)]
enum Step {
    Shard,
    Publish,
    Renamed,
}

fn stage(
    source: &Path,
    destination: &Path,
    mut completed: impl FnMut(Step) -> Result<()>,
) -> Result<()> {
    let source = source.canonicalize()?;
    let destination = std::path::absolute(destination)?;
    let name = destination
        .file_name()
        .ok_or_else(|| corrupt("expected generation directory name"))?;
    let parent = destination
        .parent()
        .ok_or_else(|| corrupt("expected generation parent"))?
        .canonicalize()?;
    let destination = parent.join(name);
    match fs::symlink_metadata(&destination) {
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "generation destination exists",
            )
            .into());
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let mut locks = Vec::new();
    lock_existing(&source, &mut locks)?;
    lock_existing(&source.join("__SYSTEM__/LOCK"), &mut locks)?;
    let mut plan = registry(
        &source,
        read_id(&source.join(HEADER))?.is_some(),
        read_id(&source.join(PROGRESS))?.is_some(),
    )?;
    let mut roots = vec![(source.clone(), PathBuf::from("base"), 16u32)];
    for (record, state) in &plan.entries {
        let root = physical_path(&namespace_path(&source, record))?;
        if *state == State::Established && !root.is_dir() {
            return Err(corrupt(format!(
                "established namespace {} has no root",
                record.id
            )));
        }
        let relative = if record.path.is_some() {
            staged_placement(Path::new(""), record.id)
        } else {
            namespace_path(Path::new("base"), record)
        };
        roots.push((root, relative, record.shards));
    }
    for (root, _, shards) in &roots {
        if overlaps(root, &destination) {
            return Err(corrupt("source and destination generation overlap"));
        }
        for i in 0..*shards {
            lock_existing(&root.join(format!("mmdb/shard_{i:02}/LOCK")), &mut locks)?;
        }
    }
    let mut staging = tempfile::Builder::new();
    staging.prefix(".vsdb-stage-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        staging.permissions(fs::Permissions::from_mode(0o700));
    }
    let temp = staging.tempdir_in(&parent)?;
    let base = temp.path().join("base");
    copy_tree(&source, &base)?;
    for (record, _) in &plan.entries {
        if record.path.is_some() {
            let src = namespace_path(&source, record);
            let placements = temp.path().join("placements");
            fs::create_dir_all(placements)?;
            if src.try_exists()? {
                copy_tree(&src, &staged_placement(temp.path(), record.id))?;
            }
        }
    }
    bind_placements(&base, temp.path(), &mut plan)?;
    migrate_universe(&base)?;
    for (src, relative, shards) in &roots {
        let dst = temp.path().join(relative);
        for n in 0..*shards {
            let relative = format!("mmdb/shard_{n:02}");
            let copy = dst.join(&relative);
            if copy.join("CURRENT").is_file() {
                export_shard(&src.join(relative), &copy)?;
                completed(Step::Shard)?;
            } else if copy.try_exists()? {
                reset_unanchored_copy(&copy)?;
            }
        }
    }
    bind_placements(&base, &destination, &mut plan)?;
    sync_generation(temp.path())?;
    completed(Step::Publish)?;
    renamest::rename_exclusive(temp.path(), &destination)?;
    let _ = temp.keep();
    completed(Step::Renamed)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, process::Command};

    fn old_tree(root: &Path) {
        let files: Vec<(&str, &[u8])> =
            postcard::from_bytes(include_bytes!("../../tests/fixtures/tree-v17.postcard")).unwrap();
        for (name, bytes) in files {
            let path = root.join(name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
    }

    #[test]
    fn interruption_never_exposes_an_unverified_generation() {
        const PROBE: &str = "VSDB_STAGE_INTERRUPTION_FIXTURE";
        if let Some(base) = env::var_os(PROBE) {
            let base = PathBuf::from(base);
            let mut shards = 0;
            let _ = stage(&base.join("v17"), &base.join("v5"), |step| {
                if matches!(step, Step::Shard) {
                    shards += 1;
                    if shards == 2 {
                        std::process::exit(82);
                    }
                }
                Ok(())
            });
            panic!("did not reach the second shard");
        }
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("v17");
        let generation = tmp.path().join("v5");
        old_tree(&source);
        fs::write(source.join("mmdb/shard_00/operator-note"), b"checkpoint").unwrap();
        let exit = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "universe::staging::tests::interruption_never_exposes_an_unverified_generation",
            ])
            .env(PROBE, tmp.path())
            .status()
            .unwrap();
        assert_eq!(exit.code(), Some(82));
        assert!(!generation.exists());
        assert!(!source.join(HEADER).exists());
        stage_universe(&source, &generation).unwrap();
        assert!(generation.join("base").join(HEADER).is_file());
        assert_eq!(
            fs::read(generation.join("base/mmdb/shard_00/operator-note")).unwrap(),
            b"checkpoint"
        );
        assert!(!source.join(HEADER).exists());
    }

    #[test]
    fn competing_and_unconfirmed_publication_preserve_each_visible_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let source = tmp.path().join("v17");
        let generation = tmp.path().join("competing");
        old_tree(&source);
        let result = stage(&source, &generation, |step| {
            if matches!(step, Step::Publish) {
                fs::create_dir(&generation)?;
                fs::write(generation.join("owner"), b"competitor")?;
            }
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(fs::read(generation.join("owner")).unwrap(), b"competitor");
        assert!(!generation.join("base").exists());

        let published = tmp.path().join("unconfirmed");
        assert!(
            stage(&source, &published, |step| {
                if matches!(step, Step::Renamed) {
                    return Err(io::Error::other("injected failure before parent sync").into());
                }
                Ok(())
            })
            .is_err()
        );
        assert!(published.join("base").join(HEADER).is_file());
        assert!(stage_universe(&source, &published).is_err());
        let shard = published.join("base/mmdb/shard_00");
        let db = mmdb::DB::open(shard_options().with_create_if_missing(false), shard).unwrap();
        db.close().unwrap();
        assert!(!source.join(HEADER).exists());
    }
}
