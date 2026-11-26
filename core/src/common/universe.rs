//! Universe bootstrap and engine identity validation. The header is published
//! last so readers cannot open partially initialized registry or engine records.
use super::{
    OpenMode, PREFIX_ALLOC_START, atomic_write_file, create_dir_all_durable, engine,
    error::{Result, corrupt},
    namespace, vsdb_get_base_dir, vsdb_open_mode,
};
use parking_lot::Mutex;
use std::{
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::OnceLock,
};

const HEADER: &str = "__SYSTEM__/universe_header";
const CEILING: &str = "__SYSTEM__/__prefix_ceiling__";
static OPEN: OnceLock<Universe> = OnceLock::new();
static INIT: Mutex<()> = Mutex::new(());

pub(crate) struct Universe {
    pub id: u128,
    pub ceiling: u64,
    _lock: Option<File>,
}

fn header_bytes(id: u128) -> [u8; 28] {
    let mut bytes = [0; 28];
    bytes[..8].copy_from_slice(b"VSUNI018");
    bytes[8..12].copy_from_slice(&18u32.to_le_bytes());
    bytes[12..].copy_from_slice(&id.to_le_bytes());
    bytes
}
fn read_header(path: &Path) -> Result<Option<u128>> {
    match fs::read(path) {
        Ok(bytes) if bytes.len() == 28 && bytes[..12] == header_bytes(0)[..12] => {
            Ok(Some(u128::from_le_bytes(bytes[12..].try_into().unwrap())))
        }
        Ok(_) => Err(corrupt(format!(
            "invalid universe header at {}",
            path.display()
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
/// Serializes cold-path adoption of an empty directory across universes.
/// Holding its directory descriptor avoids creating a lockfile in a path
/// before verifying that it belongs to us.
pub(super) fn lock_root_dir(root: &Path) -> Result<File> {
    let dir = File::open(root)?;
    dir.try_lock().map_err(io::Error::from)?;
    Ok(dir)
}

fn lock(base: &Path) -> Result<File> {
    let system = base.join("__SYSTEM__");
    create_dir_all_durable(&system)?;
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(system.join("LOCK"))?;
    file.try_lock().map_err(io::Error::from)?;
    Ok(file)
}

fn read_lock(base: &Path) -> Result<Option<File>> {
    match File::open(base.join("__SYSTEM__/LOCK")) {
        Ok(file) => {
            file.try_lock_shared().map_err(io::Error::from)?;
            Ok(Some(file))
        }
        // A read-only mount of an immutable copy may omit lock files.
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            #[cfg(test)]
            tests::after_missing_universe_lock();
            Ok(None)
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn active() -> Result<&'static Universe> {
    if let Some(universe) = OPEN.get() {
        return Ok(universe);
    }
    super::vsdb_freeze_base_dir();
    initialize(&vsdb_get_base_dir(), vsdb_open_mode())
}

/// Validate the root format marker. A writable open fills in a missing marker
/// under the root lock, including on early format-18 roots.
pub(super) fn check_format_marker(root: &Path, writable: bool) -> Result<()> {
    let path = root.join("__SYSTEM__/format_version");
    match fs::read(&path) {
        Ok(bytes) if bytes == b"18" => {
            if writable {
                // A previous attempt may have renamed the marker before its
                // directory fsync failed. Reopening must confirm the fence.
                File::open(path.parent().expect("marker has a parent"))?.sync_all()?;
            }
            Ok(())
        }
        Ok(_) => Err(corrupt(format!(
            "incompatible format marker at {}",
            path.display()
        ))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            if writable {
                atomic_write_file(&path, b"18")?;
            }
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn initialize(base: &Path, mode: OpenMode) -> Result<&'static Universe> {
    let _guard = INIT.lock();
    if let Some(universe) = OPEN.get() {
        return Ok(universe);
    }
    let writable = mode == OpenMode::ReadWrite;
    let _adoption = if writable {
        create_dir_all_durable(base)?;
        Some(lock_root_dir(base)?)
    } else {
        None
    };
    let mut lease = if writable {
        Some(lock(base)?)
    } else {
        read_lock(base)?
    };
    let id = match read_header(&base.join(HEADER))? {
        Some(id) => {
            if !writable && lease.is_none() {
                // The lock file was absent before the header was read. A
                // writer can create and hold it in that window; a second
                // attempt distinguishes that writer from a lockless copy.
                lease = read_lock(base)?;
            }
            namespace::check_registry_at(base)?;
            // A universe can contain only non-default engines.
            validate_ready_engine(base, id, 0, 16)?;
            check_format_marker(base, writable)?;
            id
        }
        None => {
            if !writable || !fresh_or_interrupted(base)? {
                return Err(corrupt(
                    "missing VSUNI018 header in an existing database directory",
                ));
            }
            check_format_marker(base, true)?;
            namespace::bootstrap_registry(base)?;
            atomic_write_file(&base.join(CEILING), &PREFIX_ALLOC_START.to_le_bytes())?;
            let id = rand::random();
            atomic_write_file(&base.join(HEADER), &header_bytes(id))?;
            id
        }
    };
    let ceiling = engine::recover_ceiling(base, id, writable)?;
    Ok(OPEN.get_or_init(|| Universe {
        id,
        ceiling,
        _lock: lease,
    }))
}

fn fresh_or_interrupted(base: &Path) -> Result<bool> {
    if base.join("mmdb").try_exists()?
        || base.join("__NAMESPACES__").try_exists()?
        || base.join("__CUSTOM__").try_exists()?
    {
        return Ok(false);
    }
    for entry in fs::read_dir(base.join("__SYSTEM__"))? {
        let name = entry?.file_name();
        let Some(name) = name.to_str() else {
            return Ok(false);
        };
        if !matches!(
            name,
            "LOCK" | "format_version" | "__namespaces__" | "__prefix_ceiling__"
        ) && !name.ends_with(".tmp")
        {
            return Ok(false);
        }
    }
    namespace::bootstrap_files_valid(base)
}
fn engine_header(root: &Path, ns: u64) -> PathBuf {
    if ns == 0 {
        root.join("mmdb/__SYSTEM__/engine_header")
    } else {
        root.join("__SYSTEM__/engine_header")
    }
}
pub(super) fn engine_bytes(id: u128, ns: u64, shards: usize) -> [u8; 36] {
    let mut bytes = [0; 36];
    bytes[..8].copy_from_slice(b"VSENG018");
    bytes[8..24].copy_from_slice(&id.to_le_bytes());
    bytes[24..32].copy_from_slice(&ns.to_le_bytes());
    bytes[32..].copy_from_slice(&(shards as u32).to_le_bytes());
    bytes
}
// The caller has already established that this particular lifecycle record
// is required. A genuinely absent pending initializer remains a separate
// cold-start state in validate_ready_engine.
fn require_engine_record(path: &Path, expected: &[u8], name: &str) -> Result<()> {
    match fs::read(path) {
        Ok(bytes) if bytes == expected => Ok(()),
        Ok(_) => Err(corrupt(format!("invalid {name} at {}", path.display()))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            Err(corrupt(format!("missing {name} at {}", path.display())))
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn verify_engine(
    root: &Path,
    id: u128,
    ns: u64,
    shards: usize,
) -> Result<()> {
    check_identity(root, id, ns, shards)?;
    require_engine_record(
        &root.join("__SYSTEM__/.shards_ready"),
        b"18",
        "shard-ready marker",
    )?;
    engine::validate_completed_dataset(root, shards)
}
pub(super) fn check_identity(
    root: &Path,
    id: u128,
    ns: u64,
    shards: usize,
) -> Result<()> {
    require_engine_record(
        &engine_header(root, ns),
        &engine_bytes(id, ns, shards),
        "engine identity",
    )
}
/// Check a registry claim before opening or deleting this pending root.
pub(super) fn validate_pending_root(
    root: &Path,
    id: u128,
    ns: u64,
    shards: usize,
) -> Result<()> {
    if engine_header(root, ns).try_exists()? {
        return check_identity(root, id, ns, shards);
    }
    let sentinel = root.join("__SYSTEM__/__initializing__");
    if sentinel.try_exists()? {
        return check_initializer(&sentinel, id, ns, shards);
    }
    // A crash can leave the empty system directory or a staging sibling
    // before the initializer's atomic rename. No other files are ours yet.
    let expected = engine_bytes(id, ns, shards);
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if entry.file_name() != "__SYSTEM__" || !entry.file_type()?.is_dir() {
            return Err(corrupt(format!(
                "unclaimed nonempty namespace root {}",
                root.display()
            )));
        }
        for file in fs::read_dir(entry.path())? {
            let file = file?;
            let name = file.file_name();
            let name = name.to_string_lossy();
            let is_stage = name
                .strip_prefix("__initializing__.")
                .and_then(|s| s.strip_suffix(".tmp"))
                .and_then(|s| s.split_once('.'))
                .is_some_and(|(pid, n)| {
                    pid.parse::<u32>().is_ok() && n.parse::<u64>().is_ok()
                });
            if !is_stage || !file.file_type()?.is_file() {
                return Err(corrupt("foreign file in pending namespace root"));
            }
            let staged = fs::read(file.path())?;
            if staged.len() > expected.len() || !expected.starts_with(&staged) {
                return Err(corrupt(
                    "foreign initializer staging file in pending namespace root",
                ));
            }
        }
    }
    Ok(())
}
fn check_initializer(path: &Path, id: u128, ns: u64, shards: usize) -> Result<()> {
    require_engine_record(path, &engine_bytes(id, ns, shards), "engine initializer")
}
pub(super) fn validate_ready_engine(
    root: &Path,
    id: u128,
    ns: u64,
    shards: usize,
) -> Result<()> {
    let ready = root.join("__SYSTEM__/.shards_ready");
    let pending = root.join("__SYSTEM__/__initializing__");
    if ready.try_exists()? {
        verify_engine(root, id, ns, shards)?;
    } else if engine_header(root, ns).try_exists()? {
        // Header and shards were published, but the ready-marker rename
        // may not have reached disk. A matching initializer proves ownership.
        check_identity(root, id, ns, shards)?;
        check_initializer(&pending, id, ns, shards)?;
        engine::validate_completed_dataset(root, shards)?;
    } else if pending.try_exists()? {
        check_initializer(&pending, id, ns, shards)?;
        engine::validate_pending_shards(root, shards)?;
    } else if root.join("mmdb").try_exists()? {
        return Err(corrupt("completed engine has no identity header"));
    }
    Ok(())
}
pub(crate) fn install_engine(
    root: &Path,
    id: u128,
    ns: u64,
    shards: usize,
) -> Result<()> {
    let path = engine_header(root, ns);
    let expected = engine_bytes(id, ns, shards);
    match fs::read(&path) {
        Ok(bytes) if bytes == expected => {
            File::open(path.parent().expect("header has a parent"))?.sync_all()?;
        }
        Ok(_) => return Err(corrupt("refused to replace another engine's identity")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            create_dir_all_durable(path.parent().unwrap())?;
            atomic_write_file(&path, &expected)?;
        }
        Err(e) => return Err(e.into()),
    }
    let ready = root.join("__SYSTEM__/.shards_ready");
    match fs::read(&ready) {
        Ok(bytes) if bytes == b"18" => {
            File::open(ready.parent().expect("marker has a parent"))?.sync_all()?;
        }
        Ok(_) => return Err(corrupt("invalid shard-ready marker")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            atomic_write_file(&ready, b"18")?;
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VsdbOptions, vsdb_configure};
    use std::{env, fs, process::Command, thread, time::Duration};

    pub(super) fn after_missing_universe_lock() {
        let Some(dir) = env::var_os("VSDB_RO_LOCK_HANDOFF") else {
            return;
        };
        let dir = PathBuf::from(dir);
        fs::write(dir.join("reader_at_lock"), b"1").unwrap();
        let continue_at = dir.join("continue");
        for _ in 0..500 {
            if continue_at.exists() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("timed out waiting for the universe lock handoff");
    }

    #[test]
    fn read_only_open_rejects_a_lock_created_before_the_header_is_read() {
        const HANDOFF: &str = "VSDB_RO_LOCK_HANDOFF";
        if let Some(dir) = env::var_os(HANDOFF) {
            let err = vsdb_configure(VsdbOptions::read_only(dir)).unwrap_err();
            match err {
                crate::VsdbError::Io(source) => {
                    assert_eq!(source.kind(), io::ErrorKind::WouldBlock);
                }
                other => panic!("expected WouldBlock Io error, got {other:?}"),
            }
            return;
        }

        let scratch = env::temp_dir()
            .join(format!("vsdb-ro-lock-handoff-{}", rand::random::<u128>()));
        let base = scratch.join("base");
        fs::create_dir_all(&base).unwrap();
        let child = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "common::universe::tests::read_only_open_rejects_a_lock_created_before_the_header_is_read",
                "--nocapture",
            ])
            .env(HANDOFF, &base)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let ready = base.join("reader_at_lock");
        for _ in 0..500 {
            if ready.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            ready.exists(),
            "reader did not reach the missing-lock window"
        );
        let system = base.join("__SYSTEM__");
        fs::create_dir(&system).unwrap();
        let lock_file = fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(system.join("LOCK"))
            .unwrap();
        lock_file.try_lock().unwrap();
        fs::write(base.join(HEADER), header_bytes(1)).unwrap();
        fs::write(base.join("continue"), b"1").unwrap();
        let output = child.wait_with_output().unwrap();
        drop(lock_file);
        fs::remove_dir_all(&scratch).unwrap();
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
