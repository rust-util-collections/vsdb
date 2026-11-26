//! Converts the allocator and namespace bootstrap metadata while the database
//! is offline. The journal retains the universe ID across interrupted upgrades.
use crate::{corrupt, exactly};
use mmdb_v4 as mmdb;
mod staging;
use serde::{Deserialize, Serialize};
pub use staging::stage_universe;
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};
use vsdb::{
    Result, VsdbError,
    common::{atomic_write_file, create_dir_all_durable, error::StorageErrorKind},
};

fn mmdb_err(error: mmdb::Error) -> VsdbError {
    use mmdb::ErrorKind;
    let kind = match error.kind() {
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

fn v4_iterator_status(stream: &mmdb::DBIterator) -> Result<()> {
    // MMDB 4.3.15 exposes only the terminal diagnostic string. Its
    // originating I/O or data-corruption kind cannot be recovered.
    if let Some(message) = stream.error() {
        return Err(VsdbError::Storage {
            kind: StorageErrorKind::Other,
            source: message.into(),
        });
    }
    Ok(())
}

const REG: &str = "__SYSTEM__/__namespaces__";
const STATES: &str = "__SYSTEM__/__namespace_state__";
const HEADER: &str = "__SYSTEM__/universe_header";
const PROGRESS: &str = "__SYSTEM__/upgrade_v18";
const CEILING: &str = "__SYSTEM__/__prefix_ceiling__";
const INIT: &str = "__SYSTEM__/__initializing__";
const FLOOR: u64 = 4096_0000;

#[derive(Serialize, Deserialize)]
struct Registry<R> {
    next_id: u64,
    entries: Vec<R>,
}
#[derive(Serialize, Deserialize)]
struct Record {
    id: u64,
    path: Option<String>,
    shards: u32,
    mem_budget_mb: Option<u64>,
    created_at: u64,
}
#[derive(Serialize, Deserialize, PartialEq, Eq)]
enum State {
    Pending,
    Established,
}
type Plan = Registry<(Record, State)>;

fn check_regular(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("expected regular file: {}", path.display()),
        ));
    }
    Ok(())
}

fn read_metadata(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let path = path.as_ref();
    check_regular(path)?;
    fs::read(path)
}

fn read_metadata_text(path: impl AsRef<Path>) -> io::Result<String> {
    String::from_utf8(read_metadata(path)?)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn working_lock(path: &Path) -> io::Result<File> {
    match check_regular(path) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        result => result?,
    }
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

fn registry(base: &Path, converted: bool, upgrading: bool) -> Result<Plan> {
    let bytes = match read_metadata(base.join(REG)) {
        Ok(b) => b,
        Err(e)
            if e.kind() == io::ErrorKind::NotFound
                && !converted
                && !base.join(STATES).try_exists()?
                && !base.join("__NAMESPACES__").try_exists()? =>
        {
            return Ok(Registry {
                next_id: 1,
                entries: vec![],
            });
        }
        Err(e) => return Err(e.into()),
    };
    let reg: Plan = if let Some(payload) = bytes.strip_prefix(b"VSNREG02") {
        if !converted && !upgrading {
            return Err(corrupt(
                "converted registry has no upgrade journal or universe header",
            ));
        }
        exactly(payload)?
    } else {
        if converted {
            return Err(corrupt("committed universe has a legacy registry"));
        }
        let old: Registry<Record> = exactly(&bytes)?;
        let mut entries = Vec::with_capacity(old.entries.len());
        for r in old.entries {
            let path = base.join(STATES).join(format!("{:016x}", r.id));
            let state = match read_metadata(path) {
                Ok(bytes) if bytes == b"P" => State::Pending,
                Ok(bytes) if bytes == b"E" => State::Established,
                Ok(_) => return Err(corrupt("invalid namespace lifecycle record")),
                Err(e) if e.kind() == io::ErrorKind::NotFound => State::Established,
                Err(e) => return Err(e.into()),
            };
            entries.push((r, state));
        }
        Registry {
            next_id: old.next_id,
            entries,
        }
    };
    let mut seen = HashSet::new();
    if reg.next_id == 0
        || reg.entries.iter().any(|(r, _)| {
            r.id == 0 || r.id >= reg.next_id || !(1..=64).contains(&r.shards) || !seen.insert(r.id)
        })
    {
        return Err(corrupt("invalid v17 namespace registry"));
    }
    validate_placement(base, &reg)?;
    validate_namespace_residue(base, &reg)?;
    Ok(reg)
}

// Resolve existing symlinked ancestors even if a pending root itself has
// not been created. Registry paths control where engine headers are installed.
fn physical_path(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err(corrupt(format!(
            "invalid namespace path: {}",
            path.display()
        )));
    }
    let mut ancestor = path;
    loop {
        match ancestor.canonicalize() {
            Ok(resolved) => return Ok(resolved.join(path.strip_prefix(ancestor).unwrap())),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                if fs::symlink_metadata(ancestor).is_ok() {
                    return Err(corrupt("dangling namespace symlink"));
                }
                ancestor = ancestor.parent().ok_or(e)?;
            }
            Err(e) => return Err(e.into()),
        }
    }
}
fn validate_placement(base: &Path, reg: &Plan) -> Result<()> {
    let mut paths = Vec::new();
    for (rec, _) in &reg.entries {
        let derived = base.join("__NAMESPACES__").join(format!("{:016x}", rec.id));
        let path = physical_path(rec.path.as_deref().map(Path::new).unwrap_or(&derived))?;
        if rec.path.is_some() && (base.starts_with(&path) || path.starts_with(base))
            || rec.path.is_none() && path != derived
            || paths
                .iter()
                .any(|other: &PathBuf| path.starts_with(other) || other.starts_with(&path))
        {
            return Err(corrupt(
                "namespace registry roots overlap or alias another placement",
            ));
        }
        paths.push(path);
    }
    Ok(())
}

fn validate_namespace_residue(base: &Path, reg: &Plan) -> Result<()> {
    let registered: HashSet<_> = reg.entries.iter().map(|(r, _)| r.id).collect();
    let derived: HashSet<_> = reg
        .entries
        .iter()
        .filter(|(rec, _)| rec.path.is_none())
        .map(|(rec, _)| rec.id)
        .collect();
    for (dir, sidecars) in [
        (base.join(STATES), true),
        (base.join("__NAMESPACES__"), false),
    ] {
        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| corrupt("invalid namespace artifact name"))?;
            let id_text = if sidecars {
                name.strip_suffix(".tmp").unwrap_or(name)
            } else {
                name
            };
            let id = u64::from_str_radix(id_text, 16)
                .ok()
                .filter(|id| format!("{id:016x}") == id_text)
                .ok_or_else(|| corrupt("unexpected namespace artifact"))?;
            if id == 0 || id >= reg.next_id {
                return Err(corrupt(
                    "namespace artifact exceeds registry allocation ceiling",
                ));
            }
            let ty = entry.file_type()?;
            if (sidecars && !ty.is_file()) || (!sidecars && !ty.is_dir()) {
                return Err(corrupt("unexpected namespace artifact type"));
            }
            // A relocated explicit namespace may have an empty placeholder
            // at its old derived path. A second populated tree is ambiguous.
            let owned = if sidecars {
                registered.contains(&id)
            } else {
                derived.contains(&id)
            };
            if !owned {
                // Publishing the burnt ID precedes publishing a pending record
                // in v17. A lone P can therefore be a normal interrupted create.
                // A live tree or an established orphan cannot be distinguished
                // from a partially lost registry, especially on another volume.
                if !sidecars && fs::read_dir(entry.path())?.next().transpose()?.is_some()
                    || sidecars && !name.ends_with(".tmp") && read_metadata(entry.path())? != b"P"
                {
                    return Err(corrupt(
                        "unregistered namespace engine or established lifecycle record",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn header(id: u128) -> Vec<u8> {
    [
        b"VSUNI018".as_slice(),
        &18u32.to_le_bytes(),
        &id.to_le_bytes(),
    ]
    .concat()
}
fn read_id(path: &Path) -> Result<Option<u128>> {
    match read_metadata(path) {
        Ok(b) if b.len() == 28 && b[..12] == header(0)[..12] => {
            Ok(Some(u128::from_le_bytes(b[12..].try_into().unwrap())))
        }
        Ok(_) => Err(corrupt("malformed universe identity")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn observe(bytes: &[u8], max: &mut u64) {
    for (i, w) in bytes.windows(8).enumerate() {
        let offset = match w {
            b"VSMAPX01" | b"VSMAPX02" => 8,
            b"VSTYPE04" => 16,
            _ => continue,
        };
        if let Some(raw) = bytes.get(i + offset..i + offset + 8) {
            *max = (*max).max(u64::from_le_bytes(raw.try_into().unwrap()));
        }
    }
}
// save_instance_meta accepts application-defined values. The canonical
// filename identifies the locally issued map independently of its payload.
fn saved_instance_prefix(entry: &fs::DirEntry) -> Option<u64> {
    let raw = entry.file_name();
    let name = raw.to_str()?;
    (name.len() == 16
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
    .then(|| u64::from_str_radix(name, 16).expect("sixteen hex digits"))
}

fn scan_files(dir: &Path, instance_meta: bool, max: &mut u64) -> Result<()> {
    if !dir.try_exists()? {
        return Ok(());
    }
    let mut dirs = vec![dir.to_owned()];
    let mut buf = vec![0; 64 * 1024];
    while let Some(d) = dirs.pop() {
        for entry in fs::read_dir(&d)? {
            let e = entry?;
            if e.file_type()?.is_dir() {
                dirs.push(e.path());
            } else if e.file_type()?.is_file() {
                if d == dir
                    && instance_meta
                    && let Some(prefix) = saved_instance_prefix(&e)
                {
                    *max = (*max).max(prefix);
                }
                let mut file = File::open(e.path())?;
                let mut carry = 0;
                loop {
                    let n = match file.read(&mut buf[carry..]) {
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        n => n?,
                    };
                    if n == 0 {
                        break;
                    }
                    let end = carry + n;
                    observe(&buf[..end], max);
                    carry = end.min(23);
                    buf.copy_within(end - carry..end, 0);
                }
            }
        }
    }
    Ok(())
}

struct Engine {
    initializing: bool,
    root: PathBuf,
    ns: u64,
    shards: u32,
}
impl Engine {
    // An established namespace always requires its complete set of shards.
    // A pending namespace can be empty or partway through its initial open.
    fn discover(
        root: PathBuf,
        ns: u64,
        shards: u32,
        established: bool,
        current: bool,
        upgrade_id: Option<u128>,
    ) -> Result<Option<Self>> {
        let mut engine = Self {
            root,
            ns,
            shards,
            initializing: false,
        };
        let system = engine.root.join("__SYSTEM__");
        let version = match read_metadata_text(system.join("format_version")) {
            Ok(s) => Some(s),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if current && version.as_deref().is_some_and(|v| v != "18") {
            return Err(corrupt("conflicting v18 downgrade marker"));
        }
        let completed = engine.header_path().try_exists()?
            || system.join(".shards_ready").try_exists()?
            || version.as_deref().is_some_and(|s| s.trim() == "16");
        if established || completed {
            return Ok(Some(engine));
        }
        if engine.root.join(INIT).try_exists()? {
            if version.as_deref().is_some_and(|s| s.trim() != "18") {
                return Err(corrupt("invalid pending engine format"));
            }
            engine.initializing = true;
            return Ok(Some(engine));
        }
        let mmdb = engine.root.join("mmdb");
        if version.is_some() {
            // The universe marker also guards a default engine that has never been opened.
            if ns == 0
                && (current || upgrade_id.is_some())
                && version.as_deref() == Some("18")
                && !mmdb.try_exists()?
            {
                return Ok(None);
            }
            return Err(corrupt("missing engine initializer"));
        }
        if mmdb.try_exists()? {
            if current
                || !fs::symlink_metadata(&mmdb)?.is_dir()
                || fs::read_dir(&mmdb)?.next().transpose()?.is_some()
            {
                return Err(corrupt("missing engine initializer"));
            }
            // v17 MmDB::open_at created mmdb/ before __SYSTEM__ and __initializing__.
            engine.initializing = true;
        }
        if ns == 0 && !current {
            let tmp = system.join("__initializing__.tmp");
            if tmp.try_exists()? {
                if !fs::symlink_metadata(&tmp)?.is_file() {
                    return Err(corrupt("invalid pending engine initializer"));
                }
                engine.initializing = true;
            }
        }
        if ns != 0 && engine.root.try_exists()? {
            for entry in fs::read_dir(&engine.root)? {
                let e = entry?;
                if !current && e.file_name() == "mmdb" && e.file_type()?.is_dir() {
                    continue;
                }
                if e.file_name() != "__SYSTEM__" || !e.file_type()?.is_dir() {
                    return Err(corrupt("foreign pending namespace contents"));
                }
                for inner in fs::read_dir(e.path())? {
                    let inner = inner?;
                    let name = inner.file_name();
                    let name = name.to_string_lossy();
                    if !inner.file_type()?.is_file() {
                        return Err(corrupt("foreign pending namespace metadata"));
                    }
                    // A current-format create can have published only its
                    // staging sibling. The engine will resume it on open.
                    if (current || upgrade_id.is_some())
                        && name
                            .strip_prefix("__initializing__.")
                            .and_then(|tail| tail.strip_suffix(".tmp"))
                            .and_then(|suffix| suffix.split_once('.'))
                            .is_some_and(|(pid, n)| {
                                pid.parse::<u32>().is_ok() && n.parse::<u64>().is_ok()
                            })
                    {
                        if let Some(id) = upgrade_id {
                            let expected = engine.bytes(id);
                            if inner.metadata()?.len() > expected.len() as u64
                                || !expected.starts_with(&read_metadata(inner.path())?)
                            {
                                return Err(corrupt(
                                    "staged initializer disagrees with upgrade journal",
                                ));
                            }
                        }
                        continue;
                    }
                    if current || name != "__initializing__.tmp" {
                        return Err(corrupt("foreign pending namespace metadata"));
                    }
                    // The v17 create journal can be interrupted before renaming
                    // its initializer, when no shard has been created yet.
                    engine.initializing = true;
                }
            }
        }
        Ok(engine.initializing.then_some(engine))
    }
    fn header_path(&self) -> PathBuf {
        let dir = if self.ns == 0 {
            self.root.join("mmdb")
        } else {
            self.root.clone()
        };
        dir.join("__SYSTEM__/engine_header")
    }
    fn bytes(&self, id: u128) -> Vec<u8> {
        [
            b"VSENG018".as_slice(),
            &id.to_le_bytes(),
            &self.ns.to_le_bytes(),
            &self.shards.to_le_bytes(),
        ]
        .concat()
    }
    fn validate(
        &self,
        id: u128,
        scan: bool,
        max: &mut u64,
        ceiling: &mut u64,
        current: bool,
        locks: &mut Vec<File>,
    ) -> Result<()> {
        let has_header = self.header_path().try_exists()?;
        if !has_header && self.root.join("__SYSTEM__/.shards_ready").try_exists()? {
            return Err(corrupt("completed engine is missing its identity"));
        }
        if self.initializing {
            let sentinel = self.root.join(INIT);
            match read_metadata(sentinel) {
                Ok(bytes) if bytes == self.bytes(id) || !current && bytes == b"1" => {}
                Ok(_) => return Err(corrupt("pending engine initializer identity mismatch")),
                Err(e) if !current && e.kind() == io::ErrorKind::NotFound => {}
                Err(e) if current && e.kind() == io::ErrorKind::NotFound => {
                    return Err(corrupt("pending engine initializer identity mismatch"));
                }
                Err(e) => return Err(e.into()),
            }
        } else if current {
            let ready = self.root.join("__SYSTEM__/.shards_ready");
            let owned = if ready.try_exists()? {
                read_metadata(ready)? == b"18"
            } else {
                match read_metadata(self.root.join(INIT)) {
                    Ok(bytes) => bytes == self.bytes(id),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => false,
                    Err(error) => return Err(error.into()),
                }
            };
            if !has_header || !owned {
                return Err(corrupt("incomplete v18 engine identity"));
            }
        } else {
            let version = read_metadata_text(self.root.join("__SYSTEM__/format_version"))?;
            if version.trim() != "16" && !(version.trim() == "18" && has_header) {
                return Err(corrupt("unsupported v17 engine marker"));
            }
        }
        if self.header_path().try_exists()? && read_metadata(self.header_path())? != self.bytes(id)
        {
            return Err(corrupt("engine identity disagrees with upgrade journal"));
        }
        let shards = self.root.join("mmdb");
        let scan_dir = match fs::read_dir(&shards) {
            Ok(dir) => dir,
            Err(e) if self.initializing && e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        for entry in scan_dir {
            let name = entry?.file_name();
            if name.to_string_lossy().starts_with("shard_")
                && !(0..self.shards).any(|s| name == format!("shard_{s:02}").as_str())
            {
                return Err(corrupt("unexpected shard directory"));
            }
        }
        for n in 0..self.shards {
            let dir = shards.join(format!("shard_{n:02}"));
            if self.initializing && !dir.try_exists()? {
                continue;
            }
            let anchored = dir.join("CURRENT").is_file();
            if !anchored && !self.initializing {
                return Err(corrupt(format!(
                    "missing shard anchor at {}",
                    dir.display()
                )));
            }
            // The universe LOCK did not exist in v17. Keep shared shard locks
            // through publication, including when the ceiling makes a full scan
            // unnecessary. A writable old process takes the exclusive mmdb lock.
            let shard_lock = working_lock(&dir.join("LOCK"))?;
            shard_lock.try_lock_shared().map_err(io::Error::from)?;
            locks.push(shard_lock);
            let default_first = self.ns == 0 && n == 0;
            if anchored && (scan || (!current && default_first)) {
                let db = mmdb::DB::open_read_only_with_options(
                    mmdb::DbOptions {
                        prefix_len: 8,
                        ..Default::default()
                    },
                    dir,
                )
                .map_err(mmdb_err)?;
                if default_first && let Some(bytes) = db.get(&[0]).map_err(mmdb_err)? {
                    // A v17 universe can retain its pre-file allocator value.
                    // It is an exclusive ceiling, not an issued prefix.
                    *ceiling = (*ceiling).max(u64::from_le_bytes(
                        bytes
                            .try_into()
                            .map_err(|_| corrupt("invalid shard-0 allocator ceiling"))?,
                    ));
                }
                if !scan {
                    continue;
                }
                let mut rows = db.iter().map_err(mmdb_err)?;
                for (key, val) in rows.by_ref() {
                    if let Some(prefix) = key.get(..8) {
                        *max = (*max).max(u64::from_le_bytes(prefix.try_into().unwrap()));
                        observe(&key[8..], max);
                    }
                    observe(&val, max);
                }
                v4_iterator_status(&rows)?;
            }
        }
        if scan && self.ns != 0 {
            scan_files(&self.root.join("__SYSTEM__/__instance_meta__"), true, max)?;
        }
        Ok(())
    }
    fn install(&self, id: u128) -> Result<()> {
        if self.initializing {
            create_dir_all_durable(&self.root.join("__SYSTEM__"))?;
            atomic_write_file(&self.root.join(INIT), &self.bytes(id))?;
            match fs::remove_file(self.root.join("__SYSTEM__/__initializing__.tmp")) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            return atomic_write_file(&self.root.join("__SYSTEM__/format_version"), b"18");
        }
        let path = self.header_path();
        create_dir_all_durable(path.parent().unwrap())?;
        atomic_write_file(&path, &self.bytes(id))?;
        atomic_write_file(&self.root.join("__SYSTEM__/.shards_ready"), b"18")?;
        // Existing older binaries reject this root before creating new rows.
        atomic_write_file(&self.root.join("__SYSTEM__/format_version"), b"18")
    }
}

// v17 persisted the empty mmdb directory before creating __SYSTEM__.
// `__CUSTOM__` may also pre-exist if `vsdb_get_custom_dir()` ran first.
// The caller holds the root directory lock while recognizing this layout.
fn ensure_bootstrap_system(base: &Path, system: &Path) -> Result<()> {
    if system.try_exists()? {
        return Ok(());
    }
    let mut has_mmdb = false;
    for entry in fs::read_dir(base)? {
        let e = entry?;
        let is_dir = e.file_type()?.is_dir();
        if e.file_name() == "mmdb" && is_dir {
            has_mmdb = true;
        } else if !(e.file_name() == "__CUSTOM__" && is_dir) {
            return Err(corrupt("unrecognized v17 bootstrap directory"));
        }
    }
    if !has_mmdb
        || fs::read_dir(base.join("mmdb"))?
            .next()
            .transpose()?
            .is_some()
    {
        return Err(corrupt("unrecognized v17 bootstrap directory"));
    }
    create_dir_all_durable(system)
}

/// Upgrade VSDB metadata on an offline v4-backed working copy. This low-level
/// step leaves existing v4 shards intact. Use [`stage_universe`] to build
/// the generation that the v5-backed runtime can open.
/// Stop every process accessing this v17 database first, including processes
/// using explicitly placed namespaces. A retry resumes the same conversion.
pub fn migrate_universe(base: impl AsRef<Path>) -> Result<()> {
    if vsdb::vsdb_open_mode() == vsdb::OpenMode::ReadOnly {
        return Err(vsdb::VsdbError::ReadOnly {
            operation: "universe migration",
        });
    }
    let base = base.as_ref().canonicalize()?;
    if !base.is_dir() {
        return Err(corrupt("universe source must be a directory"));
    }
    let system = base.join("__SYSTEM__");
    if !system.try_exists()? {
        let adoption = File::open(&base)?;
        adoption.try_lock().map_err(io::Error::from)?;
        let result = ensure_bootstrap_system(&base, &system);
        adoption.unlock()?;
        result?;
    }
    let lock = working_lock(&system.join("LOCK"))?;
    lock.try_lock().map_err(io::Error::from)?;
    let committed = read_id(&base.join(HEADER))?;
    let journal = read_id(&base.join(PROGRESS))?;
    let current = committed.is_some() && journal.is_none();
    if !current && committed.is_some() && committed != journal {
        return Err(corrupt("upgrade journal identifies a different universe"));
    }
    let id = committed.or(journal).unwrap_or_else(rand::random);
    let plan = registry(&base, committed.is_some(), journal.is_some())?;
    let mut engines = Vec::new();
    if let Some(engine) = Engine::discover(base.clone(), 0, 16, false, current, journal)? {
        engines.push(engine);
    }
    if engines.is_empty() && !base.join(REG).try_exists()? {
        return Err(corrupt("no v17 database at supplied path"));
    }
    for (r, state) in &plan.entries {
        let root = match &r.path {
            Some(p) => PathBuf::from(p),
            None => base.join("__NAMESPACES__").join(format!("{:016x}", r.id)),
        };
        if let Some(engine) = Engine::discover(
            root,
            r.id,
            r.shards,
            *state == State::Established,
            current,
            journal,
        )? {
            engines.push(engine)
        }
    }
    let stored = match read_metadata(base.join(CEILING)) {
        Ok(v) => Some(u64::from_le_bytes(
            v.try_into()
                .map_err(|_| corrupt("invalid prefix ceiling"))?,
        )),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    let mut max = FLOOR - 1;
    let mut ceiling = stored.unwrap_or(FLOOR);
    if ceiling < FLOOR {
        return Err(corrupt("prefix ceiling below allocation floor"));
    }
    let mut locks = Vec::new();
    for engine in &engines {
        engine.validate(
            id,
            stored.is_none(),
            &mut max,
            &mut ceiling,
            current,
            &mut locks,
        )?
    }
    if stored.is_none() {
        scan_files(&base.join("__CUSTOM__"), false, &mut max)?;
        scan_files(&base.join("__SYSTEM__/__instance_meta__"), true, &mut max)?;
        ceiling = ceiling.max(
            max.checked_add(1)
                .ok_or_else(|| corrupt("prefix ceiling exhausted"))?,
        );
    }
    let protection = system.join("format_version");
    if current {
        for root in
            std::iter::once(&base).chain(engines.iter().filter(|e| e.ns != 0).map(|e| &e.root))
        {
            let marker = root.join("__SYSTEM__/format_version");
            if marker.try_exists()? {
                // A previous rename may have succeeded while its directory
                // sync failed. Fence the visible marker before acknowledging a retry.
                File::open(root.join("__SYSTEM__"))?.sync_all()?;
            } else {
                atomic_write_file(&marker, b"18")?;
            }
        }
        if stored != Some(ceiling) {
            atomic_write_file(&base.join(CEILING), &ceiling.to_le_bytes())?;
        }
        return Ok(());
    }
    atomic_write_file(&base.join(PROGRESS), &header(id))?;
    let has_default = engines.iter().any(|e| e.ns == 0);
    for e in engines {
        e.install(id)?;
    }
    if !has_default {
        atomic_write_file(&protection, b"18")?;
    }
    let mut reg = b"VSNREG02".to_vec();
    postcard::to_io(&plan, &mut reg)?;
    atomic_write_file(&base.join(REG), &reg)?;
    atomic_write_file(&base.join(CEILING), &ceiling.to_le_bytes())?;
    atomic_write_file(&base.join(HEADER), &header(id))?;
    if base.join(STATES).try_exists()? {
        fs::remove_dir_all(base.join(STATES))?;
    }
    fs::remove_file(base.join(PROGRESS))?;
    File::open(system)?.sync_all()?;
    Ok(())
}
