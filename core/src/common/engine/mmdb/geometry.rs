//! Cheap preflight for the pinned MMDB v5 format before any shard can recover
//! writable. MMDB revalidates the full MANIFEST, WALs and SSTs during open.
use super::{PREFIX_SIZE, Path, Read, Result, corrupt, fs, io};
use crate::common::MMDB_SHARD_LEVELS;

pub(super) fn check_shards(dir: &Path, shards: usize, complete: bool) -> Result<()> {
    for n in 0..shards {
        let shard = dir.join(format!("shard_{n:02}"));
        let current = shard.join("CURRENT");
        match fs::symlink_metadata(&current) {
            Err(e) if e.kind() == io::ErrorKind::NotFound && !complete => continue,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(corrupt(format!(
                    "missing shard anchor {}",
                    current.display()
                )));
            }
            Err(e) => return Err(e.into()),
            Ok(m) if !m.file_type().is_file() => {
                return Err(corrupt(format!(
                    "invalid shard anchor {}",
                    current.display()
                )));
            }
            Ok(_) => {}
        }
        let mut raw_name = Vec::with_capacity(65);
        fs::File::open(&current)?
            .take(65)
            .read_to_end(&mut raw_name)?;
        if raw_name.len() > 64 {
            return Err(corrupt(format!(
                "overlong shard anchor {}",
                current.display()
            )));
        }
        let name = std::str::from_utf8(&raw_name).map_err(|_| {
            corrupt(format!(
                "invalid UTF-8 in shard anchor {}",
                current.display()
            ))
        })?;
        let name = name.trim_end_matches(['\r', '\n']);
        let number = name
            .strip_prefix("MANIFEST-")
            .and_then(|s| s.parse::<u64>().ok());
        if !number.is_some_and(|n| name == format!("MANIFEST-{n:06}")) {
            return Err(corrupt(format!(
                "invalid MANIFEST name in {}",
                current.display()
            )));
        }
        let manifest = shard.join(name);
        match fs::symlink_metadata(&manifest) {
            Ok(m) if m.file_type().is_file() => {}
            Ok(_) => {
                return Err(corrupt(format!(
                    "invalid shard MANIFEST {}",
                    manifest.display()
                )));
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(corrupt(format!(
                    "missing shard MANIFEST {}",
                    manifest.display()
                )));
            }
            Err(e) => return Err(e.into()),
        }
        let mut header = [0u8; 23];
        let mut file = fs::File::open(&manifest).map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                corrupt(format!("missing shard MANIFEST {}", manifest.display()))
            } else {
                e.into()
            }
        })?;
        file.read_exact(&mut header).map_err(|e| {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                corrupt(format!("missing MMDB v5 header in {}", manifest.display()))
            } else {
                e.into()
            }
        })?;
        // MMDB writes a single 16-byte Full record at offset zero. All
        // integers are little endian. The CRC covers the type byte + payload.
        if header[4..7] != [16, 0, 1]
            || header[7..11] != *b"MMDB"
            || header[11..15] != 5u32.to_le_bytes()
            || header[..4] != crc32fast::hash(&header[6..]).to_le_bytes()
        {
            return Err(corrupt(format!(
                "expected a checked MMDB v5 header in {}; convert v4 shards offline",
                manifest.display()
            )));
        }
        if header[15..19] != (MMDB_SHARD_LEVELS as u32).to_le_bytes()
            || header[19..] != (PREFIX_SIZE as u32).to_le_bytes()
        {
            return Err(corrupt(format!(
                "VSDB requires seven levels and an eight-byte prefix in {}",
                manifest.display()
            )));
        }
    }
    Ok(())
}
