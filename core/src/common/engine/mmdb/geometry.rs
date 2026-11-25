//! Cheap preflight for the pinned MMDB v5 format before any shard can recover
//! writable. MMDB revalidates the full MANIFEST, WALs and SSTs during open.
use super::{PREFIX_SIZE, Path, Read, Result, corrupt, fs, io};

pub(super) fn check_shards(dir: &Path, shards: usize, complete: bool) -> Result<()> {
    for n in 0..shards {
        let shard = dir.join(format!("shard_{n:02}"));
        let current = shard.join("CURRENT");
        match fs::symlink_metadata(&current) {
            Err(e) if e.kind() == io::ErrorKind::NotFound && !complete => continue,
            Err(e) => return Err(e.into()),
            Ok(m) if !m.file_type().is_file() => {
                return Err(corrupt(format!(
                    "invalid shard anchor {}",
                    current.display()
                )));
            }
            Ok(_) => {}
        }
        let mut name = String::new();
        fs::File::open(&current)?
            .take(65)
            .read_to_string(&mut name)?;
        if name.len() > 64 {
            return Err(corrupt(format!(
                "overlong shard anchor {}",
                current.display()
            )));
        }
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
        if !fs::symlink_metadata(&manifest)?.file_type().is_file() {
            return Err(corrupt(format!(
                "invalid shard MANIFEST {}",
                manifest.display()
            )));
        }
        let mut header = [0u8; 23];
        fs::File::open(&manifest)?
            .read_exact(&mut header)
            .map_err(|e| {
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
        if header[15..19] != 7u32.to_le_bytes()
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
