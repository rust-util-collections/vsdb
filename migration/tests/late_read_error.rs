use std::fs;
use vsdb::{
    MapxRaw, Namespace, NamespaceOpts, StorageErrorKind, VsdbError, VsdbOptions, vsdb_configure,
};
use vsdb_migrate::{raw_map, rewrite_values};

fn payload(id: u64) -> [u8; 256] {
    let mut bytes = [0u8; 256];
    for (j, out) in bytes.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        let n = id * 32 + j as u64;
        let mixed = n.wrapping_add(0x9e3779b97f4a7c15);
        let mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
        out.copy_from_slice(&(mixed ^ (mixed >> 31)).to_le_bytes());
    }
    bytes
}

#[test]
fn rewrite_values_propagates_late_sst_read_error_after_committed_chunk() {
    let root = std::env::temp_dir().join(format!("vsdb-rewrite-err-{}", rand::random::<u128>()));
    vsdb_configure(VsdbOptions::new(&root)).unwrap();

    let ns = Namespace::create_with(NamespaceOpts {
        shards: 1,
        mem_budget_mb: Some(128),
        ..Default::default()
    })
    .unwrap();
    let mut map = MapxRaw::new_in(&ns);
    let id = map.instance_id();
    let total_rows = 20_000u64;
    for chunk_start in (0..total_rows).step_by(2_000) {
        let mut batch = map.batch();
        for i in chunk_start..chunk_start + 2_000 {
            batch.insert(i.to_be_bytes(), payload(i));
        }
        batch.commit().unwrap();
    }
    ns.flush();

    let files: Vec<_> = fs::read_dir(ns.path().join("mmdb/shard_00"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert_eq!(files.len(), 1);
    let intact = fs::read(&files[0]).unwrap();
    let mut damaged = intact.clone();
    let needle = payload(19_500);
    let offset = damaged
        .windows(32)
        .position(|w| w == &needle[..32])
        .unwrap();
    damaged[offset + 16] ^= 1;
    fs::write(&files[0], &damaged).unwrap();

    let convert = |_: &[u8], val: &[u8]| {
        if val.starts_with(b"v18:") {
            Ok(None)
        } else {
            let mut out = Vec::with_capacity(4 + val.len());
            out.extend_from_slice(b"v18:");
            out.extend_from_slice(val);
            Ok(Some(out))
        }
    };

    let err = rewrite_values(id, convert).unwrap_err();
    assert!(matches!(
        err,
        VsdbError::Storage {
            kind: StorageErrorKind::Corruption,
            ..
        }
    ));
    // First 4 MiB chunk committed before the late SST corruption was reached.
    assert_eq!(
        raw_map(id)
            .unwrap()
            .try_get_with(0u64.to_be_bytes(), |v| Ok(v.starts_with(b"v18:")))
            .unwrap(),
        Some(true)
    );

    // Restoring the damaged SST block allows a resumable retry to finish cleanly.
    fs::write(&files[0], &intact).unwrap();
    let remaining = rewrite_values(id, convert).unwrap();
    assert!(remaining > 0 && remaining < total_rows);
    assert_eq!(rewrite_values(id, convert).unwrap(), 0);
    assert_eq!(
        raw_map(id)
            .unwrap()
            .try_get_with(19_500u64.to_be_bytes(), |v| Ok(
                v == [&b"v18:"[..], &needle[..]].concat()
            ))
            .unwrap(),
        Some(true)
    );

    drop(map);
    let ns_id = ns.id();
    drop(ns);
    Namespace::close_by_id(ns_id).unwrap();
    Namespace::destroy(ns_id).unwrap();
    fs::remove_dir_all(&root).ok();
}
