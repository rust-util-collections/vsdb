use super::*;
use crate::{NamespaceOpts, StorageErrorKind};
use std::fs;

#[test]
fn fallible_clone_wipes_committed_chunks_after_late_source_corruption() {
    let namespace = || {
        Namespace::create_with(NamespaceOpts {
            shards: 1,
            mem_budget_mb: Some(128),
            ..Default::default()
        })
        .unwrap()
    };
    let source_ns = namespace();
    let destination_ns = namespace();
    let mut source = Mapx::new_in(&source_ns);
    fn payload(id: u64) -> [u8; 256] {
        let mut bytes = [0; 256];
        for (j, out) in bytes.as_chunks_mut::<8>().0.iter_mut().enumerate() {
            let n = id * 32 + j as u64;
            let mixed = n.wrapping_add(0x9e3779b97f4a7c15);
            let mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d049bb133111eb);
            out.copy_from_slice(&(mixed ^ (mixed >> 31)).to_le_bytes());
        }
        bytes
    }
    let mut batch = source.batch_begin();
    for i in 0..5000u64 {
        batch.insert(&i.to_be_bytes(), &payload(i));
    }
    batch.commit_sync().unwrap();
    source_ns.flush();
    let files: Vec<_> = fs::read_dir(source_ns.path().join("mmdb/shard_00"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "sst"))
        .collect();
    assert_eq!(files.len(), 1);
    let mut contents = fs::read(&files[0]).unwrap();
    let key = payload(4700);
    let offset = contents.windows(32).position(|w| w == &key[..32]).unwrap();
    contents[offset + 16] ^= 1;
    fs::write(&files[0], contents).unwrap();

    let target = Mapx::new_in(&destination_ns);
    let observer = target.reader();
    let err = source.copy_into(target).unwrap_err();
    assert!(matches!(
        err,
        VsdbError::Storage {
            kind: StorageErrorKind::Corruption,
            ..
        }
    ));
    let written = destination_ns.shard_properties("stats.bytes_written")[0]
        .as_ref()
        .unwrap()
        .parse::<usize>()
        .unwrap();
    assert!(
        written >= 4096 * 256,
        "expected a completed destination chunk, {written} bytes"
    );
    assert_eq!(
        observer
            .read_view()
            .try_range(..)
            .unwrap()
            .try_step_next()
            .unwrap(),
        None
    );
}
