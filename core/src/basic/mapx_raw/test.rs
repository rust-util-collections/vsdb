use super::*;
use ruc::*;
use std::{fs, mem::size_of};

#[test]
fn test_insert() {
    let mut hdr = MapxRaw::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(max + i)))
        .for_each(|(key, value)| {
            assert!(hdr.get(key).is_none());
            hdr.entry(&key).or_insert(&value);

            // After inserting, should exist
            assert!(hdr.contains_key(key));
            assert_eq!(&pnk!(hdr.get(key))[..], &value[..]);

            // Remove it
            hdr.remove(key);
            assert!(hdr.get(key).is_none());

            // Insert again
            hdr.insert(key, value);
            assert!(hdr.contains_key(key));
        });

    hdr.clear();
    (0..max).map(|i: u64| to_bytes(i)).for_each(|key| {
        assert!(hdr.get(key).is_none());
    });
}

#[test]
fn test_iter() {
    let mut hdr = MapxRaw::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(i)))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });

    hdr.iter_mut().for_each(|(_k, mut v)| {
        *v = to_bytes(to_u64(&v) + 1).to_vec();
    });

    for (idx, (_key, value)) in hdr.iter().enumerate() {
        assert_eq!(idx as u64 + 1, to_u64(&value));
    }
}

#[test]
fn test_first_last() {
    let mut hdr = MapxRaw::new();
    let max = 100;
    (0..max)
        .map(|i: u64| (to_bytes(i), to_bytes(i)))
        .for_each(|(key, value)| {
            hdr.insert(key, value);
        });

    let (_, value) = pnk!(hdr.iter().next());
    let val = to_u64(&value);
    assert_eq!(0, val);

    let (_, value) = pnk!(hdr.iter().next_back());
    let val = to_u64(&value);
    assert_eq!(max - 1, val);
}

#[test]
fn test_batch() {
    let mut hdr = MapxRaw::new();
    let max = 100u64;

    {
        let mut batch = hdr.batch_entry();
        for i in 0..max {
            let key = to_bytes(i);
            let value = to_bytes(max + i);
            batch.insert(&key, &value);
        }
        batch.commit().unwrap();
    }

    for i in 0..max {
        let key = to_bytes(i);
        let value = to_bytes(max + i);
        assert_eq!(&pnk!(hdr.get(key))[..], &value[..]);
    }

    {
        let mut batch = hdr.batch_entry();
        for i in 0..max {
            let key = to_bytes(i);
            batch.remove(&key);
        }
        batch.commit().unwrap();
    }

    for i in 0..max {
        let key = to_bytes(i);
        assert!(hdr.get(key).is_none());
    }
}

fn to_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(<[u8; size_of::<u64>()]>::try_from(bytes).unwrap())
}

fn to_bytes(i: u64) -> [u8; size_of::<u64>()] {
    i.to_be_bytes()
}

#[test]
fn test_save_and_from_meta() {
    let mut hdr = MapxRaw::new();
    hdr.insert([1], [10]);
    hdr.insert([2], [20]);

    let id = pnk!(hdr.save_meta());
    assert_eq!(id, hdr.instance_id());

    let restored = pnk!(MapxRaw::from_meta(id));
    assert_eq!(restored.get([1]), Some(vec![10]));
    assert_eq!(restored.get([2]), Some(vec![20]));
    assert!(restored.is_the_same_instance(&hdr));
}

#[test]
fn test_from_meta_rejects_legacy_prefix_metadata() {
    // Pre-v13.4 meta files stored a bare 8-byte prefix; v14 removed the
    // legacy acceptance path, so such a file must be rejected.
    let mut hdr = MapxRaw::new();
    hdr.insert([1], [10]);

    let id = hdr.instance_id();
    fs::write(crate::common::vsdb_meta_path(id), hdr.as_bytes()).unwrap();

    assert!(MapxRaw::from_meta(id).is_err());

    // Re-saving under the current (magic-tagged) format restores access.
    let id = pnk!(hdr.save_meta());
    let restored = pnk!(MapxRaw::from_meta(id));
    assert_eq!(restored.get([1]), Some(vec![10]));
    assert!(restored.is_the_same_instance(&hdr));
}

#[test]
fn test_from_meta_nonexistent() {
    assert!(MapxRaw::from_meta(u64::MAX).is_err());
}

/// Verify postcard serde roundtrip produces a valid, usable MapxRaw.
#[test]
fn test_serde_roundtrip() {
    let mut hdr = MapxRaw::new();
    for i in 0u64..50 {
        hdr.insert(i.to_be_bytes(), (i * 10).to_be_bytes());
    }

    let bytes = postcard::to_allocvec(&hdr).unwrap();
    let restored: MapxRaw = postcard::from_bytes(&bytes).unwrap();

    assert!(restored.is_the_same_instance(&hdr));
    for i in 0u64..50 {
        assert_eq!(
            &restored.get(i.to_be_bytes()).unwrap()[..],
            &(i * 10).to_be_bytes()
        );
    }
}

/// Verify the serialized size stays compact while carrying a metadata magic.
#[test]
fn test_serde_size() {
    let hdr = MapxRaw::new();
    let bytes = postcard::to_allocvec(&hdr).unwrap();
    // engine::Mapx hand-written serde: magic + 8-byte prefix as a byte string.
    assert!(bytes.len() <= 20, "expected ≤20 bytes, got {}", bytes.len());
}

#[test]
fn test_serde_rejects_raw_prefix_bytes() {
    let hdr = MapxRaw::new();
    assert!(postcard::from_bytes::<MapxRaw>(hdr.as_bytes()).is_err());
}

#[test]
fn test_serde_rejects_legacy_prefix_payload() {
    let hdr = MapxRaw::new();
    // A pre-magic (v13 legacy) payload is a bare 8-byte prefix; it must
    // be rejected unconditionally — the legacy acceptance path was
    // removed in v14.
    let legacy_payload = postcard::to_allocvec(hdr.as_bytes().as_slice()).unwrap();
    assert!(postcard::from_bytes::<MapxRaw>(&legacy_payload).is_err());
}

#[test]
fn test_serde_rejects_allocator_future_prefix() {
    let mut meta = Vec::new();
    meta.extend_from_slice(b"VSMAPX01");
    meta.extend_from_slice(&u64::MAX.to_le_bytes());

    let payload = postcard::to_allocvec(meta.as_slice()).unwrap();
    assert!(postcard::from_bytes::<MapxRaw>(&payload).is_err());
}

#[test]
fn test_serde_reserves_recovered_future_prefix() {
    let hdr = MapxRaw::new();
    let future = hdr.instance_id() + 1;

    let mut meta = Vec::new();
    meta.extend_from_slice(b"VSMAPX01");
    meta.extend_from_slice(&future.to_le_bytes());
    let payload = postcard::to_allocvec(meta.as_slice()).unwrap();

    let mut recovered: MapxRaw = postcard::from_bytes(&payload).unwrap();
    recovered.insert(b"k", b"recovered");

    let mut next = MapxRaw::new();
    next.insert(b"k", b"next");

    assert_ne!(next.instance_id(), future);
    assert_eq!(&recovered.get(b"k").unwrap()[..], b"recovered");
    assert_eq!(&next.get(b"k").unwrap()[..], b"next");
}

/// Mutate after restoring from meta — ensures the restored handle is fully live.
#[test]
fn test_meta_restore_then_mutate() {
    let mut hdr = MapxRaw::new();
    hdr.insert(b"k1", b"v1");

    let id = pnk!(hdr.save_meta());
    let mut restored = pnk!(MapxRaw::from_meta(id));

    // Mutate through the restored handle
    restored.insert(b"k2", b"v2");
    restored.remove(b"k1");

    assert!(restored.get(b"k1").is_none());
    assert_eq!(&restored.get(b"k2").unwrap()[..], b"v2");

    // Original handle sees the same data (same underlying storage)
    assert!(hdr.get(b"k1").is_none());
    assert_eq!(&hdr.get(b"k2").unwrap()[..], b"v2");
}

/// INV-E3 (prefix scoping): two independently created instances must be
/// fully isolated — no operation through one handle may ever observe or
/// affect the other's data, even for identical keys.
#[test]
fn test_prefix_isolation_between_instances() {
    let mut a = MapxRaw::new();
    let mut b = MapxRaw::new();
    assert!(!a.is_the_same_instance(&b));

    // Same keys, different values, interleaved writes.
    for i in 0u64..100 {
        a.insert(to_bytes(i), to_bytes(i));
        b.insert(to_bytes(i), to_bytes(i + 1000));
    }
    for i in 0u64..100 {
        assert_eq!(to_u64(&pnk!(a.get(to_bytes(i)))), i);
        assert_eq!(to_u64(&pnk!(b.get(to_bytes(i)))), i + 1000);
    }

    // A key present in only one instance is invisible to the other.
    a.insert(to_bytes(500), to_bytes(500));
    assert!(b.get(to_bytes(500)).is_none());

    // Removal through one handle never affects the other.
    a.remove(to_bytes(0));
    assert!(a.get(to_bytes(0)).is_none());
    assert_eq!(to_u64(&pnk!(b.get(to_bytes(0)))), 1000);

    // Iteration is scoped: each handle sees exactly its own entries.
    assert_eq!(a.iter().count(), 100); // 1..=99 plus key 500
    assert_eq!(b.iter().count(), 100);
    for (_, v) in b.iter() {
        assert!(to_u64(&v) >= 1000);
    }

    // clear() is scoped to the instance.
    b.clear();
    assert!(b.iter().next().is_none());
    assert_eq!(a.iter().count(), 100);
    assert_eq!(to_u64(&pnk!(a.get(to_bytes(1)))), 1);
}
