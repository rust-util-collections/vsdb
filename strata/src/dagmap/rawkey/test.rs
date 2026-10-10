use super::*;
use std::{thread, time::Duration};

macro_rules! s {
    ($i: expr) => {{ $i.as_bytes().to_vec() }};
}

#[test]
fn mutable_guard_persists_interior_edits() {
    use std::cell::RefCell;

    let mut dag = DagMapRawKey::<RefCell<u32>>::new(None);
    dag.insert(b"key", &RefCell::new(10));
    {
        let guard = dag.get_mut(b"key").unwrap();
        *guard.borrow_mut() = 20;
    }
    assert_eq!(dag.get(b"key").unwrap().into_inner(), 20);
}

#[test]
fn mutable_guard_preserves_untouched_variable_encoding() {
    struct TaggedValue(u32);
    impl ValueEnDe for TaggedValue {
        fn try_encode(&self) -> Result<Vec<u8>> {
            use std::sync::atomic::{AtomicU32, Ordering};
            static TAG: AtomicU32 = AtomicU32::new(0);
            let mut bytes = self.0.to_le_bytes().to_vec();
            bytes.extend_from_slice(&TAG.fetch_add(1, Ordering::Relaxed).to_le_bytes());
            Ok(bytes)
        }

        fn decode(bytes: &[u8]) -> Result<Self> {
            Ok(Self(u32::from_le_bytes(bytes[..4].try_into().unwrap())))
        }
    }

    let mut dag = DagMapRawKey::<TaggedValue>::new(None);
    dag.insert(b"key", &TaggedValue(10));
    let original = dag.inner.get(b"key").unwrap();
    {
        let guard = dag.get_mut(b"key").unwrap();
        assert_eq!(guard.0, 10);
    }
    assert_eq!(dag.inner.get(b"key").unwrap(), original);
    *dag.get_mut(b"key").unwrap() = TaggedValue(20);
    assert_eq!(dag.get(b"key").unwrap().0, 20);
}

#[test]
fn mutable_guard_discards_interrupted_interior_edits() {
    use std::{
        cell::RefCell,
        panic::{AssertUnwindSafe, catch_unwind},
    };

    let mut dag = DagMapRawKey::<RefCell<u32>>::new(None);
    dag.insert(b"key", &RefCell::new(10));
    let interrupted = catch_unwind(AssertUnwindSafe(|| {
        let guard = dag.get_mut(b"key").unwrap();
        *guard.borrow_mut() = 20;
        panic!("interrupted edit");
    }));
    assert!(interrupted.is_err());
    assert_eq!(dag.get(b"key").unwrap().into_inner(), 10);
}

#[test]
fn dagmaprawkey_functions() {
    let mut i0: DagMapRawKey<Vec<u8>> = DagMapRawKey::new(None);
    i0.insert("k0", &s!("v0"));
    assert_eq!(i0.get("k0").unwrap(), s!("v0"));
    assert!(i0.get("k1").is_none());
    let mut i0_raw = i0.into_inner();

    let mut i1: DagMapRawKey<Vec<u8>> = DagMapRawKey::new(Some(&mut i0_raw));
    i1.insert("k1", &s!("v1"));
    assert_eq!(i1.get("k1").unwrap(), s!("v1"));
    assert_eq!(i1.get("k0").unwrap(), s!("v0"));
    let mut i1_raw = i1.into_inner();

    let mut i2: DagMapRawKey<Vec<u8>> = DagMapRawKey::new(Some(&mut i1_raw));
    i2.insert("k2", &s!("v2"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0"));
    i2.insert("k2", &s!("v2x"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2x"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0"));
    i2.insert("k1", &s!("v1x"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2x"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1x"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0"));
    i2.insert("k0", &s!("v0x"));
    assert_eq!(i2.get("k2").unwrap(), s!("v2x"));
    assert_eq!(i2.get("k1").unwrap(), s!("v1x"));
    assert_eq!(i2.get("k0").unwrap(), s!("v0x"));

    // Overlay isolation: parents never see descendant writes.
    assert!(i1_raw.get("k2").is_none());
    assert_eq!(i1_raw.get("k1").unwrap(), s!("v1").encode());
    assert_eq!(i1_raw.get("k0").unwrap(), s!("v0").encode());

    assert!(i0_raw.get("k2").is_none());
    assert!(i0_raw.get("k1").is_none());
    assert_eq!(i0_raw.get("k0").unwrap(), s!("v0").encode());

    // The original owned handles alias the same storage and observe the
    // post-prune state (`Clone` would deep-copy instead).
    let mut head = i2.prune().unwrap();
    thread::sleep(Duration::from_millis(1000));

    assert_eq!(head.get("k2").unwrap(), s!("v2x"));
    assert_eq!(head.get("k1").unwrap(), s!("v1x"));
    assert_eq!(head.get("k0").unwrap(), s!("v0x"));

    // The intermediate mainline node was merged into genesis and cleared.
    assert!(i1_raw.is_dead());
    // The genesis handle sees the merged result (same storage as `head`).
    assert_eq!(i0_raw.get("k2").unwrap(), s!("v2x").encode());

    // prune with deep stack
    for i in 10u8..=255 {
        head.insert(i.to_be_bytes(), &i.to_be_bytes().to_vec());
        let mut raw = head.into_inner();
        head = DagMapRawKey::new(Some(&mut raw));
    }

    let mut head = head.prune().unwrap();
    thread::sleep(Duration::from_millis(1000));

    for i in 10u8..=255 {
        assert_eq!(head.get(i.to_be_bytes()).unwrap(), i.to_be_bytes().to_vec());
    }

    for i in 0u8..=254 {
        head.remove(i.to_be_bytes());
        assert!(head.get(i.to_be_bytes()).is_none());
    }

    *(head.get_mut(255u8.to_be_bytes()).unwrap()) = 0u8.to_be_bytes().to_vec();
    assert_eq!(
        head.get(255u8.to_be_bytes()).unwrap().as_slice(),
        0u8.to_be_bytes()
    );
}

#[test]
fn test_save_and_from_meta() {
    let mut dag: DagMapRawKey<Vec<u8>> = DagMapRawKey::new(None);
    dag.insert("k1", &s!("v1"));
    dag.insert("k2", &s!("v2"));

    let id = dag.save_meta().unwrap();
    assert_eq!(id, dag.instance_id());

    let restored: DagMapRawKey<Vec<u8>> = DagMapRawKey::from_meta(id).unwrap();
    assert_eq!(restored.get("k1").unwrap(), s!("v1"));
    assert_eq!(restored.get("k2").unwrap(), s!("v2"));
}

/// `V` is phantom-only, so this exercises the typed-handle envelope:
/// restoring under a different value type must fail loudly.
#[test]
fn test_from_meta_rejects_wrong_value_type() {
    let mut dag: DagMapRawKey<Vec<u8>> = DagMapRawKey::new(None);
    dag.insert("k1", &s!("v1"));

    let id = dag.save_meta().unwrap();
    assert!(DagMapRawKey::<String>::from_meta(id).is_err());
}

/// Postcard serde roundtrip for DagMapRawKey (delegates to DagMapRaw).
#[test]
fn test_serde_roundtrip() {
    let mut dag: DagMapRawKey<Vec<u8>> = DagMapRawKey::new(None);
    dag.insert("x", &s!("X"));
    dag.insert("y", &s!("Y"));

    let bytes = postcard::to_allocvec(&dag).unwrap();
    let restored: DagMapRawKey<Vec<u8>> = postcard::from_bytes(&bytes).unwrap();

    assert_eq!(restored.get("x").unwrap(), s!("X"));
    assert_eq!(restored.get("y").unwrap(), s!("Y"));
}

#[test]
#[should_panic(expected = "empty encoded value is a tombstone")]
fn typed_empty_encoding_panics() {
    let mut dag: DagMapRawKey<()> = DagMapRawKey::new(None);
    dag.insert("unit", &());
}

/// from_meta nonexistent.
#[test]
fn test_from_meta_nonexistent() {
    assert!(DagMapRawKey::<Vec<u8>>::from_meta(u64::MAX).is_err());
}

/// Restore meta with parent-child lineage.
#[test]
fn test_meta_with_parent_child() {
    let mut p = DagMapRawKey::<Vec<u8>>::new(None);
    p.insert("pk", &s!("pv"));

    let mut p_raw = p.into_inner();
    let mut c = DagMapRawKey::<Vec<u8>>::new(Some(&mut p_raw));
    c.insert("ck", &s!("cv"));

    let id = c.save_meta().unwrap();
    let restored: DagMapRawKey<Vec<u8>> = DagMapRawKey::from_meta(id).unwrap();

    assert_eq!(restored.get("ck").unwrap(), s!("cv"));
    assert_eq!(restored.get("pk").unwrap(), s!("pv"));
}
