use super::*;

#[test]
fn test_compare() {
    assert_eq!(Orphan::new(0), 0);
    assert!(Orphan::new(111) > 0);
    assert!(Orphan::new(111) >= 0);
    assert!(Orphan::new(0) < 111);
    assert!(Orphan::new(0) <= 111);
}

#[test]
fn test_calc() {
    assert_eq!(Orphan::new(111) + 111, 222);
    assert_eq!(Orphan::new(111) - 111, 0);
    assert_eq!(Orphan::new(111) * 111, 111 * 111);
    assert_eq!(Orphan::new(111) / 2, 55);
    assert_eq!(Orphan::new(111) % 2, 1);

    assert_eq!(-Orphan::new(111), -111);
    assert_eq!(!Orphan::new(111), !111);

    assert_eq!(Orphan::new(111) >> 2, 111 >> 2);
    assert_eq!(Orphan::new(111) << 2, 111 << 2);

    assert_eq!(Orphan::new(111) | 2, 111 | 2);
    assert_eq!(Orphan::new(111) & 2, 111 & 2);
    assert_eq!(Orphan::new(111) ^ 2, 111 ^ 2);
}
#[test]
fn test_mut() {
    let mut v = Orphan::new(1);
    v += 1;
    assert_eq!(v, 2);
    v *= 100;
    assert_eq!(v, 200);
    v -= 1;
    assert_eq!(v, 199);
    v /= 10;
    assert_eq!(v, 19);
    v %= 10;
    assert_eq!(v, 9);

    *v.get_mut() = -v.get_value();
    assert_eq!(v, -9);
}

#[test]
fn custom_types() {
    #[derive(
        Default, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize,
    )]
    struct Foo {
        a: i32,
        b: String,
        c: bool,
    }

    assert_eq!(Orphan::new(Foo::default()), Foo::default());
    assert_eq!(Orphan::new(Foo::default()), Orphan::new(Foo::default()));

    assert!(
        Orphan::new(Foo::default())
            < Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );
    assert!(
        Orphan::new(Foo::default())
            <= Foo {
                a: 1,
                b: "".to_string(),
                c: true
            }
    );

    assert!(Orphan::new(Foo::default()) >= Foo::default());
    assert!(Orphan::new(Foo::default()) >= Orphan::new(Foo::default()));
}

#[test]
fn test_save_and_from_meta() {
    let o = Orphan::new(42i64);

    let id = o.save_meta().unwrap();
    assert_eq!(id, o.instance_id());

    let restored: Orphan<i64> = Orphan::from_meta(id).unwrap();
    assert_eq!(restored.get_value(), 42);
    assert!(restored.is_the_same_instance(&o));
}

/// Postcard serde roundtrip for Orphan.
#[test]
fn test_serde_roundtrip() {
    let o = Orphan::new(999u64);
    let bytes = postcard::to_allocvec(&o).unwrap();
    let restored: Orphan<u64> = postcard::from_bytes(&bytes).unwrap();
    assert!(restored.is_the_same_instance(&o));
    assert_eq!(restored.get_value(), 999);
}

/// Serialized size should be minimal.
#[test]
fn test_serde_size() {
    let o = Orphan::new(0u32);
    let bytes = postcard::to_allocvec(&o).unwrap();
    assert!(bytes.len() <= 20, "expected ≤20 bytes, got {}", bytes.len());
}

/// from_meta with nonexistent ID.
#[test]
fn test_from_meta_nonexistent() {
    assert!(Orphan::<u64>::from_meta(u64::MAX).is_err());
}

/// Mutate after meta restore.
#[test]
fn test_meta_restore_then_mutate() {
    let o = Orphan::new(10i32);
    let id = o.save_meta().unwrap();

    let mut restored: Orphan<i32> = Orphan::from_meta(id).unwrap();
    *restored.get_mut() = 42;

    assert_eq!(o.get_value(), 42);
}

/// ValueEnDe roundtrip for Orphan.
#[test]
fn test_valueende_roundtrip() {
    let o = Orphan::new("hello".to_string());
    let encoded = o.encode();
    let decoded: Orphan<String> = Orphan::decode(&encoded).unwrap();
    assert!(decoded.is_the_same_instance(&o));
    assert_eq!(decoded.get_value(), "hello".to_string());
}

/// Orphan holding a complex type (Mapx inside).
#[test]
fn test_orphan_of_mapx_serde_roundtrip() {
    use crate::basic::mapx::Mapx;
    let mut m: Mapx<u32, String> = Mapx::new();
    m.insert(&1, &"one".into());
    m.insert(&2, &"two".into());

    let o: Orphan<Mapx<u32, String>> = Orphan::new(m);
    let bytes = postcard::to_allocvec(&o).unwrap();
    let restored: Orphan<Mapx<u32, String>> = postcard::from_bytes(&bytes).unwrap();

    let inner = restored.get_value();
    assert_eq!(inner.get(&1), Some("one".into()));
    assert_eq!(inner.get(&2), Some("two".into()));
}
