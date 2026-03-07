use super::*;

macro_rules! s {
    ($i: expr) => {{ $i.as_bytes().to_vec() }};
}

#[test]
fn dagmaprawkey_functions() {
    let mut i0 = DagMapRawKey::new(&mut Orphan::new(None)).unwrap();
    i0.insert("k0", &s!("v0"));
    assert_eq!(i0.get("k0").unwrap(), s!("v0"));
    assert!(i0.get("k1").is_none());
    let mut i0_raw = Orphan::new(Some(i0.into_inner()));

    let mut i1 = DagMapRawKey::new(&mut i0_raw).unwrap();
    i1.insert("k1", &s!("v1"));
    assert_eq!(i1.get("k1").unwrap(), s!("v1"));
    assert_eq!(i1.get("k0").unwrap(), s!("v0"));
    let mut i1_raw = Orphan::new(Some(i1.into_inner()));

    let mut i2 = DagMapRawKey::new(&mut i1_raw).unwrap();
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

    assert!(i1_raw.get_value().unwrap().get("k2").is_none());
    assert_eq!(
        i1_raw.get_value().unwrap().get("k1").unwrap(),
        s!("v1").encode()
    );
    assert_eq!(
        i1_raw.get_value().unwrap().get("k0").unwrap(),
        s!("v0").encode()
    );

    assert!(i0_raw.get_value().unwrap().get("k2").is_none());
    assert!(i0_raw.get_value().unwrap().get("k1").is_none());
    assert_eq!(
        i0_raw.get_value().unwrap().get("k0").unwrap(),
        s!("v0").encode()
    );

    let mut head = pnk!(i2.prune());
    sleep_ms!(1000); // give some time to the async cleaner

    assert_eq!(head.get("k2").unwrap(), s!("v2x"));
    assert_eq!(head.get("k1").unwrap(), s!("v1x"));
    assert_eq!(head.get("k0").unwrap(), s!("v0x"));

    assert!(i1_raw.get_value().is_none());
    assert!(i1_raw.get_value().is_none());
    assert!(i1_raw.get_value().is_none());

    assert!(i0_raw.get_value().is_none());
    assert!(i0_raw.get_value().is_none());
    assert!(i0_raw.get_value().is_none());

    // prune with deep stack
    for i in 10u8..=255 {
        head.insert(i.to_be_bytes(), &i.to_be_bytes().to_vec());
        head = DagMapRawKey::new(&mut Orphan::new(Some(head.into_inner()))).unwrap();
    }

    let mut head = pnk!(head.prune());
    sleep_ms!(1000); // give some time to the async cleaner

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
