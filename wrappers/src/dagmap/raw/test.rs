use super::*;

#[test]
fn dagmapraw_functions() {
    let mut i0 = DagMapRaw::new(&mut Orphan::new(None)).unwrap();
    i0.insert("k0", "v0");
    assert_eq!(i0.get("k0").unwrap().as_slice(), "v0".as_bytes());
    assert!(i0.get("k1").is_none());
    let mut i0 = Orphan::new(Some(i0));

    let mut i1 = DagMapRaw::new(&mut i0).unwrap();
    i1.insert("k1", "v1");
    assert_eq!(i1.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i1.get("k0").unwrap().as_slice(), "v0".as_bytes());
    let mut i1 = Orphan::new(Some(i1));

    let mut i2 = DagMapRaw::new(&mut i1).unwrap();
    i2.insert("k2", "v2");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
    i2.insert("k2", "v2x");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
    i2.insert("k1", "v1x");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0".as_bytes());
    i2.insert("k0", "v0x");
    assert_eq!(i2.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(i2.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(i2.get("k0").unwrap().as_slice(), "v0x".as_bytes());

    assert!(i1.get_value().unwrap().get("k2").is_none());
    assert_eq!(
        i1.get_value().unwrap().get("k1").unwrap().as_slice(),
        "v1".as_bytes()
    );
    assert_eq!(
        i1.get_value().unwrap().get("k0").unwrap().as_slice(),
        "v0".as_bytes()
    );

    assert!(i0.get_value().unwrap().get("k2").is_none());
    assert!(i0.get_value().unwrap().get("k1").is_none());
    assert_eq!(
        i0.get_value().unwrap().get("k0").unwrap().as_slice(),
        "v0".as_bytes()
    );

    let mut head = pnk!(i2.prune());
    sleep_ms!(1000); // give some time to the async cleaner

    assert_eq!(head.get("k2").unwrap().as_slice(), "v2x".as_bytes());
    assert_eq!(head.get("k1").unwrap().as_slice(), "v1x".as_bytes());
    assert_eq!(head.get("k0").unwrap().as_slice(), "v0x".as_bytes());

    assert!(i1.get_value().is_none());
    assert!(i1.get_value().is_none());
    assert!(i1.get_value().is_none());

    assert!(i0.get_value().is_none());
    assert!(i0.get_value().is_none());
    assert!(i0.get_value().is_none());

    // prune with deep stack
    for i in 10u8..=255 {
        head.insert(i.to_be_bytes(), i.to_be_bytes());
        head = DagMapRaw::new(&mut Orphan::new(Some(head))).unwrap();
    }

    let mut head = pnk!(head.prune());
    sleep_ms!(1000); // give some time to the async cleaner
    assert!(head.parent.get_value().is_none());
    assert!(head.children.is_empty());

    for i in 10u8..=255 {
        assert_eq!(
            head.get(i.to_be_bytes()).unwrap().as_slice(),
            i.to_be_bytes()
        );
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
