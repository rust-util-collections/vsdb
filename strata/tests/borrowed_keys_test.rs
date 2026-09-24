//! Lookups accept borrowed key forms (`&str` for `String`, `&[u8]` for
//! `Vec<u8>`), like `HashMap::get` / `BTreeMap::get`.

use vsdb::{Mapx, MapxOrd, VerMap, VsdbOptions, vsdb_configure};

fn setup() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let dir = format!("/tmp/vsdb_testing/borrowed_keys_{}", rand::random::<u64>());
        vsdb_configure(VsdbOptions::new(dir)).unwrap();
    });
}

#[test]
fn mapx_accepts_str_and_byte_slices() {
    setup();
    let mut m: Mapx<String, u32> = Mapx::new();
    m.insert(&"alpha".to_string(), &1);
    m.insert(&"beta".to_string(), &2);
    assert_eq!(m.get("alpha"), Some(1));
    assert!(m.contains_key("beta"));
    *m.get_mut("beta").unwrap() = 20;
    assert_eq!(m.get("beta"), Some(20));
    m.remove("alpha");
    assert_eq!(m.get("alpha"), None);
    // The owned form still works.
    assert_eq!(m.get(&"beta".to_string()), Some(20));

    let mut b: Mapx<Vec<u8>, u32> = Mapx::new();
    b.insert(&vec![1, 2, 3], &7);
    assert_eq!(b.get(&[1u8, 2, 3][..]), Some(7));
}

#[test]
fn mapx_ord_accepts_str_for_point_and_neighbor_lookups() {
    setup();
    let mut m: MapxOrd<String, u32> = MapxOrd::new();
    for (i, k) in ["a", "c", "e"].into_iter().enumerate() {
        m.insert(&k.to_string(), &(i as u32));
    }
    assert_eq!(m.get("c"), Some(1));
    assert_eq!(m.get_le("d"), Some(("c".to_string(), 1)));
    assert_eq!(m.get_ge("d"), Some(("e".to_string(), 2)));
    m.remove("a");
    assert!(!m.contains_key("a"));
}

#[test]
fn vermap_accepts_str_keys() {
    setup();
    let mut m: VerMap<String, u32> = VerMap::new();
    let main = m.main_branch();
    m.insert(main, &"k".to_string(), &1).unwrap();
    let c = m.commit(main).unwrap();
    assert_eq!(m.get(main, "k").unwrap(), Some(1));
    assert!(m.contains_key(main, "k").unwrap());
    m.remove(main, "k").unwrap();
    assert_eq!(m.get(main, "k").unwrap(), None);
    assert_eq!(m.at(c).unwrap().get("k"), Some(1));
}
