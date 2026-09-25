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
fn mapx_array_slice_queries_use_array_encoding() {
    setup();
    let mut map: Mapx<[u8; 2], u32> = Mapx::new();
    let key = [1, 0];
    map.insert(&key, &7);

    assert_eq!(map.get(key.as_slice()), Some(7));
    assert!(map.contains_key(key.as_slice()));
    *map.get_mut(key.as_slice()).unwrap() = 8;
    assert_eq!(map.get(&key), Some(8));
    map.remove(key.as_slice());
    assert!(!map.contains_key(&key));
}

#[test]
fn mapx_wrong_length_slices_cannot_address_array_keys() {
    setup();
    let mut map: Mapx<[u8; 2], u32> = Mapx::new();
    // Postcard encodes the slice [0] as [1, 0], the stored array key.
    let key = [1, 0];
    map.insert(&key, &7);
    for query in [&[0][..], &[][..], &[1, 0, 0][..]] {
        assert_eq!(map.get(query), None);
        assert!(!map.contains_key(query));
        assert!(map.get_mut(query).is_none());
        map.remove(query);
        assert_eq!(map.get(&key), Some(7));
    }
}

#[test]
fn mapx_empty_array_slice_queries() {
    setup();
    let mut map: Mapx<[u8; 0], u32> = Mapx::new();
    map.insert(&[], &7);
    assert_eq!(map.get(&[][..]), Some(7));
    assert_eq!(map.get(&[0][..]), None);
    map.remove(&[0][..]);
    assert!(map.contains_key(&[][..]));
    *map.get_mut(&[][..]).unwrap() = 8;
    assert_eq!(map.get(&[]), Some(8));
    map.remove(&[][..]);
    assert!(map.iter().next().is_none());
}

#[test]
fn mapx_vec_and_box_slice_queries_keep_length_prefixes() {
    setup();
    let mut vec_map: Mapx<Vec<u8>, u32> = Mapx::new();
    let mut box_map: Mapx<Box<[u8]>, u32> = Mapx::new();
    for key in [vec![], vec![0], vec![1, 0], vec![255; 128]] {
        let boxed = key.clone().into_boxed_slice();
        vec_map.insert(&key, &7);
        box_map.insert(&boxed, &7);
        assert_eq!(vec_map.get(key.as_slice()), Some(7));
        assert_eq!(box_map.get(key.as_slice()), Some(7));
        assert!(vec_map.contains_key(key.as_slice()));
        assert!(box_map.contains_key(key.as_slice()));
        *vec_map.get_mut(key.as_slice()).unwrap() = 8;
        *box_map.get_mut(key.as_slice()).unwrap() = 8;
        assert_eq!(vec_map.get(&key), Some(8));
        assert_eq!(box_map.get(&boxed), Some(8));
        vec_map.remove(key.as_slice());
        box_map.remove(key.as_slice());
        assert!(vec_map.iter().next().is_none());
        assert!(box_map.iter().next().is_none());
    }
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
