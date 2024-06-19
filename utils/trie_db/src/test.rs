#![warn(warnings)]

use super::*;
use std::collections::BTreeMap;

#[test]
fn trie_db_destroy_and_prune() {
    let mut s = MptStore::new();
    let mut hdr = pnk!(s.trie_init(&[0]));

    pnk!(hdr.insert(b"k", b"v0"));
    assert_eq!(b"v0", pnk!(hdr.get(b"k")).unwrap().as_slice());
    let mut hdr0 = hdr.commit().unwrap();
    let root0 = hdr0.root();

    pnk!(hdr0.insert(b"k", b"v1"));
    assert_eq!(b"v1", pnk!(hdr0.get(b"k")).unwrap().as_slice());
    let mut hdr1 = hdr0.commit().unwrap();
    let root1 = hdr1.root();

    pnk!(s.trie_rederive(&[0], root0));

    pnk!(hdr1.insert(b"k", b"V2"));
    assert_eq!(b"V2", pnk!(hdr1.get(b"k")).unwrap().as_slice());
    let mut hdr2 = hdr1.commit().unwrap();
    let root2 = hdr2.root();
    assert_eq!(b"V2", pnk!(hdr2.get(b"k")).unwrap().as_slice());

    pnk!(s.trie_rederive(&[0], root0));
    pnk!(s.trie_rederive(&[0], root1));

    pnk!(hdr2.insert(b"k", b"V3"));
    assert_eq!(b"V3", pnk!(hdr2.get(b"k")).unwrap().as_slice());
    let mut hdr3 = hdr2.commit().unwrap();
    let root3 = hdr3.root();
    assert_eq!(b"V3", pnk!(hdr3.get(b"k")).unwrap().as_slice());

    pnk!(s.trie_rederive(&[0], root2));

    pnk!(hdr3.insert(b"k", b"V4"));
    assert_eq!(b"V4", pnk!(hdr3.get(b"k")).unwrap().as_slice());
    let hdr4 = hdr3.commit().unwrap();
    let root4 = hdr4.root();
    assert_eq!(b"V4", pnk!(hdr4.get(b"k")).unwrap().as_slice());

    assert_ne!(root3, root2);
    assert_ne!(root3, root4);

    pnk!(s.trie_rederive(&[0], root0));
    pnk!(s.trie_rederive(&[0], root1));
    pnk!(s.trie_rederive(&[0], root2));
    pnk!(s.trie_rederive(&[0], root3));
    pnk!(s.trie_rederive(&[0], root4));

    let mut hdr = pnk!(s.trie_rederive(&[0], root3));
    assert_eq!(b"V3", pnk!(hdr.get(b"k")).unwrap().as_slice());

    pnk!(hdr.insert(b"k", b"v5"));
    assert_eq!(b"v5", pnk!(hdr.get(b"k")).unwrap().as_slice());
    let mut hdr5 = hdr.commit().unwrap();
    let root5 = hdr5.root();
    assert_eq!(b"v5", pnk!(hdr5.get(b"k")).unwrap().as_slice());

    pnk!(hdr5.insert(b"k", b"v6"));
    assert_eq!(b"v6", pnk!(hdr5.get(b"k")).unwrap().as_slice());
    let hdr6 = hdr5.commit().unwrap();
    let root6 = hdr6.root();
    assert_eq!(b"v6", pnk!(hdr6.get(b"k")).unwrap().as_slice());

    pnk!(s.trie_prune(&[0], root3));

    assert!(s.trie_rederive(&[0], root0).is_err());
    assert!(s.trie_rederive(&[0], root1).is_err());
    assert!(s.trie_rederive(&[0], root2).is_err());

    assert_eq!(
        b"V3",
        pnk!(pnk!(s.trie_rederive(&[0], root3)).get(b"k"))
            .unwrap()
            .as_slice()
    );
    assert_eq!(
        b"V4",
        pnk!(pnk!(s.trie_rederive(&[0], root4)).get(b"k"))
            .unwrap()
            .as_slice()
    );
    assert_eq!(
        b"v5",
        pnk!(pnk!(s.trie_rederive(&[0], root5)).get(b"k"))
            .unwrap()
            .as_slice()
    );
    assert_eq!(
        b"v6",
        pnk!(pnk!(s.trie_rederive(&[0], root6)).get(b"k"))
            .unwrap()
            .as_slice()
    );

    pnk!(s.trie_prune(&[0], root4));

    assert_eq!(
        b"V4",
        pnk!(pnk!(s.trie_rederive(&[0], root4)).get(b"k"))
            .unwrap()
            .as_slice()
    );

    assert!(s.trie_rederive(&[0], root0).is_err());
    assert!(s.trie_rederive(&[0], root1).is_err());
    assert!(s.trie_rederive(&[0], root2).is_err());
    assert!(s.trie_rederive(&[0], root3).is_err());
    assert!(s.trie_rederive(&[0], root5).is_err());
    assert!(s.trie_rederive(&[0], root6).is_err());

    s.trie_destroy(&[0]);

    assert!(s.trie_rederive(&[0], root0).is_err());
    assert!(s.trie_rederive(&[0], root1).is_err());
    assert!(s.trie_rederive(&[0], root2).is_err());
    assert!(s.trie_rederive(&[0], root3).is_err());
    assert!(s.trie_rederive(&[0], root4).is_err());
    assert!(s.trie_rederive(&[0], root5).is_err());
    assert!(s.trie_rederive(&[0], root6).is_err());
}

#[test]
fn trie_db_rederive() {
    let mut s = MptStore::new();
    let mut hdr = pnk!(s.trie_init(b""));

    pnk!(hdr.insert(b"key", b"value"));
    assert_eq!(b"value", pnk!(hdr.get(b"key")).unwrap().as_slice());

    let hdr = hdr.commit().unwrap();
    let root = hdr.root();
    assert_eq!(root, hdr.root());

    let hdr_encoded = hdr.encode();
    drop(hdr);

    let mut hdr = pnk!(MptOnce::decode(&hdr_encoded));
    assert_eq!(b"value", pnk!(hdr.get(b"key")).unwrap().as_slice());
    assert_eq!(root, hdr.root());

    pnk!(hdr.insert(b"key1", b"value1"));
    assert_eq!(b"value1", pnk!(hdr.get(b"key1")).unwrap().as_slice());

    let old_hdr_ro = pnk!(hdr.ro_handle(root));
    assert_eq!(root, old_hdr_ro.root());
    assert_eq!(b"value", pnk!(old_hdr_ro.get(b"key")).unwrap().as_slice());
    assert!(pnk!(old_hdr_ro.get(b"key1")).is_none());

    let hdr = hdr.commit().unwrap();
    let new_root = hdr.root();
    assert_eq!(new_root, hdr.root());
}

#[test]
fn trie_db_iter() {
    let mut s = MptStore::new();
    let mut hdr = pnk!(s.trie_init(b"backend_key"));
    assert!(hdr.is_empty());

    /////////////////////////////////////////////

    let samples = (0u8..200).map(|i| ([i], [i])).collect::<Vec<_>>();
    samples.iter().for_each(|(k, v)| {
        pnk!(hdr.insert(k, v));
    });

    let mut hdr = hdr.commit().unwrap();
    let root = hdr.root();

    let ro_hdr = hdr.ro_handle(root).unwrap();
    let bt = ro_hdr
        .iter()
        .map(|i| i.unwrap())
        .collect::<BTreeMap<_, _>>();

    bt.iter().enumerate().for_each(|(i, (k, v))| {
        assert_eq!(&[i as u8], k.as_slice());
        assert_eq!(k, v);
    });

    let keylist = ro_hdr.key_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
    assert_eq!(keylist, bt.keys().cloned().collect::<Vec<_>>());

    /////////////////////////////////////////////

    let samples = (0u8..200).map(|i| ([i], [i + 1])).collect::<Vec<_>>();
    samples.iter().for_each(|(k, v)| {
        pnk!(hdr.insert(k, v));
    });

    let mut hdr = hdr.commit().unwrap();
    let root = hdr.root();

    let ro_hdr = hdr.ro_handle(root).unwrap();
    let bt = ro_hdr
        .iter()
        .map(|i| i.unwrap())
        .collect::<BTreeMap<_, _>>();

    bt.iter().enumerate().for_each(|(i, (k, v))| {
        assert_eq!(&[i as u8], k.as_slice());
        assert_eq!(&[k[0] + 1], v.as_slice());
    });

    let keylist = ro_hdr.key_iter().map(|i| i.unwrap()).collect::<Vec<_>>();
    assert_eq!(keylist, bt.keys().cloned().collect::<Vec<_>>());
    assert!(!hdr.is_empty());

    /////////////////////////////////////////////

    assert!(!hdr.is_empty());
    hdr.clear().unwrap();
    assert!(hdr.is_empty());
}
