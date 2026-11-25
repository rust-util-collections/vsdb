//! A closed application universe written by VSDB 6ddaed9 on MMDB 4.3.15.
//! The fixture preserves its original engine files, application-owned handle
//! and expected query/proof outputs. No v18 collection conversion is needed.
use std::fs;
use vsdb::{
    CommitId, DagMapRaw, InstanceId, L2, MapxOrd, MptCalc, SlotDex64, VecDex, VerMap,
    VerMapWithProof, VsdbOptions, slotdex::Order, vsdb_configure,
};
use vsdb_migrate::stage_universe;

#[test]
fn unchanged_v18_collection_handles_open_after_physical_cutover() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("v4");
    let files: Vec<(String, Vec<u8>)> =
        postcard::from_bytes(include_bytes!("fixtures/collections-v18-mmdb4.postcard")).unwrap();
    for (name, bytes) in &files {
        let dest = source.join(name);
        fs::create_dir_all(dest.parent().unwrap()).unwrap();
        fs::write(dest, bytes).unwrap();
    }
    let out = dir.path().join("v5");
    stage_universe(&source, &out).unwrap();
    for (name, bytes) in &files {
        assert_eq!(fs::read(source.join(name)).unwrap(), *bytes, "{name}");
    }
    let base = out.join("base");
    vsdb_configure(VsdbOptions::new(&base)).unwrap();
    let oracle = fs::read(base.join("__CUSTOM__/oracle")).unwrap();
    type Oracle = (
        MapxOrd<u64, String>,
        InstanceId,
        CommitId,
        [u8; 32],
        InstanceId,
        InstanceId,
        Vec<u8>,
        InstanceId,
        InstanceId,
        Vec<(u64, u32)>,
    );
    let (embedded, version, commit, hash, parent, child, edge, slots, vecdex, original_hits):Oracle=postcard::from_bytes(&oracle).unwrap();
    assert_eq!(embedded.get(&42).as_deref(), Some("embedded"));
    let map = VerMap::<u64, u64>::from_meta(version).unwrap();
    let branch = map.main_branch();
    assert_eq!(map.head_commit(branch).unwrap().unwrap().id(), commit);
    assert_eq!(map.get(branch, &1).unwrap(), Some(21));
    let mut proof = VerMapWithProof::<_, _, MptCalc>::from_map(map);
    assert_eq!(proof.merkle_root_at_commit(commit).unwrap(), hash);
    proof.merkle_root(branch).unwrap();
    let key_proof = proof.prove_key(&1).unwrap();
    assert!(VerMapWithProof::<u64, u64, MptCalc>::verify_key_proof(&hash, &1, &key_proof).unwrap());
    let parent = DagMapRaw::from_meta(parent).unwrap();
    let child = DagMapRaw::from_meta(child).unwrap();
    assert_eq!(parent.child_id(&child).as_ref(), Some(&edge));
    assert_eq!(child.get(b"base").as_deref(), Some(b"ancestor".as_slice()));
    assert_eq!(child.get(b"own").as_deref(), Some(b"child".as_slice()));
    let slots = SlotDex64::<u64>::from_meta(slots).unwrap();
    assert_eq!(slots.page(.., 3, 0, Order::Asc), [10, 11, 30]);
    let graph = VecDex::<u64, L2>::from_meta(vecdex).unwrap();
    let hits: Vec<_> = graph
        .search(&[1.0, 0.0], 3)
        .unwrap()
        .into_iter()
        .map(|(k, d)| (k, d.to_bits()))
        .collect();
    assert_eq!(hits, original_hits);
}
