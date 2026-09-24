use std::fs;
use vsdb::{MptCalc, SmtCalc, TrieCalc, VerMap, VerMapWithProof};

fn checkpoints<T: TrieCalc>(kind: &str) {
    let mut proof = VerMapWithProof::<u64, u64, T>::new();
    let branch = proof.map().main_branch();
    let id = proof.map().save_meta().unwrap();
    let dir = proof.map().namespace().system_dir();
    let path = dir.join(format!("{kind}_cache_{}.bin", id.map_id));
    proof.map_mut().insert(branch, &1, &100).unwrap();
    let first = proof.map_mut().commit(branch).unwrap();
    let first_root = proof.merkle_root(branch).unwrap();
    assert!(
        !path.exists(),
        "root calculation must not write a full cache"
    );

    proof.save_cache(first).unwrap();
    let checkpoint = fs::read(&path).unwrap();
    let (_, tag, root) = T::load_cache(&dir, id.map_id).unwrap();
    assert_eq!(tag, first.raw());
    assert_eq!(root, first_root);

    proof.map_mut().insert(branch, &1, &200).unwrap();
    let second = proof.map_mut().commit(branch).unwrap();
    let second_root = proof.merkle_root(branch).unwrap();
    assert_ne!(first_root, second_root);
    assert_eq!(fs::read(&path).unwrap(), checkpoint);
    drop(proof);
    assert_eq!(
        fs::read(&path).unwrap(),
        checkpoint,
        "Drop must not perform hidden I/O"
    );

    // An intentionally stale checkpoint must catch up to the authoritative map.
    let mut proof =
        VerMapWithProof::<_, _, T>::from_map(VerMap::<u64, u64>::from_meta(id).unwrap());
    assert_eq!(proof.merkle_root(branch).unwrap(), second_root);
    proof.map_mut().insert(branch, &1, &300).unwrap();
    let dirty_root = proof.merkle_root(branch).unwrap();
    assert_ne!(dirty_root, second_root);
    proof.save_cache(second).unwrap();
    assert_eq!(proof.map().get(branch, &1).unwrap(), Some(300));
    assert_eq!(T::load_cache(&dir, id.map_id).unwrap().2, second_root);
    assert_eq!(proof.merkle_root(branch).unwrap(), dirty_root);

    // I/O failure is observable and is not silently retried by Drop.
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(proof.save_cache(second).is_err());
    fs::remove_dir(&path).unwrap();
    drop(proof);
    assert!(!path.exists());
}

#[test]
fn explicit_checkpoints_preserve_incremental_reads_and_surface_io_failures() {
    checkpoints::<SmtCalc>("smt");
    checkpoints::<MptCalc>("mpt");
}
