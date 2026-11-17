use vsdb::{MptCalc, SmtCalc, TrieCalc, VerMap, VerMapWithProof, VsdbError};

mod common;

fn reclaimed_commit<T: TrieCalc>() {
    common::setup();
    let mut proof = VerMapWithProof::<u64, u64, T>::new();
    let branch = proof.map().main_branch();
    proof.map_mut().insert(branch, &1, &100).unwrap();
    let first = proof.map_mut().commit(branch).unwrap();
    proof.map_mut().insert(branch, &1, &200).unwrap();
    let removed = proof.map_mut().commit(branch).unwrap();
    proof.merkle_root_at_commit(removed).unwrap();
    proof.save_cache(removed).unwrap();
    let id = proof.map().save_meta().unwrap();
    proof.map_mut().rollback_to(branch, first).unwrap();
    assert!(matches!(
        proof.map().at(removed),
        Err(VsdbError::CommitNotFound { .. })
    ));
    assert!(matches!(
        proof.merkle_root_at_commit(removed),
        Err(VsdbError::CommitNotFound { .. })
    ));
    drop(proof);
    // A well-formed disk cache can still refer to a reclaimed commit.
    let mut reopened =
        VerMapWithProof::<_, _, T>::from_map(VerMap::<u64, u64>::from_meta(id).unwrap());
    assert!(matches!(
        reopened.merkle_root_at_commit(removed),
        Err(VsdbError::CommitNotFound { .. })
    ));
    let expected = T::from_entries(reopened.map().at(first).unwrap().raw_iter())
        .unwrap()
        .root_hash()
        .unwrap();
    assert_eq!(reopened.merkle_root(branch).unwrap(), expected);
}

#[test]
fn cached_roots_do_not_resurrect_reclaimed_commits() {
    reclaimed_commit::<SmtCalc>();
    reclaimed_commit::<MptCalc>();
}

fn nonexistent_cached_commit_is_ignored<T: TrieCalc>() {
    common::setup();
    let map = VerMap::<u64, u64>::new();
    let map_id = map.instance_id().map_id;
    let sys_dir = map.namespace().system_dir();

    // Plant a valid cache stamped with future CommitId(1) before commit 1 exists.
    let mut foreign = T::default();
    foreign.insert(b"foreign-key", b"foreign-val").unwrap();
    foreign.save_cache(&sys_dir, map_id, 1).unwrap();

    let mut wrapper = VerMapWithProof::<u64, u64, T>::from_map(map);
    let main = wrapper.map().main_branch();
    wrapper.map_mut().insert(main, &7, &700).unwrap();
    let c1 = wrapper.map_mut().commit(main).unwrap();

    let expected_c1 = T::from_entries(wrapper.map().at(c1).unwrap().raw_iter())
        .unwrap()
        .root_hash()
        .unwrap();
    assert_eq!(wrapper.merkle_root_at_commit(c1).unwrap(), expected_c1);
    assert_eq!(wrapper.merkle_root(main).unwrap(), expected_c1);

    // Subsequent incremental commits must also build on the real c1 state.
    wrapper.map_mut().insert(main, &8, &800).unwrap();
    let c2 = wrapper.map_mut().commit(main).unwrap();
    let expected_c2 = T::from_entries(wrapper.map().at(c2).unwrap().raw_iter())
        .unwrap()
        .root_hash()
        .unwrap();
    assert_eq!(wrapper.merkle_root(main).unwrap(), expected_c2);
}

#[test]
fn construction_ignores_cache_for_nonexistent_or_reclaimed_commit() {
    nonexistent_cached_commit_is_ignored::<SmtCalc>();
    nonexistent_cached_commit_is_ignored::<MptCalc>();

    // Also verify `prove_key` / `prove_mpt` on a freshly reopened wrapper
    // cannot serve proofs from a reclaimed commit's surviving cache file.
    common::setup();
    let mut smt = VerMapWithProof::<u64, u64, SmtCalc>::new();
    let main = smt.map().main_branch();
    smt.map_mut().insert(main, &1, &100).unwrap();
    let c1 = smt.map_mut().commit(main).unwrap();
    smt.map_mut().insert(main, &2, &200).unwrap();
    let c2 = smt.map_mut().commit(main).unwrap();
    smt.save_cache(c2).unwrap();
    let id = smt.map().save_meta().unwrap();
    smt.map_mut().rollback_to(main, c1).unwrap();
    drop(smt);

    let reopened = VerMapWithProof::<u64, u64, SmtCalc>::from_map(
        VerMap::<u64, u64>::from_meta(id).unwrap(),
    );
    assert_eq!(reopened.prove_key(&2).unwrap().value(), None);
}
