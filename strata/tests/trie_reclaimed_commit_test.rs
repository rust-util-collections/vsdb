use vsdb::{MptCalc, SmtCalc, TrieCalc, VerMap, VerMapWithProof, VsdbError};

fn reclaimed_commit<T: TrieCalc>() {
    let mut proof = VerMapWithProof::<u64, u64, T>::new();
    let branch = proof.map().main_branch();
    proof.map_mut().insert(branch, &1, &100).unwrap();
    let first = proof.map_mut().commit(branch).unwrap();
    proof.map_mut().insert(branch, &1, &200).unwrap();
    let removed = proof.map_mut().commit(branch).unwrap();
    proof.merkle_root_at_commit(removed).unwrap();
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
