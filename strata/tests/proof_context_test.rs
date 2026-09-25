use vsdb::{BranchId, MptCalc, SmtCalc, VerMapWithProof, VsdbError};

macro_rules! context_test {
    ($name:ident, $calc:ty) => {
        #[test]
        fn $name() {
            type State = VerMapWithProof<u64, String, $calc>;
            let mut state = State::new();
            let main = state.map().main_branch();
            // Empty states and dirty branches require no preliminary root query.
            let empty = state.prove_at(main, &1).unwrap();
            assert_eq!(empty.proof.value(), None);
            assert!(
                State::verify_key_proof(&empty.root_hash, &1, &empty.proof).unwrap()
            );
            state.map_mut().insert(main, &1, &"base".into()).unwrap();
            let base = state.map_mut().commit(main).unwrap();
            let side = state.map_mut().create_branch("side", main).unwrap();
            state.map_mut().insert(main, &1, &"current".into()).unwrap();
            let later = state.map_mut().commit(main).unwrap();
            state.map_mut().insert(main, &2, &"pending".into()).unwrap();
            let dirty = state.prove_at(main, &2).unwrap();
            assert_eq!(
                dirty.proof.value(),
                Some(postcard::to_allocvec("pending").unwrap().as_slice())
            );
            assert!(
                State::verify_key_proof(&dirty.root_hash, &2, &dirty.proof).unwrap()
            );

            state.save_cache(base).unwrap();
            let current = state.prove_at(main, &1).unwrap();
            assert_eq!(current.root_hash, dirty.root_hash);
            assert_eq!(
                current.proof.value(),
                Some(postcard::to_allocvec("current").unwrap().as_slice())
            );
            assert!(
                State::verify_key_proof(&current.root_hash, &1, &current.proof).unwrap()
            );
            let side_view = state.prove_at(side, &1).unwrap();
            assert_ne!(side_view.root_hash, current.root_hash);
            assert_eq!(
                side_view.proof.value(),
                Some(postcard::to_allocvec("base").unwrap().as_slice())
            );
            let historical = state.prove_at_commit(base, &2).unwrap();
            assert_eq!(historical.root_hash, side_view.root_hash);
            assert_eq!(historical.proof.value(), None);
            assert!(
                State::verify_key_proof(&historical.root_hash, &2, &historical.proof)
                    .unwrap()
            );
            assert_eq!(state.prove_at(main, &2).unwrap(), dirty);

            let wire = postcard::to_allocvec(&current).unwrap();
            assert_eq!(
                postcard::from_bytes::<vsdb::ProofWithRoot<_>>(&wire).unwrap(),
                current
            );
            state.map_mut().discard(main).unwrap();
            state.map_mut().rollback_to(main, base).unwrap();
            assert!(matches!(
                state.prove_at_commit(later, &1),
                Err(VsdbError::CommitNotFound { .. })
            ));
            assert!(matches!(
                state.prove_at(BranchId::from_raw(u64::MAX), &1),
                Err(VsdbError::BranchNotFound { .. })
            ));
            // Previously returned proof/root pairs remain usable after rollback.
            assert!(
                State::verify_key_proof(&current.root_hash, &1, &current.proof).unwrap()
            );
        }
    };
}

context_test!(mpt_proofs_select_the_requested_state, MptCalc);
context_test!(smt_proofs_select_the_requested_state, SmtCalc);
