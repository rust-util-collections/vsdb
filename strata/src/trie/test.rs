#[cfg(test)]
mod tests {
    use crate::trie::MptCalc;
    use std::fs;

    #[test]
    fn test_simple_insert_get() {
        let mut trie = MptCalc::new();

        trie.insert(b"hello", b"world").unwrap();
        assert_eq!(trie.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(trie.get(b"hell").unwrap(), None);

        trie.insert(b"foo", b"bar").unwrap();
        assert_eq!(trie.get(b"foo").unwrap(), Some(b"bar".to_vec()));

        trie.remove(b"hello").unwrap();
        assert_eq!(trie.get(b"hello").unwrap(), None);
        assert_eq!(trie.get(b"foo").unwrap(), Some(b"bar".to_vec()));
    }

    #[test]
    fn test_branch_split() {
        let mut trie = MptCalc::new();

        // "do", "dog" -> Split leaf into Branch
        trie.insert(b"dog", b"puppy").unwrap();
        trie.insert(b"do", b"verb").unwrap();

        assert_eq!(trie.get(b"dog").unwrap(), Some(b"puppy".to_vec()));
        assert_eq!(trie.get(b"do").unwrap(), Some(b"verb".to_vec()));
    }

    #[test]
    fn test_extension_split() {
        let mut trie = MptCalc::new();

        // "abc", "abd" -> Extension "ab", Branch at 'c'/'d'
        trie.insert(b"abc", b"1").unwrap();
        trie.insert(b"abd", b"2").unwrap();

        assert_eq!(trie.get(b"abc").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"abd").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_clone_preserves_state() {
        let mut trie = MptCalc::new();
        trie.insert(b"key1", b"val1").unwrap();
        trie.insert(b"key2", b"val2").unwrap();
        trie.insert(b"key3", b"val3").unwrap();

        // Clone preserves the full in-memory trie.
        let trie2 = trie.clone();
        assert_eq!(trie2.get(b"key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(trie2.get(b"key2").unwrap(), Some(b"val2".to_vec()));
        assert_eq!(trie2.get(b"key3").unwrap(), Some(b"val3".to_vec()));
        assert_eq!(trie2.get(b"key4").unwrap(), None);
    }

    #[test]
    fn test_overwrite_value() {
        let mut trie = MptCalc::new();

        trie.insert(b"key", b"val1").unwrap();
        assert_eq!(trie.get(b"key").unwrap(), Some(b"val1".to_vec()));

        trie.insert(b"key", b"val2").unwrap();
        assert_eq!(trie.get(b"key").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn test_remove_all_keys() {
        let mut trie = MptCalc::new();

        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();

        trie.remove(b"a").unwrap();
        trie.remove(b"b").unwrap();

        assert_eq!(trie.get(b"a").unwrap(), None);
        assert_eq!(trie.get(b"b").unwrap(), None);

        // Root should be the empty root
        assert_eq!(trie.root_hash().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let mut trie = MptCalc::new();

        trie.insert(b"exists", b"yes").unwrap();
        let root_before = trie.root_hash().unwrap();

        trie.remove(b"does_not_exist").unwrap();
        let root_after = trie.root_hash().unwrap();

        // Root should not change
        assert_eq!(root_before, root_after);
        assert_eq!(trie.get(b"exists").unwrap(), Some(b"yes".to_vec()));
    }

    #[test]
    fn test_remove_nonexistent_preserves_cached_root() {
        // After root_hash() the internal nodes are Cached. Removing keys that
        // don't exist walks Branch/Extension/Leaf no-change paths, which must
        // re-wrap nodes while preserving their precomputed hashes. A wrong
        // preserved hash would corrupt the committed root.
        let mut trie = MptCalc::new();
        for i in 0u32..200 {
            trie.insert(&i.to_be_bytes(), &(i * 11).to_be_bytes())
                .unwrap();
        }
        let root = trie.root_hash().unwrap(); // commit → Cached nodes

        for k in [
            b"missing".as_slice(),
            &1000u32.to_be_bytes(),
            &54321u32.to_be_bytes(),
        ] {
            trie.remove(k).unwrap();
        }

        // Root must be unchanged and equal a fresh rebuild of the same data.
        assert_eq!(trie.root_hash().unwrap(), root);
        let mut fresh = MptCalc::new();
        for i in 0u32..200 {
            fresh
                .insert(&i.to_be_bytes(), &(i * 11).to_be_bytes())
                .unwrap();
        }
        assert_eq!(fresh.root_hash().unwrap(), root);

        // All original entries must still be present.
        for i in 0u32..200 {
            assert_eq!(
                trie.get(&i.to_be_bytes()).unwrap(),
                Some((i * 11).to_be_bytes().to_vec())
            );
        }
    }

    #[test]
    fn test_batch_update() {
        let mut trie = MptCalc::new();

        trie.batch_update(&[
            (b"k1".as_slice(), Some(b"v1".as_slice())),
            (b"k2".as_slice(), Some(b"v2".as_slice())),
            (b"k3".as_slice(), Some(b"v3".as_slice())),
        ])
        .unwrap();

        assert_eq!(trie.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(trie.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(trie.get(b"k3").unwrap(), Some(b"v3".to_vec()));

        // Batch with mixed insert/remove
        trie.batch_update(&[
            (b"k1".as_slice(), None),
            (b"k4".as_slice(), Some(b"v4".as_slice())),
        ])
        .unwrap();

        assert_eq!(trie.get(b"k1").unwrap(), None);
        assert_eq!(trie.get(b"k4").unwrap(), Some(b"v4".to_vec()));
    }

    #[test]
    fn test_deterministic_root() {
        let mut trie_a = MptCalc::new();
        trie_a.insert(b"x", b"1").unwrap();
        trie_a.insert(b"y", b"2").unwrap();
        trie_a.insert(b"z", b"3").unwrap();

        // Insert in different order, same data
        let mut trie_b = MptCalc::new();
        trie_b.insert(b"z", b"3").unwrap();
        trie_b.insert(b"x", b"1").unwrap();
        trie_b.insert(b"y", b"2").unwrap();

        assert_eq!(trie_a.root_hash().unwrap(), trie_b.root_hash().unwrap());
    }

    #[test]
    fn test_empty_key_and_value() {
        let mut trie = MptCalc::new();

        trie.insert(b"", b"empty_key").unwrap();
        assert_eq!(trie.get(b"").unwrap(), Some(b"empty_key".to_vec()));

        trie.insert(b"nonempty", b"").unwrap();
        assert_eq!(trie.get(b"nonempty").unwrap(), Some(b"".to_vec()));
    }

    #[test]
    fn test_many_keys() {
        let mut trie = MptCalc::new();

        for i in 0u32..100 {
            let key = i.to_be_bytes();
            let val = (i * 2).to_be_bytes();
            trie.insert(&key, &val).unwrap();
        }

        for i in 0u32..100 {
            let key = i.to_be_bytes();
            let val = (i * 2).to_be_bytes();
            assert_eq!(trie.get(&key).unwrap(), Some(val.to_vec()));
        }

        // Remove half
        for i in (0u32..100).step_by(2) {
            let key = i.to_be_bytes();
            trie.remove(&key).unwrap();
        }

        for i in 0u32..100 {
            let key = i.to_be_bytes();
            if i % 2 == 0 {
                assert_eq!(trie.get(&key).unwrap(), None);
            } else {
                let val = (i * 2).to_be_bytes();
                assert_eq!(trie.get(&key).unwrap(), Some(val.to_vec()));
            }
        }
    }

    #[test]
    fn test_from_entries() {
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
        ];

        let trie = MptCalc::from_entries(entries).unwrap();
        assert_eq!(trie.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(trie.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_from_entries_matches_incremental() {
        let entries = vec![
            (b"x".to_vec(), b"1".to_vec()),
            (b"y".to_vec(), b"2".to_vec()),
            (b"z".to_vec(), b"3".to_vec()),
        ];
        let mut from_entries = MptCalc::from_entries(entries).unwrap();

        let mut incremental = MptCalc::new();
        incremental.insert(b"x", b"1").unwrap();
        incremental.insert(b"y", b"2").unwrap();
        incremental.insert(b"z", b"3").unwrap();

        assert_eq!(
            from_entries.root_hash().unwrap(),
            incremental.root_hash().unwrap()
        );
    }

    #[test]
    fn test_cache_save_load_roundtrip() {
        let cache_id = 1001;
        let _cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"hello", b"world").unwrap();
        trie.insert(b"foo", b"bar").unwrap();
        trie.insert(b"key", b"value").unwrap();
        let root = trie.root_hash().unwrap();

        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 42)
            .unwrap();

        let (loaded, sync_tag, loaded_root) =
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        assert_eq!(sync_tag, 42);
        assert_eq!(loaded_root, root);
        assert_eq!(loaded.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(loaded.get(b"foo").unwrap(), Some(b"bar".to_vec()));
        assert_eq!(loaded.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(loaded.get(b"missing").unwrap(), None);
    }

    #[test]
    fn test_cache_incremental_after_load() {
        let cache_id = 1002;
        let _cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 1)
            .unwrap();

        // Load and apply incremental changes.
        let (mut loaded, _, _) =
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        loaded.insert(b"c", b"3").unwrap();
        loaded.remove(b"a").unwrap();

        assert_eq!(loaded.get(b"a").unwrap(), None);
        assert_eq!(loaded.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(loaded.get(b"c").unwrap(), Some(b"3".to_vec()));

        // Root hash should match a fresh build with the same data.
        let mut fresh = MptCalc::new();
        fresh.insert(b"b", b"2").unwrap();
        fresh.insert(b"c", b"3").unwrap();
        assert_eq!(loaded.root_hash().unwrap(), fresh.root_hash().unwrap());
    }

    #[test]
    fn test_cache_empty_trie() {
        let cache_id = 1003;
        let _cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 0)
            .unwrap();

        let (loaded, sync_tag, root_hash) =
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        assert_eq!(sync_tag, 0);
        assert_eq!(root_hash, vec![0u8; 32]);
        assert_eq!(loaded.get(b"anything").unwrap(), None);
    }

    #[test]
    fn test_cache_many_keys() {
        let cache_id = 1004;
        let _cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        for i in 0u32..200 {
            trie.insert(&i.to_be_bytes(), &(i * 3).to_be_bytes())
                .unwrap();
        }
        let root = trie.root_hash().unwrap();
        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 99)
            .unwrap();

        let (mut loaded, _, loaded_root) =
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        assert_eq!(loaded_root, root);

        for i in 0u32..200 {
            assert_eq!(
                loaded.get(&i.to_be_bytes()).unwrap(),
                Some((i * 3).to_be_bytes().to_vec())
            );
        }
        assert_eq!(loaded.root_hash().unwrap(), root);
    }

    #[test]
    fn test_cache_corrupted_data() {
        let cache_id = 1005;
        let cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"hello", b"world").unwrap();
        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 1)
            .unwrap();

        // Corrupt a byte in the middle of the file.
        let mut data = fs::read(&cache_path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        fs::write(&cache_path, &data).unwrap();

        assert!(
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .is_err()
        );
    }

    #[test]
    fn test_cache_truncated_file() {
        let cache_id = 1006;
        let cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"key", b"val").unwrap();
        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 1)
            .unwrap();

        // Truncate the file.
        let data = fs::read(&cache_path).unwrap();
        fs::write(&cache_path, &data[..data.len() / 2]).unwrap();

        assert!(
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .is_err()
        );
    }

    #[test]
    fn test_root_hash_idempotent() {
        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();

        let h1 = trie.root_hash().unwrap();
        let h2 = trie.root_hash().unwrap();
        let h3 = trie.root_hash().unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
    }

    #[test]
    fn test_insert_after_commit() {
        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        let h1 = trie.root_hash().unwrap();

        // Insert after hashing should still work and produce a different root.
        trie.insert(b"b", b"2").unwrap();
        let h2 = trie.root_hash().unwrap();
        assert_ne!(h1, h2);

        // Data should be intact.
        assert_eq!(trie.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_remove_after_commit() {
        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        let _h1 = trie.root_hash().unwrap();

        trie.remove(b"a").unwrap();
        assert_eq!(trie.get(b"a").unwrap(), None);
        assert_eq!(trie.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_batch_update_deterministic() {
        let mut trie_a = MptCalc::new();
        trie_a.insert(b"x", b"1").unwrap();
        trie_a.insert(b"y", b"2").unwrap();
        trie_a.insert(b"z", b"3").unwrap();

        let mut trie_b = MptCalc::new();
        trie_b
            .batch_update(&[
                (b"z".as_slice(), Some(b"3".as_slice())),
                (b"x".as_slice(), Some(b"1".as_slice())),
                (b"y".as_slice(), Some(b"2".as_slice())),
            ])
            .unwrap();

        assert_eq!(trie_a.root_hash().unwrap(), trie_b.root_hash().unwrap());
    }

    #[test]
    fn test_single_key_operations() {
        let mut trie = MptCalc::new();

        // Single insert + hash.
        trie.insert(b"only", b"one").unwrap();
        let h = trie.root_hash().unwrap();
        assert_ne!(h, vec![0u8; 32]);

        // Remove the only key.
        trie.remove(b"only").unwrap();
        assert_eq!(trie.root_hash().unwrap(), vec![0u8; 32]);
    }

    #[test]
    fn test_long_keys_and_values() {
        let mut trie = MptCalc::new();

        let long_key = vec![0xAB; 256];
        let long_val = vec![0xCD; 1024];
        trie.insert(&long_key, &long_val).unwrap();
        assert_eq!(trie.get(&long_key).unwrap(), Some(long_val.clone()));

        let h = trie.root_hash().unwrap();
        assert_eq!(h.len(), 32);

        trie.remove(&long_key).unwrap();
        assert_eq!(trie.get(&long_key).unwrap(), None);
    }

    #[test]
    fn test_prefix_keys() {
        let mut trie = MptCalc::new();

        // Keys where one is a prefix of another.
        trie.insert(b"ab", b"1").unwrap();
        trie.insert(b"abc", b"2").unwrap();
        trie.insert(b"abcd", b"3").unwrap();

        assert_eq!(trie.get(b"ab").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"abc").unwrap(), Some(b"2".to_vec()));
        assert_eq!(trie.get(b"abcd").unwrap(), Some(b"3".to_vec()));

        // Remove middle key.
        trie.remove(b"abc").unwrap();
        assert_eq!(trie.get(b"ab").unwrap(), Some(b"1".to_vec()));
        assert_eq!(trie.get(b"abc").unwrap(), None);
        assert_eq!(trie.get(b"abcd").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_cache_save_load_preserves_hash_after_mutation() {
        let cache_id = 1007;
        let _cache_path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        trie.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 10)
            .unwrap();

        let (mut loaded, _, _) =
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();

        // Mutate, then save again.
        loaded.insert(b"c", b"3").unwrap();
        loaded.remove(b"a").unwrap();
        let h_mutated = loaded.root_hash().unwrap();
        loaded
            .save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 20)
            .unwrap();

        // Reload and verify.
        let (mut reloaded, tag, _) =
            MptCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        assert_eq!(tag, 20);
        assert_eq!(reloaded.root_hash().unwrap(), h_mutated);
        assert_eq!(reloaded.get(b"a").unwrap(), None);
        assert_eq!(reloaded.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(reloaded.get(b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_rejected_insert_preserves_existing_data() {
        use crate::trie::MAX_MPT_KEY_LEN;

        let mut trie = MptCalc::new();
        trie.insert(b"short", b"v").unwrap();
        let root_before = trie.root_hash().unwrap();

        // A key over MAX_MPT_KEY_LEN is rejected; the trie must be
        // completely unaffected — not silently emptied.
        let oversized_key = vec![0u8; MAX_MPT_KEY_LEN + 1];
        assert!(trie.insert(&oversized_key, b"x").is_err());

        assert_eq!(trie.get(b"short").unwrap(), Some(b"v".to_vec()));
        assert_eq!(trie.root_hash().unwrap(), root_before);
    }

    #[test]
    fn test_rejected_batch_update_preserves_existing_data() {
        use crate::trie::MAX_MPT_KEY_LEN;

        let mut trie = MptCalc::new();
        trie.insert(b"short", b"v").unwrap();
        let root_before = trie.root_hash().unwrap();

        // The valid op before the oversized one is applied (batch is
        // not atomic), but the failure must not wipe out prior state.
        let oversized_key = vec![0u8; MAX_MPT_KEY_LEN + 1];
        let ops: Vec<(&[u8], Option<&[u8]>)> =
            vec![(b"another", Some(b"w")), (&oversized_key, Some(b"x"))];
        assert!(trie.batch_update(&ops).is_err());

        assert_eq!(trie.get(b"short").unwrap(), Some(b"v".to_vec()));
        assert_eq!(trie.get(b"another").unwrap(), Some(b"w".to_vec()));
        assert_ne!(trie.root_hash().unwrap(), root_before);
    }

    #[test]
    fn test_trie_calc_errors_use_vsdb_error_not_trie_error() {
        use crate::VsdbError;
        use crate::trie::MAX_MPT_KEY_LEN;

        // `TrieCalc`/`MptCalc`/`SmtCalc` must surface the crate-wide
        // `VsdbError` in their public API (not the internal `TrieError`),
        // per the "single error type" invariant. This is primarily a
        // type-level guarantee (this test wouldn't compile otherwise),
        // but also checks the actual variant produced end-to-end.
        let mut trie = MptCalc::new();
        let oversized_key = vec![0u8; MAX_MPT_KEY_LEN + 1];
        let err: VsdbError = trie.insert(&oversized_key, b"x").unwrap_err();
        assert!(matches!(err, VsdbError::Trie { .. }));
    }
}

// =====================================================================
// SMT tests
// =====================================================================

#[cfg(test)]
mod smt_tests {
    use crate::trie::SmtCalc;
    use std::fs;

    #[test]
    fn test_smt_insert_get() {
        let mut smt = SmtCalc::new();
        smt.insert(b"hello", b"world").unwrap();
        assert_eq!(smt.get(b"hello").unwrap(), Some(b"world".to_vec()));
        assert_eq!(smt.get(b"nonexistent").unwrap(), None);
    }

    #[test]
    fn test_smt_overwrite() {
        let mut smt = SmtCalc::new();
        smt.insert(b"key", b"value1").unwrap();
        smt.insert(b"key", b"value2").unwrap();
        assert_eq!(smt.get(b"key").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_smt_remove() {
        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        smt.insert(b"b", b"2").unwrap();
        smt.remove(b"a").unwrap();
        assert_eq!(smt.get(b"a").unwrap(), None);
        assert_eq!(smt.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_smt_remove_nonexistent() {
        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        smt.remove(b"zzz").unwrap();
        assert_eq!(smt.get(b"a").unwrap(), Some(b"1".to_vec()));
    }

    #[test]
    fn test_smt_remove_nonexistent_preserves_cached_root() {
        // After root_hash() the internal nodes are Cached. Removing keys
        // that don't exist must walk the Internal no-change path (fast-path
        // peek didn't catch it because the path prefix matched even
        // though the leaf key didn't), which has to re-wrap nodes while
        // preserving their precomputed hashes rather than unconditionally
        // rebuilding via `compact` — a correctness bug would corrupt the
        // committed root; a preserved-hash regression would just force
        // wasteful re-hashing (checked below via the internal `root`
        // field, since both would compute the same *value*).
        let mut smt = SmtCalc::new();
        for i in 0u32..200 {
            smt.insert(&i.to_be_bytes(), &(i * 11).to_be_bytes())
                .unwrap();
        }
        let root = smt.root_hash().unwrap(); // commit → Cached nodes

        for k in [
            b"missing".as_slice(),
            &1000u32.to_be_bytes(),
            &54321u32.to_be_bytes(),
        ] {
            smt.remove(k).unwrap();
        }

        // Root must be unchanged and equal a fresh rebuild of the same data.
        assert_eq!(smt.root_hash().unwrap(), root);
        let mut fresh = SmtCalc::new();
        for i in 0u32..200 {
            fresh
                .insert(&i.to_be_bytes(), &(i * 11).to_be_bytes())
                .unwrap();
        }
        assert_eq!(fresh.root_hash().unwrap(), root);

        // All original entries must still be present.
        for i in 0u32..200 {
            assert_eq!(
                smt.get(&i.to_be_bytes()).unwrap(),
                Some((i * 11).to_be_bytes().to_vec())
            );
        }

        // White-box: the no-op removes must not have discarded the
        // Cached status of the (already-committed) root — a no-op
        // remove that unconditionally reconstructs via `compact` would
        // leave the root `InMemory` again, forcing a full ancestor
        // re-hash on the next `root_hash()` call.
        assert!(matches!(smt.root, crate::trie::smt::SmtHandle::Cached(..)));
    }

    #[test]
    fn test_smt_root_hash_empty() {
        let mut smt = SmtCalc::new();
        let h = smt.root_hash().unwrap();
        assert_eq!(h, vec![0u8; 32]); // EMPTY_HASH
    }

    #[test]
    fn test_smt_root_hash_deterministic() {
        let mut a = SmtCalc::new();
        a.insert(b"x", b"1").unwrap();
        a.insert(b"y", b"2").unwrap();

        let mut b = SmtCalc::new();
        b.insert(b"y", b"2").unwrap();
        b.insert(b"x", b"1").unwrap();

        assert_eq!(a.root_hash().unwrap(), b.root_hash().unwrap());
    }

    #[test]
    fn test_smt_root_hash_idempotent() {
        let mut smt = SmtCalc::new();
        smt.insert(b"k", b"v").unwrap();
        let h1 = smt.root_hash().unwrap();
        let h2 = smt.root_hash().unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_smt_insert_after_commit() {
        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        let h1 = smt.root_hash().unwrap();

        smt.insert(b"b", b"2").unwrap();
        let h2 = smt.root_hash().unwrap();
        assert_ne!(h1, h2);

        assert_eq!(smt.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(smt.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_smt_remove_after_commit() {
        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        smt.insert(b"b", b"2").unwrap();
        let _ = smt.root_hash().unwrap();

        smt.remove(b"a").unwrap();
        assert_eq!(smt.get(b"a").unwrap(), None);
        assert_eq!(smt.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn test_smt_batch_update() {
        let mut smt = SmtCalc::new();
        smt.batch_update(&[
            (b"a".as_ref(), Some(b"1".as_ref())),
            (b"b".as_ref(), Some(b"2".as_ref())),
            (b"c".as_ref(), Some(b"3".as_ref())),
        ])
        .unwrap();

        assert_eq!(smt.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(smt.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(smt.get(b"c").unwrap(), Some(b"3".to_vec()));

        smt.batch_update(&[(b"a".as_ref(), None), (b"d".as_ref(), Some(b"4".as_ref()))])
            .unwrap();

        assert_eq!(smt.get(b"a").unwrap(), None);
        assert_eq!(smt.get(b"d").unwrap(), Some(b"4".to_vec()));
    }

    #[test]
    fn test_smt_many_keys() {
        let mut smt = SmtCalc::new();
        for i in 0u32..200 {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        for i in 0u32..200 {
            assert_eq!(
                smt.get(&i.to_be_bytes()).unwrap(),
                Some(i.to_be_bytes().to_vec()),
                "key {} missing",
                i
            );
        }
        let h1 = smt.root_hash().unwrap();

        // Remove half.
        for i in 0u32..100 {
            smt.remove(&i.to_be_bytes()).unwrap();
        }
        for i in 0u32..100 {
            assert_eq!(smt.get(&i.to_be_bytes()).unwrap(), None);
        }
        for i in 100u32..200 {
            assert_eq!(
                smt.get(&i.to_be_bytes()).unwrap(),
                Some(i.to_be_bytes().to_vec())
            );
        }
        let h2 = smt.root_hash().unwrap();
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_smt_from_entries() {
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0u32..50)
            .map(|i| (i.to_be_bytes().to_vec(), i.to_be_bytes().to_vec()))
            .collect();
        let mut smt =
            SmtCalc::from_entries(entries.iter().map(|(k, v)| (k, v))).unwrap();
        for (k, v) in &entries {
            assert_eq!(smt.get(k).unwrap(), Some(v.clone()));
        }
        let h = smt.root_hash().unwrap();
        assert_ne!(h, vec![0u8; 32]);
    }

    // =================================================================
    // Proof tests
    // =================================================================

    #[test]
    fn test_smt_proof_membership() {
        let mut smt = SmtCalc::new();
        smt.insert(b"alpha", b"A").unwrap();
        smt.insert(b"beta", b"B").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        // Membership proof for "alpha".
        let proof = smt.prove(b"alpha").unwrap();
        assert_eq!(proof.value(), Some(b"A".as_ref()));
        assert!(SmtCalc::verify_proof(&root32, b"alpha", &proof).unwrap());
        assert!(!SmtCalc::verify_proof(&root32, b"beta", &proof).unwrap());

        // Membership proof for "beta".
        let proof = smt.prove(b"beta").unwrap();
        assert_eq!(proof.value(), Some(b"B".as_ref()));
        assert!(SmtCalc::verify_proof(&root32, b"beta", &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_nonmembership_empty() {
        let mut smt = SmtCalc::new();
        smt.insert(b"alpha", b"A").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        // Non-membership proof for a key that doesn't exist.
        let proof = smt.prove(b"missing").unwrap();
        assert_eq!(proof.value(), None);
        assert!(SmtCalc::verify_proof(&root32, b"missing", &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_nonmembership_divergent() {
        // Insert many keys so we get internal nodes with compressed paths,
        // then prove non-membership of a key that diverges within a
        // compressed prefix.
        let mut smt = SmtCalc::new();
        for i in 0u32..20 {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let proof = smt.prove(b"nonexistent_key").unwrap();
        assert_eq!(proof.value(), None);
        assert!(SmtCalc::verify_proof(&root32, b"nonexistent_key", &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_wrong_root_fails() {
        let mut smt = SmtCalc::new();
        smt.insert(b"key", b"val").unwrap();
        let _ = smt.root_hash().unwrap();
        let proof = smt.prove(b"key").unwrap();

        let wrong_root = [0xFFu8; 32];
        assert!(!SmtCalc::verify_proof(&wrong_root, b"key", &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_tampered_value_fails() {
        let mut smt = SmtCalc::new();
        smt.insert(b"key", b"val").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let mut proof = smt.prove(b"key").unwrap();
        let (kh, _) = proof.leaf.take().unwrap();
        proof.leaf = Some((kh, b"tampered".to_vec()));
        assert!(!SmtCalc::verify_proof(&root32, b"key", &proof).unwrap());
    }

    /// Appending an extra EMPTY sibling relocates the leaf one level
    /// deeper — depth is bound by the sibling fold, so this must fail.
    #[test]
    fn test_smt_proof_depth_extension_fails() {
        let mut smt = SmtCalc::new();
        smt.insert(b"alpha", b"A").unwrap();
        smt.insert(b"beta", b"B").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let mut proof = smt.prove(b"alpha").unwrap();
        assert!(SmtCalc::verify_proof(&root32, b"alpha", &proof).unwrap());

        proof.siblings.push([0u8; 32]);
        assert!(!SmtCalc::verify_proof(&root32, b"alpha", &proof).unwrap());
    }

    /// Truncating the sibling list (claiming a shallower position)
    /// must fail for the same reason.
    #[test]
    fn test_smt_proof_sibling_truncation_fails() {
        let mut smt = SmtCalc::new();
        for i in 0u32..20 {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let mut proof = smt.prove(&3u32.to_be_bytes()).unwrap();
        assert!(!proof.siblings.is_empty());
        assert!(SmtCalc::verify_proof(&root32, &3u32.to_be_bytes(), &proof).unwrap());

        proof.siblings.pop();
        assert!(!SmtCalc::verify_proof(&root32, &3u32.to_be_bytes(), &proof).unwrap());
    }

    /// A conflicting-leaf non-membership proof must carry a leaf whose
    /// key hash shares every path bit above the terminal subtree with
    /// the queried key; substituting an unrelated leaf must fail fast.
    #[test]
    fn test_smt_proof_conflicting_leaf_prefix_check() {
        let mut smt = SmtCalc::new();
        smt.insert(b"alpha", b"A").unwrap();
        smt.insert(b"beta", b"B").unwrap();
        smt.insert(b"gamma", b"C").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        // Find an absent key whose proof terminates at a conflicting
        // leaf (leaf present but different key hash).
        let mut conflict_proof = None;
        for i in 0u32..1000 {
            let key = i.to_be_bytes();
            let proof = smt.prove(&key).unwrap();
            if let Some((kh, _)) = &proof.leaf
                && *kh != proof.key_hash
            {
                conflict_proof = Some((key, proof));
                break;
            }
        }
        let (key, mut proof) =
            conflict_proof.expect("no conflicting-leaf proof found in 1000 probes");
        assert_eq!(proof.value(), None);
        assert!(SmtCalc::verify_proof(&root32, &key, &proof).unwrap());

        // Replace the conflicting leaf with one whose key hash does not
        // share the required prefix: the verifier must reject it before
        // any hashing (prefix consistency check).
        let (_, val) = proof.leaf.take().unwrap();
        let mut foreign_kh = proof.key_hash;
        foreign_kh[0] ^= 0x80; // diverges at bit 0
        proof.leaf = Some((foreign_kh, val));
        if !proof.siblings.is_empty() {
            assert!(!SmtCalc::verify_proof(&root32, &key, &proof).unwrap());
        }
    }

    /// Proofs must be compact — O(log N) siblings, not 256.
    #[test]
    fn test_smt_proof_is_compact() {
        let mut smt = SmtCalc::new();
        for i in 0u32..100 {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        let _ = smt.root_hash().unwrap();

        for i in 0u32..100 {
            let proof = smt.prove(&i.to_be_bytes()).unwrap();
            assert!(
                proof.siblings.len() <= 64,
                "proof for key {} has {} siblings — expected O(log N)",
                i,
                proof.siblings.len()
            );
        }
    }

    #[test]
    fn test_smt_proof_single_key() {
        let mut smt = SmtCalc::new();
        smt.insert(b"only", b"one").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let proof = smt.prove(b"only").unwrap();
        assert!(SmtCalc::verify_proof(&root32, b"only", &proof).unwrap());

        let proof = smt.prove(b"other").unwrap();
        assert_eq!(proof.value(), None);
        assert!(SmtCalc::verify_proof(&root32, b"other", &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_many_keys() {
        let mut smt = SmtCalc::new();
        let n = 50u32;
        for i in 0..n {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        // Verify membership for all inserted keys.
        for i in 0..n {
            let proof = smt.prove(&i.to_be_bytes()).unwrap();
            assert_eq!(proof.value(), Some(i.to_be_bytes().as_ref()));
            assert!(
                SmtCalc::verify_proof(&root32, &i.to_be_bytes(), &proof).unwrap(),
                "membership proof failed for key {}",
                i
            );
        }

        // Verify non-membership for keys not inserted.
        for i in n..n + 20 {
            let proof = smt.prove(&i.to_be_bytes()).unwrap();
            assert_eq!(proof.value(), None);
            assert!(
                SmtCalc::verify_proof(&root32, &i.to_be_bytes(), &proof).unwrap(),
                "non-membership proof failed for key {}",
                i
            );
        }
    }

    #[test]
    fn test_smt_proof_after_remove() {
        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        smt.insert(b"b", b"2").unwrap();
        smt.remove(b"a").unwrap();

        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let proof_a = smt.prove(b"a").unwrap();
        assert_eq!(proof_a.value(), None);
        assert!(SmtCalc::verify_proof(&root32, b"a", &proof_a).unwrap());

        let proof_b = smt.prove(b"b").unwrap();
        assert_eq!(proof_b.value(), Some(b"2".as_ref()));
        assert!(SmtCalc::verify_proof(&root32, b"b", &proof_b).unwrap());
    }

    #[test]
    fn test_smt_proof_empty_tree() {
        let mut smt = SmtCalc::new();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let proof = smt.prove(b"anything").unwrap();
        assert_eq!(proof.value(), None);
        assert!(SmtCalc::verify_proof(&root32, b"anything", &proof).unwrap());
    }

    // =================================================================
    // Cache tests
    // =================================================================

    #[test]
    fn test_smt_cache_roundtrip() {
        let cache_id = 1008;
        let _path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("smt_cache_{}.bin", cache_id));

        let mut smt = SmtCalc::new();
        for i in 0u32..30 {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        let h1 = smt.root_hash().unwrap();
        smt.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 42)
            .unwrap();

        let (mut loaded, tag, h2) =
            SmtCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        assert_eq!(tag, 42);
        assert_eq!(h1, h2);

        for i in 0u32..30 {
            assert_eq!(
                loaded.get(&i.to_be_bytes()).unwrap(),
                Some(i.to_be_bytes().to_vec())
            );
        }
        let h3 = loaded.root_hash().unwrap();
        assert_eq!(h1, h3);
    }

    #[test]
    fn test_smt_cache_corrupted() {
        let cache_id = 1009;
        let path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("smt_cache_{}.bin", cache_id));

        let mut smt = SmtCalc::new();
        smt.insert(b"k", b"v").unwrap();
        smt.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 1)
            .unwrap();

        // Corrupt a byte in the middle.
        let mut data = fs::read(&path).unwrap();
        if data.len() > 10 {
            data[10] ^= 0xFF;
        }
        fs::write(&path, &data).unwrap();

        assert!(
            SmtCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .is_err()
        );
    }

    #[test]
    fn test_smt_cache_incremental() {
        let cache_id = 1010;
        let _path = vsdb_core::common::vsdb_get_system_dir()
            .join(format!("smt_cache_{}.bin", cache_id));

        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        smt.insert(b"b", b"2").unwrap();
        smt.save_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id, 10)
            .unwrap();

        let (mut loaded, _, _) =
            SmtCalc::load_cache(vsdb_core::common::vsdb_get_system_dir(), cache_id)
                .unwrap();
        loaded.insert(b"c", b"3").unwrap();
        let h = loaded.root_hash().unwrap();

        // Build the same from scratch.
        let mut fresh = SmtCalc::new();
        fresh.insert(b"a", b"1").unwrap();
        fresh.insert(b"b", b"2").unwrap();
        fresh.insert(b"c", b"3").unwrap();
        assert_eq!(fresh.root_hash().unwrap(), h);
    }
}

// =========================================================================
// MPT proof tests
// =========================================================================

#[cfg(test)]
mod mpt_proof_tests {
    use crate::trie::MptCalc;

    #[test]
    fn test_mpt_proof_membership_single() {
        let mut trie = MptCalc::new();
        trie.insert(b"hello", b"world").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        let proof = trie.prove(b"hello").unwrap();
        assert_eq!(proof.value, Some(b"world".to_vec()));
        assert!(MptCalc::verify_proof(&root_hash, b"hello", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_membership_multiple() {
        let mut trie = MptCalc::new();
        trie.insert(b"alpha", b"1").unwrap();
        trie.insert(b"beta", b"2").unwrap();
        trie.insert(b"gamma", b"3").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        for (key, val) in [
            (&b"alpha"[..], &b"1"[..]),
            (&b"beta"[..], &b"2"[..]),
            (&b"gamma"[..], &b"3"[..]),
        ] {
            let proof = trie.prove(key).unwrap();
            assert_eq!(proof.value.as_deref(), Some(val));
            assert!(MptCalc::verify_proof(&root_hash, key, &proof).unwrap());
        }
    }

    #[test]
    fn test_mpt_proof_prefix_keys() {
        let mut trie = MptCalc::new();
        trie.insert(b"ab", b"v1").unwrap();
        trie.insert(b"abc", b"v2").unwrap();
        trie.insert(b"abcd", b"v3").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        for (key, val) in [
            (&b"ab"[..], &b"v1"[..]),
            (&b"abc"[..], &b"v2"[..]),
            (&b"abcd"[..], &b"v3"[..]),
        ] {
            let proof = trie.prove(key).unwrap();
            assert_eq!(proof.value.as_deref(), Some(val));
            assert!(MptCalc::verify_proof(&root_hash, key, &proof).unwrap());
        }
    }

    /// Non-membership where the queried key's nibble path diverges in the
    /// MIDDLE of an Extension node's compressed path (not at a Branch).
    ///
    /// `0x1234` / `0x1235` share nibbles `[1,2,3]` → Extension([1,2,3]) →
    /// Branch{4,5}. The absent key `0x1934` departs at the Extension's
    /// second nibble.
    #[test]
    fn test_mpt_proof_nonmembership_diverges_inside_extension() {
        let mut trie = MptCalc::new();
        trie.insert(&[0x12, 0x34], b"a").unwrap();
        trie.insert(&[0x12, 0x35], b"b").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        for absent in [
            &[0x19u8, 0x34][..],       // diverges at the Extension's 2nd nibble
            &[0x12u8, 0x44][..],       // diverges at the Extension's last nibble
            &[0x12u8, 0x34, 0x56][..], // walks past a leaf
        ] {
            let proof = trie.prove(absent).unwrap();
            assert_eq!(proof.value, None, "key {absent:02x?} must be absent");
            assert!(
                MptCalc::verify_proof(&root_hash, absent, &proof).unwrap(),
                "non-membership proof for {absent:02x?} must verify"
            );
        }

        // Present keys still prove correctly against the same root.
        for (key, val) in [(&[0x12u8, 0x34][..], &b"a"[..]), (&[0x12, 0x35], b"b")] {
            let proof = trie.prove(key).unwrap();
            assert_eq!(proof.value.as_deref(), Some(val));
            assert!(MptCalc::verify_proof(&root_hash, key, &proof).unwrap());
        }
    }

    /// Proof path traversing an Extension→Branch→Extension→Branch chain.
    ///
    /// Keys `0x11111111` / `0x11111122` / `0x11221111` share nibbles
    /// `[1,1]` (outer Extension), split at a Branch (1 vs 2), and the
    /// first two then share `[1,1,1]` again (inner Extension) before a
    /// second Branch splits them.
    #[test]
    fn test_mpt_proof_extension_branch_extension_chain() {
        let mut trie = MptCalc::new();
        let keys: [&[u8]; 3] = [
            &[0x11, 0x11, 0x11, 0x11],
            &[0x11, 0x11, 0x11, 0x22],
            &[0x11, 0x22, 0x11, 0x11],
        ];
        for (i, key) in keys.iter().enumerate() {
            trie.insert(key, &[i as u8]).unwrap();
        }
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        // Membership through the full chain.
        for (i, key) in keys.iter().enumerate() {
            let proof = trie.prove(key).unwrap();
            assert_eq!(proof.value, Some(vec![i as u8]));
            assert!(MptCalc::verify_proof(&root_hash, key, &proof).unwrap());
        }

        // Non-membership diverging inside the INNER extension: shares
        // [1,1] + branch slot 1 + inner nibbles [1,1] then departs (9).
        let absent: &[u8] = &[0x11, 0x11, 0x19, 0x11];
        let proof = trie.prove(absent).unwrap();
        assert_eq!(proof.value, None);
        assert!(MptCalc::verify_proof(&root_hash, absent, &proof).unwrap());

        // A tampered value must not verify anywhere along the chain.
        let mut bad = trie.prove(keys[0]).unwrap();
        bad.value = Some(vec![0xFF]);
        assert!(!MptCalc::verify_proof(&root_hash, keys[0], &bad).unwrap_or(false));
    }

    #[test]
    fn test_mpt_proof_nonmembership_empty_trie() {
        let trie = MptCalc::new();
        let proof = trie.prove(b"anything").unwrap();
        assert_eq!(proof.value, None);
        let root_hash = [0u8; 32];
        assert!(MptCalc::verify_proof(&root_hash, b"anything", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_nonmembership_missing_key() {
        let mut trie = MptCalc::new();
        trie.insert(b"present", b"yes").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        let proof = trie.prove(b"absent").unwrap();
        assert_eq!(proof.value, None);
        assert!(MptCalc::verify_proof(&root_hash, b"absent", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_wrong_root_fails() {
        let mut trie = MptCalc::new();
        trie.insert(b"key", b"value").unwrap();
        let _root = trie.root_hash().unwrap();

        let proof = trie.prove(b"key").unwrap();
        let wrong_root = [0xFFu8; 32];
        assert!(!MptCalc::verify_proof(&wrong_root, b"key", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_tampered_value_fails() {
        let mut trie = MptCalc::new();
        trie.insert(b"key", b"value").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        let mut proof = trie.prove(b"key").unwrap();
        // Tamper with the claimed value
        proof.value = Some(b"tampered".to_vec());
        assert!(!MptCalc::verify_proof(&root_hash, b"key", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_tampered_node_fails() {
        let mut trie = MptCalc::new();
        trie.insert(b"key", b"value").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        let mut proof = trie.prove(b"key").unwrap();
        // Corrupt a byte in the first node
        if let Some(first) = proof.nodes.first_mut()
            && let Some(last) = first.last_mut()
        {
            *last ^= 0xFF;
        }
        assert!(!MptCalc::verify_proof(&root_hash, b"key", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_after_remove() {
        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        trie.remove(b"a").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        // "a" should be non-membership
        let proof_a = trie.prove(b"a").unwrap();
        assert_eq!(proof_a.value, None);
        assert!(MptCalc::verify_proof(&root_hash, b"a", &proof_a).unwrap());

        // "b" should be membership
        let proof_b = trie.prove(b"b").unwrap();
        assert_eq!(proof_b.value, Some(b"2".to_vec()));
        assert!(MptCalc::verify_proof(&root_hash, b"b", &proof_b).unwrap());
    }

    #[test]
    fn test_mpt_proof_after_update() {
        let mut trie = MptCalc::new();
        trie.insert(b"key", b"v1").unwrap();
        trie.insert(b"key", b"v2").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        let proof = trie.prove(b"key").unwrap();
        assert_eq!(proof.value, Some(b"v2".to_vec()));
        assert!(MptCalc::verify_proof(&root_hash, b"key", &proof).unwrap());
    }

    #[test]
    fn test_mpt_proof_many_keys() {
        let mut trie = MptCalc::new();
        let keys: Vec<Vec<u8>> = (0u32..50).map(|i| i.to_be_bytes().to_vec()).collect();
        for (i, key) in keys.iter().enumerate() {
            trie.insert(key, format!("val{i}").as_bytes()).unwrap();
        }
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        // Verify membership for all inserted keys
        for (i, key) in keys.iter().enumerate() {
            let proof = trie.prove(key).unwrap();
            assert_eq!(
                proof.value.as_deref(),
                Some(format!("val{i}").as_bytes()),
                "membership failed for key {i}"
            );
            assert!(
                MptCalc::verify_proof(&root_hash, key, &proof).unwrap(),
                "verification failed for key {i}"
            );
        }

        // Verify non-membership for keys not inserted
        for i in 50u32..55 {
            let key = i.to_be_bytes();
            let proof = trie.prove(&key).unwrap();
            assert_eq!(proof.value, None, "should be non-member for key {i}");
            assert!(
                MptCalc::verify_proof(&root_hash, &key, &proof).unwrap(),
                "non-membership verification failed for key {i}"
            );
        }
    }

    #[test]
    fn test_mpt_proof_wrong_key_fails() {
        let mut trie = MptCalc::new();
        trie.insert(b"real_key", b"value").unwrap();
        let root = trie.root_hash().unwrap();
        let root_hash: [u8; 32] = root.try_into().unwrap();

        let proof = trie.prove(b"real_key").unwrap();
        // Proof is valid for "real_key" but must fail for a different key
        assert!(MptCalc::verify_proof(&root_hash, b"real_key", &proof).unwrap());
        assert!(!MptCalc::verify_proof(&root_hash, b"wrong_key", &proof).unwrap());
    }
}

// =====================================================================
// Cache deserializer structural validation
//
// The cache files are a trust boundary: a checksum-valid but malformed
// file must not be able to drive the tree walkers into states organic
// trees can never reach (over-256-bit SMT descents that drop the
// consumed working tree, out-of-range MPT nibbles that panic on branch
// indexing, or zero-progress nesting that overflows the stack).  These
// tests craft such trees in memory, serialize them through the real
// writer (so checksums/framing are valid), and assert the loader
// rejects them — while organically built trees keep round-tripping
// (covered by the pre-existing roundtrip tests).
// =====================================================================

#[cfg(test)]
mod cache_validation_tests {
    use crate::trie::cache as mpt_cache;
    use crate::trie::nibbles::Nibbles;
    use crate::trie::node::{Node, NodeHandle};
    use crate::trie::smt::bitpath::BitPath;
    use crate::trie::smt::cache as smt_cache;
    use crate::trie::smt::{SmtHandle, SmtNode};

    fn smt_load(root: &SmtHandle) -> crate::trie::error::Result<SmtHandle> {
        let mut buf = Vec::new();
        smt_cache::save(root, 0, &[0u8; 32], &mut buf).unwrap();
        smt_cache::load(&mut buf.as_slice()).map(|(h, _, _)| h)
    }

    fn mpt_load(root: &NodeHandle) -> crate::trie::error::Result<NodeHandle> {
        let mut buf = Vec::new();
        mpt_cache::save(root, 0, &[0u8; 32], &mut buf).unwrap();
        mpt_cache::load(&mut buf.as_slice()).map(|(h, _, _)| h)
    }

    fn smt_leaf(key_hash: [u8; 32], path: BitPath) -> SmtHandle {
        SmtHandle::InMemory(Box::new(SmtNode::Leaf {
            path,
            key_hash,
            value: b"v".to_vec(),
        }))
    }

    #[test]
    fn smt_rejects_cumulative_depth_over_256() {
        // Two nested internals whose combined paths exceed a 256-bit
        // key path.  Before validation, loading this and inserting a
        // key descending through it hit insert_rec's depth rejection —
        // *after* the working tree had been consumed — silently
        // emptying the whole tree.
        let deep_child = SmtHandle::InMemory(Box::new(SmtNode::Internal {
            path: BitPath::from_bits(&[0; 200]),
            left: SmtHandle::default(),
            right: SmtHandle::default(),
        }));
        let root = SmtHandle::InMemory(Box::new(SmtNode::Internal {
            path: BitPath::from_bits(&[0; 200]),
            left: deep_child,
            right: SmtHandle::default(),
        }));
        assert!(smt_load(&root).is_err());
    }

    #[test]
    fn smt_rejects_leaf_incoherent_with_key_hash() {
        // Leaf path shorter than the remaining key path.
        let short = smt_leaf([0xAB; 32], BitPath::from_bits(&[1; 100]));
        assert!(smt_load(&short).is_err());

        // Right length, but positioned on a path that doesn't match
        // the leaf's own key hash.
        let mispositioned = smt_leaf([0xAB; 32], BitPath::from_hash(&[0xCD; 32]));
        assert!(smt_load(&mispositioned).is_err());

        // Coherent leaf loads fine.
        let coherent = smt_leaf([0xAB; 32], BitPath::from_hash(&[0xAB; 32]));
        assert!(smt_load(&coherent).is_ok());
    }

    #[test]
    fn smt_rejects_bad_cached_hash_length() {
        // A non-32-byte Cached hash would surface deep inside
        // commit_rec (whose failure drops the consumed working tree).
        let root = SmtHandle::Cached(vec![0u8; 5], Box::new(SmtNode::Empty));
        assert!(smt_load(&root).is_err());

        let ok = SmtHandle::Cached(vec![0u8; 32], Box::new(SmtNode::Empty));
        assert!(smt_load(&ok).is_ok());
    }

    #[test]
    fn mpt_rejects_cumulative_path_over_key_length_cap() {
        // Three nested 1000-nibble extensions: 3000 nibbles > the
        // 2 * MAX_MPT_KEY_LEN = 2048 bound organic insertion enforces.
        // Such a trie bypasses the insertion-time stack-depth cap.
        let mut h = NodeHandle::InMemory(Box::new(Node::Leaf {
            path: Nibbles::from_nibbles_unsafe(vec![]),
            value: b"v".to_vec(),
        }));
        for _ in 0..3 {
            h = NodeHandle::InMemory(Box::new(Node::Extension {
                path: Nibbles::from_nibbles_unsafe(vec![0u8; 1000]),
                child: h,
            }));
        }
        assert!(mpt_load(&h).is_err());
    }

    #[test]
    fn mpt_rejects_empty_extension_path() {
        // Organic extensions are never empty; a crafted chain of empty
        // ones makes zero progress against the nibble budget, so the
        // walkers (and the deserializer itself) would recurse once per
        // node with nothing bounding the nesting.  Rejecting the first
        // empty extension is what keeps the nibble budget a real
        // recursion bound.
        let mut h = NodeHandle::InMemory(Box::new(Node::Leaf {
            path: Nibbles::from_nibbles_unsafe(vec![]),
            value: b"v".to_vec(),
        }));
        for _ in 0..3 {
            h = NodeHandle::InMemory(Box::new(Node::Extension {
                path: Nibbles::from_nibbles_unsafe(vec![]),
                child: h,
            }));
        }
        assert!(mpt_load(&h).is_err());
    }

    #[test]
    fn mpt_rejects_out_of_range_nibble_values() {
        // Branch children are indexed by nibble value; a nibble > 0x0F
        // from a malformed file would panic on `children[idx]`.
        // `Nibbles` can't even represent one (debug-asserted), so
        // patch the serialized payload directly and re-checksum —
        // exactly what a hand-crafted file could contain.
        use crate::trie::codec_util::{CHECKSUM_LEN, compute_checksum};

        let root = NodeHandle::InMemory(Box::new(Node::Leaf {
            path: Nibbles::from_nibbles_unsafe(vec![0x0A]),
            value: b"v".to_vec(),
        }));
        let mut buf = Vec::new();
        mpt_cache::save(&root, 0, &[0u8; 32], &mut buf).unwrap();

        // Sanity: the loader accepts the untampered file.
        assert!(mpt_cache::load(&mut buf.as_slice()).is_ok());

        let payload_len = buf.len() - CHECKSUM_LEN;
        let idx = buf[..payload_len].iter().rposition(|&b| b == 0x0A).unwrap();
        buf[idx] = 0x1F;
        let checksum = compute_checksum(&buf[..payload_len]);
        buf[payload_len..].copy_from_slice(&checksum);

        assert!(mpt_cache::load(&mut buf.as_slice()).is_err());
    }

    #[test]
    fn mpt_rejects_bad_cached_hash_length() {
        let root = NodeHandle::Cached(vec![0u8; 5], Box::new(Node::Null));
        assert!(mpt_load(&root).is_err());

        let ok = NodeHandle::Cached(vec![0u8; 32], Box::new(Node::Null));
        assert!(mpt_load(&ok).is_ok());
    }

    #[test]
    fn mpt_rejects_inmemory_child_under_cached_parent() {
        // `commit_rec` skips Cached subtrees, so an InMemory child under
        // a Cached parent is never re-hashed by `root_hash()`; encoding
        // the parent (e.g. in `prove`) would then panic on the child's
        // missing hash.  Organic saves always commit first, so this
        // shape only comes from a crafted file — reject at load.
        let in_mem_leaf = NodeHandle::InMemory(Box::new(Node::Leaf {
            path: Nibbles::from_nibbles_unsafe(vec![0x01]),
            value: b"v".to_vec(),
        }));

        // Cached extension → InMemory child.
        let ext = NodeHandle::Cached(
            vec![0u8; 32],
            Box::new(Node::Extension {
                path: Nibbles::from_nibbles_unsafe(vec![0x0A]),
                child: in_mem_leaf.clone(),
            }),
        );
        assert!(mpt_load(&ext).is_err());

        // Cached branch → InMemory child.
        let mut children: Box<[Option<NodeHandle>; 16]> = Default::default();
        children[3] = Some(in_mem_leaf);
        let branch = NodeHandle::Cached(
            vec![0u8; 32],
            Box::new(Node::Branch {
                children,
                value: None,
            }),
        );
        assert!(mpt_load(&branch).is_err());

        // Deeper mix: Cached → Cached → InMemory is caught at the
        // intermediate Cached parent.
        let deep = NodeHandle::Cached(
            vec![1u8; 32],
            Box::new(Node::Extension {
                path: Nibbles::from_nibbles_unsafe(vec![0x02]),
                child: NodeHandle::Cached(
                    vec![2u8; 32],
                    Box::new(Node::Extension {
                        path: Nibbles::from_nibbles_unsafe(vec![0x03]),
                        child: NodeHandle::InMemory(Box::new(Node::Leaf {
                            path: Nibbles::from_nibbles_unsafe(vec![0x04]),
                            value: b"v".to_vec(),
                        })),
                    }),
                ),
            }),
        );
        assert!(mpt_load(&deep).is_err());

        // Fully-Cached counterpart still loads.
        let all_cached = NodeHandle::Cached(
            vec![0u8; 32],
            Box::new(Node::Extension {
                path: Nibbles::from_nibbles_unsafe(vec![0x0A]),
                child: NodeHandle::Cached(
                    vec![0u8; 32],
                    Box::new(Node::Leaf {
                        path: Nibbles::from_nibbles_unsafe(vec![0x01]),
                        value: b"v".to_vec(),
                    }),
                ),
            }),
        );
        assert!(mpt_load(&all_cached).is_ok());
    }
}
