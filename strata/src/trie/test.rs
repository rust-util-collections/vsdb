#[cfg(test)]
mod tests {
    use crate::trie::MptCalc;

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
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"hello", b"world").unwrap();
        trie.insert(b"foo", b"bar").unwrap();
        trie.insert(b"key", b"value").unwrap();
        let root = trie.root_hash().unwrap();

        trie.save_cache(cache_id, 42).unwrap();

        let (loaded, sync_tag, loaded_root) = MptCalc::load_cache(cache_id).unwrap();
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
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        trie.save_cache(cache_id, 1).unwrap();

        // Load and apply incremental changes.
        let (mut loaded, _, _) = MptCalc::load_cache(cache_id).unwrap();
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
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.save_cache(cache_id, 0).unwrap();

        let (loaded, sync_tag, root_hash) = MptCalc::load_cache(cache_id).unwrap();
        assert_eq!(sync_tag, 0);
        assert_eq!(root_hash, vec![0u8; 32]);
        assert_eq!(loaded.get(b"anything").unwrap(), None);
    }

    #[test]
    fn test_cache_many_keys() {
        let cache_id = 1004;
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        for i in 0u32..200 {
            trie.insert(&i.to_be_bytes(), &(i * 3).to_be_bytes())
                .unwrap();
        }
        let root = trie.root_hash().unwrap();
        trie.save_cache(cache_id, 99).unwrap();

        let (mut loaded, _, loaded_root) = MptCalc::load_cache(cache_id).unwrap();
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
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"hello", b"world").unwrap();
        trie.save_cache(cache_id, 1).unwrap();

        // Corrupt a byte in the middle of the file.
        let mut data = std::fs::read(&cache_path).unwrap();
        let mid = data.len() / 2;
        data[mid] ^= 0xFF;
        std::fs::write(&cache_path, &data).unwrap();

        assert!(MptCalc::load_cache(cache_id).is_err());
    }

    #[test]
    fn test_cache_truncated_file() {
        let cache_id = 1006;
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"key", b"val").unwrap();
        trie.save_cache(cache_id, 1).unwrap();

        // Truncate the file.
        let data = std::fs::read(&cache_path).unwrap();
        std::fs::write(&cache_path, &data[..data.len() / 2]).unwrap();

        assert!(MptCalc::load_cache(cache_id).is_err());
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
        let cache_path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("mpt_cache_{}.bin", cache_id));

        let mut trie = MptCalc::new();
        trie.insert(b"a", b"1").unwrap();
        trie.insert(b"b", b"2").unwrap();
        trie.save_cache(cache_id, 10).unwrap();

        let (mut loaded, _, _) = MptCalc::load_cache(cache_id).unwrap();

        // Mutate, then save again.
        loaded.insert(b"c", b"3").unwrap();
        loaded.remove(b"a").unwrap();
        let h_mutated = loaded.root_hash().unwrap();
        loaded.save_cache(cache_id, 20).unwrap();

        // Reload and verify.
        let (mut reloaded, tag, _) = MptCalc::load_cache(cache_id).unwrap();
        assert_eq!(tag, 20);
        assert_eq!(reloaded.root_hash().unwrap(), h_mutated);
        assert_eq!(reloaded.get(b"a").unwrap(), None);
        assert_eq!(reloaded.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(reloaded.get(b"c").unwrap(), Some(b"3".to_vec()));
    }
}

// =====================================================================
// SMT tests
// =====================================================================

#[cfg(test)]
mod smt_tests {
    use crate::trie::SmtCalc;

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
        assert_eq!(proof.value, Some(b"A".to_vec()));
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());

        // Membership proof for "beta".
        let proof = smt.prove(b"beta").unwrap();
        assert_eq!(proof.value, Some(b"B".to_vec()));
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_nonmembership_empty() {
        let mut smt = SmtCalc::new();
        smt.insert(b"alpha", b"A").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        // Non-membership proof for a key that doesn't exist.
        let proof = smt.prove(b"missing").unwrap();
        assert_eq!(proof.value, None);
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());
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
        assert_eq!(proof.value, None);
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_wrong_root_fails() {
        let mut smt = SmtCalc::new();
        smt.insert(b"key", b"val").unwrap();
        let _ = smt.root_hash().unwrap();
        let proof = smt.prove(b"key").unwrap();

        let wrong_root = [0xFFu8; 32];
        assert!(!SmtCalc::verify_proof(&wrong_root, &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_tampered_value_fails() {
        let mut smt = SmtCalc::new();
        smt.insert(b"key", b"val").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let mut proof = smt.prove(b"key").unwrap();
        proof.value = Some(b"tampered".to_vec());
        assert!(!SmtCalc::verify_proof(&root32, &proof).unwrap());
    }

    #[test]
    fn test_smt_proof_single_key() {
        let mut smt = SmtCalc::new();
        smt.insert(b"only", b"one").unwrap();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let proof = smt.prove(b"only").unwrap();
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());

        let proof = smt.prove(b"other").unwrap();
        assert_eq!(proof.value, None);
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());
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
            assert_eq!(proof.value, Some(i.to_be_bytes().to_vec()));
            assert!(
                SmtCalc::verify_proof(&root32, &proof).unwrap(),
                "membership proof failed for key {}",
                i
            );
        }

        // Verify non-membership for keys not inserted.
        for i in n..n + 20 {
            let proof = smt.prove(&i.to_be_bytes()).unwrap();
            assert_eq!(proof.value, None);
            assert!(
                SmtCalc::verify_proof(&root32, &proof).unwrap(),
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
        assert_eq!(proof_a.value, None);
        assert!(SmtCalc::verify_proof(&root32, &proof_a).unwrap());

        let proof_b = smt.prove(b"b").unwrap();
        assert_eq!(proof_b.value, Some(b"2".to_vec()));
        assert!(SmtCalc::verify_proof(&root32, &proof_b).unwrap());
    }

    #[test]
    fn test_smt_proof_empty_tree() {
        let mut smt = SmtCalc::new();
        let root = smt.root_hash().unwrap();
        let root32: [u8; 32] = root.try_into().unwrap();

        let proof = smt.prove(b"anything").unwrap();
        assert_eq!(proof.value, None);
        assert!(SmtCalc::verify_proof(&root32, &proof).unwrap());
    }

    // =================================================================
    // Cache tests
    // =================================================================

    #[test]
    fn test_smt_cache_roundtrip() {
        let cache_id = 1008;
        let path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("smt_cache_{}.bin", cache_id));

        let mut smt = SmtCalc::new();
        for i in 0u32..30 {
            smt.insert(&i.to_be_bytes(), &i.to_be_bytes()).unwrap();
        }
        let h1 = smt.root_hash().unwrap();
        smt.save_cache(cache_id, 42).unwrap();

        let (mut loaded, tag, h2) = SmtCalc::load_cache(cache_id).unwrap();
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
        let path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("smt_cache_{}.bin", cache_id));

        let mut smt = SmtCalc::new();
        smt.insert(b"k", b"v").unwrap();
        smt.save_cache(cache_id, 1).unwrap();

        // Corrupt a byte in the middle.
        let mut data = std::fs::read(&path).unwrap();
        if data.len() > 10 {
            data[10] ^= 0xFF;
        }
        std::fs::write(&path, &data).unwrap();

        assert!(SmtCalc::load_cache(cache_id).is_err());
    }

    #[test]
    fn test_smt_cache_incremental() {
        let cache_id = 1010;
        let path = vsdb_core::common::vsdb_get_custom_dir()
            .join(format!("smt_cache_{}.bin", cache_id));

        let mut smt = SmtCalc::new();
        smt.insert(b"a", b"1").unwrap();
        smt.insert(b"b", b"2").unwrap();
        smt.save_cache(cache_id, 10).unwrap();

        let (mut loaded, _, _) = SmtCalc::load_cache(cache_id).unwrap();
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
